/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bqhiveloader

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Paths}

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.RetryOption
import com.google.cloud.bigquery._
import com.google.cloud.bqhiveloader.ExternalTableManager.{Orc, createExternalTable, hasOrcPositionalColNames, loadPart, waitForCreation}
import com.google.cloud.bqhiveloader.MetaStore._
import com.google.cloud.storage.{Storage, StorageOptions}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.threeten.bp.Duration


object SparkJobs extends Logging {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"
  val MaxSQLLength = 12 * 1024 * 1024

  def run(config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.yarn.maxAppAttempts","1")
      .appName("BQHiveORCLoader")
      .enableHiveSupport
      .getOrCreate()

    logger.info(s"launching with MetaStore type '${config.hiveMetastoreType}'")
    val metaStore = {
      config.hiveMetastoreType match {
        case "jdbc" =>
          JDBCMetaStore(config.hiveJdbcUrl, spark)
        case "sql" =>
          SparkSQLMetaStore(spark)
        case "external" =>
          ExternalCatalogMetaStore(spark)
        case x =>
          throw new IllegalArgumentException(s"unsupported metastore type '$x'")
      }
    }

    for {
      keytab <- config.krbKeyTab
      principal <- config.krbPrincipal
    } yield {
      Kerberos.configureJaas(keytab, principal)
    }

    runWithMetaStore(config, metaStore, spark)
  }

  /** Reads partitions from MetaStore and launches Spark Job
    *
    * @param config
    * @param metaStore
    * @param spark
    */
  def runWithMetaStore(config: Config, metaStore: MetaStore, spark: SparkSession): Unit = {
    val table = metaStore.getTable(config.hiveDbName, config.hiveTableName)
    val partitions: Seq[Partition] =
      if (config.partitioned && table.partitionColumnNames.nonEmpty) {
        metaStore.filterPartitions(db = config.hiveDbName,
          table = config.hiveTableName,
          filterExpression = config.partFilters
        ) match {
          case parts if parts.nonEmpty =>
            logger.info(s"Selected ${parts.length} partitions for table '${config.hiveDbName}.${config.hiveTableName}':\n\t${parts.map(_.location).mkString("\n\t")}")
            parts
          case _ =>
            throw new RuntimeException(s"No partitions found with filter expression '${config.partFilters}'")
        }
      } else {
        table.location match {
          case Some(location) =>
            logger.info(s"Loading '${config.hiveDbName}.${config.hiveTableName}' as non-partitioned table from $location")
            Seq(Partition(Seq.empty, location))
          case _ =>
            throw new RuntimeException("Location not found in table description for non-partitioned table")
        }
      }
    val sc = spark.sparkContext
    sc.runJob(rdd = sc.makeRDD(Seq(config), numSlices = 1),
              func = loadPartitionsJob(table, partitions))
  }

  /** Spark Job
    *
    * @param table
    * @param partitions
    * @return
    */
  def loadPartitionsJob(table: TableMetadata, partitions: Seq[Partition]): Iterator[Config] => Unit =
    (it: Iterator[Config]) => loadPartitions(it.next, table, partitions)

  def readLocalOrSparkFile(maybePath: scala.Option[String]): scala.Option[Array[Byte]] = {
    val direct = maybePath
      .map(Paths.get(_))
      .filter{_.toFile.exists()}

    val fromSparkFiles = maybePath
      .map(SparkFiles.get)
      .map(Paths.get(_))
      .filter{_.toFile.exists}

    direct.orElse(fromSparkFiles)
      .map(Files.readAllBytes)
      .filter(_.nonEmpty)
  }

  /** Spark Job to create external table and create BigQuery load job
    * The data is not touched, this is launched as a remote job to use
    * a Service Account Key File located on the worker node.
    *
    * @param c
    * @param table
    * @param partitions
    */
  def loadPartitions(c: Config, table: TableMetadata, partitions: Seq[Partition]): Unit = {
    for {
      keytab <- c.krbKeyTab
      principal <- c.krbPrincipal
    } yield {
      Kerberos.configureJaas(keytab, principal)
    }

    val writeKeyPath = c.bqWriteKeyFile.orElse(c.bqKeyFile)
    val createKeyPath = c.bqCreateTableKeyFile.orElse(c.bqKeyFile)
    val gcsKeyPath = c.gcsKeyFile.orElse(c.bqKeyFile)
    val writeKey = readLocalOrSparkFile(writeKeyPath)
    val createKey = readLocalOrSparkFile(createKeyPath)
    val gcsKey = readLocalOrSparkFile(gcsKeyPath)
    writeKeyPath.foreach{p =>
      require(writeKey.isDefined, s"unable to load BigQuery write key from $p")
      logger.info(s"loaded BigQuery write key from $p")
    }
    createKeyPath.foreach{p =>
      require(createKey.isDefined, s"unable to load BigQuery create key from $p")
      logger.info(s"loaded BigQuery create key from $p")
    }
    gcsKeyPath.foreach{p =>
      require(gcsKey.isDefined, s"unable to load GCS key from $p")
      logger.info(s"loaded GCS key from $p")
    }

    val bqCreateCredentials: GoogleCredentials =
      createKey match {
        case Some(bytes) =>
          GoogleCredentials
            .fromStream(new ByteArrayInputStream(bytes))
            .createScoped(BigQueryScope)
        case _ =>
          GoogleCredentials.getApplicationDefault
      }

    val bqWriteCredentials: GoogleCredentials =
      writeKey match {
        case Some(bytes) =>
          GoogleCredentials
            .fromStream(new ByteArrayInputStream(bytes))
            .createScoped(BigQueryScope)
        case _ =>
          GoogleCredentials.getApplicationDefault
      }

    val storageCreds: GoogleCredentials =
      gcsKey match {
        case Some(bytes) =>
          GoogleCredentials
            .fromStream(new ByteArrayInputStream(bytes))
            .createScoped(StorageScope)
        case _ =>
          GoogleCredentials.getApplicationDefault
      }

    val retrySettings = RetrySettings.newBuilder()
      .setMaxAttempts(1)
      .build()

    val bigqueryCreate: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bqLocation)
      .setCredentials(bqCreateCredentials)
      .setProjectId(c.bqProject)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "BQHiveLoader 0.1"))
      .setRetrySettings(retrySettings)
      .build()
      .getService

    val bigqueryWrite: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bqLocation)
      .setCredentials(bqWriteCredentials)
      .setProjectId(c.bqProject)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "BQHiveLoader 0.1"))
      .setRetrySettings(retrySettings)
      .build()
      .getService

    val gcs: Storage = StorageOptions.newBuilder()
      .setCredentials(storageCreds)
      .setProjectId(c.bqProject)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "BQHiveLoader 0.1"))
      .setRetrySettings(retrySettings)
      .build()
      .getService

    // TODO detect from metadata
    val storageFormat = c.hiveStorageFormat
        .map(ExternalTableManager.parseStorageFormat)
        .getOrElse(Orc)

    val filteredSchema = StructType(table.schema
      .filterNot{x => c.dropColumns.contains(x.name)}
      .filter{x => c.keepColumns.contains(x.name) || c.keepColumns.isEmpty}
      .map{x =>
        c.renameColumns.find(_._1 == x.name) match {
          case Some((_, newName)) =>
            x.copy(name = newName)
          case _ =>
            x
        }
      })

    /* Create BigQuery table to be loaded */
    NativeTableManager.createTableIfNotExists(c.bqProject, c.bqDataset, c.bqTable, c, filteredSchema, bigqueryWrite)

    val externalTables: Seq[(Partition,TableInfo, Boolean)] = partitions.map{part =>
      val extTable = createExternalTable(c.bqProject, c.tempDataset, c.bqTable,
        table, part, storageFormat, bigqueryCreate, gcs, c.dryRun)

      val renameOrcCols = if (!c.dryRun) {
        val creation = waitForCreation(extTable.getTableId, timeoutMillis = 120000L, bigqueryCreate)
        hasOrcPositionalColNames(creation)
      } else true

      (part, extTable, renameOrcCols)
    }

    val sql: Seq[String] = externalTables.map{x =>
      val partition = x._1
      val extTableId = x._2.getTableId
      val schema = table.schema
      val renameOrcCols = x._3
      SQLGenerator.generateSelectFromExternalTable(
        extTable = extTableId,
        schema = schema,
        partition = partition,
        unusedColumnName = c.unusedColumnName,
        formats = c.partColFormats.toMap,
        renameOrcCols = renameOrcCols,
        dropColumns = c.dropColumns,
        keepColumns = c.keepColumns)
    }
    val unionSQL = sql.mkString("\n\nUNION ALL\n\n")
    logger.info(s"Generated SQL with length ${unionSQL.length}")
    if (unionSQL.length < MaxSQLLength) {
      logger.debug("Submitting Query:\n" + unionSQL)
      val destTableId = TableId.of(c.bqProject, c.bqDataset, c.bqTable)
      val queryJob = ExternalTableManager.runQuery(unionSQL, destTableId, c.bqProject,
        c.bqLocation, c.dryRun, c.bqOverwrite, c.bqBatch, bigqueryWrite)
      Option(queryJob.waitFor(RetryOption.totalTimeout(Duration.ofHours(8)))) match {
        case None =>
          logger.error("job failed")
          throw new RuntimeException("job doesn't exist")
        case Some(j) if j.getStatus().getError() != null =>
          logger.error("job failed")
          throw new RuntimeException(j.getStatus().getError().getMessage)
        case _ =>
      }
      logger.info("finished loading partitions")
    } else {
      val tmpTableName = c.bqTable + "_" + c.refreshPartition.getOrElse("") + "_tmp_" + (System.currentTimeMillis()/1000L).toString
      logger.info("Loading partitions into temporary table " + tmpTableName)

      NativeTableManager.createTableIfNotExists(c.bqProject, c.tempDataset, tmpTableName, c, table.schema, bigqueryWrite, Some(Duration.ofHours(6).toMillis))

      logger.info("Loading partitions from external tables")
      val tmpTableId = TableId.of(c.bqProject, c.tempDataset, tmpTableName)
      externalTables.flatMap { x =>
        val partition = x._1
        val extTableId = x._2.getTableId
        val schema = table.schema
        val renameOrcCols = x._3
        ExternalTableManager.loadPart(
          destTableId = tmpTableId,
          schema = schema,
          partition = partition,
          extTableId = extTableId,
          unusedColumnName = c.unusedColumnName,
          partColFormats = c.partColFormats.toMap,
          bigqueryWrite = bigqueryWrite,
          batch = c.bqBatch,
          overwrite = c.bqOverwrite,
          renameOrcCols = renameOrcCols,
          dryRun = c.dryRun,
          dropColumns = c.dropColumns,
          keepColumns = c.keepColumns,
          renameColumns = c.renameColumns.toMap)
      }
      logger.info("Finished loading " + tmpTableName)
      NativeTableManager.copyOnto(c.bqProject, c.tempDataset, tmpTableName,
        c.bqProject, c.bqDataset, c.bqTable, destPartition = c.refreshPartition,
        bq = bigqueryWrite, dryRun = c.dryRun, batch = c.bqBatch)
      logger.info(s"Finished loading ${c.bqTable}")
    }
  }
}
