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

package com.google.example

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Paths}

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery._
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.example.ExternalTableManager.Orc
import com.google.example.MetaStore._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object SparkJobs {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"

  def run(config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BQHiveORCLoader")
      .enableHiveSupport
      .getOrCreate()

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

    runWithMetaStore(config, metaStore, spark)
  }

  def runWithMetaStore(config: Config, metaStore: MetaStore, spark: SparkSession): Unit = {
    val table = metaStore.getTable(config.hiveDbName, config.hiveTableName)
    val partitions: Seq[Partition] =
      if (table.location.isEmpty && config.partitioned) {
        metaStore.filterPartitions(db = config.hiveDbName,
          table = config.hiveTableName,
          filterExpression = config.partFilters
        ) match {
          case parts if parts.nonEmpty =>
            parts
          case _ =>
            throw new RuntimeException(s"No partitions found with filter expression '${config.partFilters}'")
        }
      } else {
        table.location match {
          case Some(location) =>
            Seq(Partition(Seq.empty, location))
          case _ =>
            throw new RuntimeException("Location not found in table description for non-partitioned table")
        }
      }
    val sc = spark.sparkContext
    sc.runJob(rdd = sc.makeRDD(Seq(config), numSlices = 1),
              func = loadPartitionsJob(table, partitions))
  }

  def loadPartitionsJob(table: TableMetadata, partitions: Seq[Partition]): Iterator[Config] => Unit =
    (it: Iterator[Config]) => loadPartitions(it.next, table, partitions)

  def readLocalOrSparkFile(maybePath: scala.Option[String]): scala.Option[Array[Byte]] = {
    val direct = maybePath.map(Paths.get(_)).filter{_.toFile.exists()}
    val fromSparkFiles = maybePath.map(SparkFiles.get).map(Paths.get(_)).filter{_.toFile.exists()}
    direct.orElse(fromSparkFiles)
      .map(Files.readAllBytes)
      .filter(_.nonEmpty)
  }

  def loadPartitions(c: Config, table: TableMetadata, partitions: Seq[Partition]): Unit = {
    val writeKeyPath = c.bqWriteKeyFile.orElse(c.bqKeyFile)
    val createKeyPath = c.bqCreateTableKeyFile.orElse(c.bqKeyFile)
    val gcsKeyPath = c.gcsKeyFile.orElse(c.bqKeyFile)
    val writeKey = readLocalOrSparkFile(writeKeyPath)
    val createKey = readLocalOrSparkFile(createKeyPath)
    val gcsKey = readLocalOrSparkFile(gcsKeyPath)
    writeKeyPath.foreach{p =>
      require(writeKey.isDefined, s"unable to load BigQuery write key from $p")
      System.out.println(s"loaded BigQuery write key from $p")
    }
    createKeyPath.foreach{p =>
      require(createKey.isDefined, s"unable to load BigQuery create key from $p")
      System.out.println(s"loaded BigQuery create key from $p")
    }
    gcsKeyPath.foreach{p =>
      require(gcsKey.isDefined, s"unable to load GCS key from $p")
      System.out.println(s"loaded GCS key from $p")
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

    val bigqueryCreate: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bqLocation)
      .setCredentials(bqCreateCredentials)
      .setProjectId(c.bqProject)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "BQHiveLoader 0.1"))
      .build()
      .getService

    val bigqueryWrite: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bqLocation)
      .setCredentials(bqWriteCredentials)
      .setProjectId(c.bqProject)
      .setHeaderProvider(FixedHeaderProvider.create("user-agent", "BQHiveLoader 0.1"))
      .build()
      .getService

    val gcs: Storage = StorageOptions.newBuilder()
      .setCredentials(storageCreds)
      .setProjectId(c.bqProject)
      .build()
      .getService

    // TODO detect from metadata
    val storageFormat = c.hiveStorageFormat
        .map(ExternalTableManager.parseStorageFormat)
        .getOrElse(Orc)

    val destTableId = TableId.of(c.bqProject, c.bqDataset, c.bqTable)
    if (!ExternalTableManager.tableExists(destTableId, bigqueryWrite)) {
      NativeTableManager.createTable(c, table.schema, destTableId, bigqueryWrite)
    }

    Try(
      ExternalTableManager.loadParts(project = c.bqProject,
                                   dataset = c.bqDataset,
                                   tableName = c.bqTable,
                                   tableMetadata = table,
                                   partitions = partitions,
                                   unusedColumnName = c.unusedColumnName,
                                   partColFormats = c.partColFormats.toMap,
                                   storageFormat = storageFormat,
                                   bigqueryCreate = bigqueryCreate,
                                   bigqueryWrite = bigqueryWrite,
                                   overwrite = c.bqOverwrite,
                                   batch = c.bqBatch,
                                   gcs = gcs)
    ) match {
      case Success(value) =>
        value
      case Failure(exception) =>
        throw new RuntimeException(s"failed to load partitions with config:\n$c\n\ntable:\n$table", exception)
    }
  }
}
