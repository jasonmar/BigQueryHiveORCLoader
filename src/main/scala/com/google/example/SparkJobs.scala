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

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery._
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.example.ExternalTableManager.Orc
import com.google.example.MetaStore._
import org.apache.spark.sql.SparkSession

object SparkJobs {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"

  def run(config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BQHiveORCLoader")
      .enableHiveSupport
      .getOrCreate()
    run(config, spark)
  }

  def run(config: Config, spark: SparkSession): Unit = {
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
    val table = metaStore.getTable(config.hiveDbName, config.hiveTableName)
    val partitions: Seq[Partition] = metaStore.filterPartitions(config.hiveDbName, config.hiveTableName, config.partFilters)

    val sc = spark.sparkContext
    sc.runJob(rdd = sc.makeRDD(Seq(config),1),
      func = SparkJobs.loadPartitionsJob(table, partitions))
  }

  def loadPartitionsJob(table: TableMetadata, partitions: Seq[Partition]): Iterator[Config] => Unit =
    (it: Iterator[Config]) => loadPartitions(it.next, table, partitions)

  def loadPartitions(c: Config, table: TableMetadata, partitions: Seq[Partition]): Unit = {
    val bqCreateCredentials: GoogleCredentials =
      c.bqCreateTableKeyFile.orElse(c.bqKeyFile) match {
        case Some(f) =>
          GoogleCredentials
            .fromStream(new ByteArrayInputStream(Files.readAllBytes(Paths.get(f))))
            .createScoped(BigQueryScope)
        case _ =>
          GoogleCredentials.getApplicationDefault
      }

    val bqWriteCredentials: GoogleCredentials =
      c.bqWriteKeyFile.orElse(c.bqKeyFile) match {
        case Some(f) =>
          GoogleCredentials
            .fromStream(new ByteArrayInputStream(Files.readAllBytes(Paths.get(f))))
            .createScoped(BigQueryScope)
        case _ =>
          GoogleCredentials.getApplicationDefault
      }

    val storageCreds: GoogleCredentials = c.gcsKeyFile match {
      case Some(f) =>
        GoogleCredentials.fromStream(new ByteArrayInputStream(Files.readAllBytes(Paths.get(f)))).createScoped(StorageScope)
      case _ =>
        GoogleCredentials.getApplicationDefault
    }

    val bigqueryCreate: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bqLocation)
      .setCredentials(bqCreateCredentials)
      .setProjectId(c.bqProject)
      .build()
      .getService

    val bigqueryWrite: BigQuery = BigQueryOptions.newBuilder()
      .setLocation(c.bqLocation)
      .setCredentials(bqWriteCredentials)
      .setProjectId(c.bqProject)
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
    if (!bigqueryCreate.getTable(destTableId).exists()) {
      NativeTableManager.createTable(c, table.schema, destTableId, bigqueryWrite)
    }

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
  }
}
