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
import com.google.example.BQHiveLoader.{BigQueryScope, Config, StorageScope}
import com.google.example.ExternalTableManager.Orc
import com.google.example.MetaStore.{Partition, TableMetadata}
import org.apache.spark.sql.types.DateType

object SparkJobs {
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

    val bigquery: BigQuery = BigQueryOptions.newBuilder()
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

    import scala.collection.JavaConverters._
    val destTableId = TableId.of(c.bqProject, c.bqDataset, c.bqTable)
    val destTableExists = bigquery.getTable(destTableId).exists()
    if (!destTableExists) {
      require(c.clusterColumns.nonEmpty, "destination table does not exist, clusterColumns must not be empty")
      require(c.partitionColumn.nonEmpty, "destination table does not exist, partitionColumn must not be empty")
      val destTableSchema = if (c.partitionColumn.map(_.toLowerCase).contains("none")) {
        Mapping.convertStructType(table.schema.add(c.unusedColumnName, DateType))
      } else {
        Mapping.convertStructType(table.schema)
      }

      val destTableDefBuilder = StandardTableDefinition.newBuilder()
        .setLocation(c.bqLocation)
        .setSchema(destTableSchema)
        .setTimePartitioning(TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
          .setField(c.partitionColumn.map(_.toLowerCase)
            .filterNot(_ == "none")
            .getOrElse(c.unusedColumnName))
          .build())

      if (c.clusterColumns.map(_.toLowerCase) != Seq("none")) {
        destTableDefBuilder.setClustering(Clustering.newBuilder()
          .setFields(c.clusterColumns.map(_.toLowerCase).asJava).build())
      }

      val tableInfo = TableInfo.newBuilder(destTableId, destTableDefBuilder.build())
        .build()
      bigquery.create(tableInfo)
    }

    ExternalTableManager.loadParts(project = c.bqProject,
                                   dataset = c.bqDataset,
                                   tableName = c.bqTable,
                                   tableMetadata = table,
                                   partitions = partitions,
                                   unusedColumnName = c.unusedColumnName,
                                   partColFormats = c.partColFormats.toMap,
                                   storageFormat = storageFormat,
                                   bigqueryCreate = bigquery,
                                   bigqueryWrite = bigqueryWrite,
                                   overwrite = c.bqOverwrite,
                                   batch = c.bqBatch,
                                   gcs = gcs)
  }
}
