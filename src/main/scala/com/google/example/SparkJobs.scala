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
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.example.BQHiveLoader.{BigQueryScope, Config, StorageScope}
import com.google.example.MetaStore.{Partition, TableMetadata}

object SparkJobs {
  def loadPartitionsJob(table: TableMetadata, partitions: Seq[Partition]): Iterator[Config] => Unit =
    (it: Iterator[Config]) => loadPartitions(it.next, table, partitions)

  def loadPartitions(c: Config, table: TableMetadata, partitions: Seq[Partition]): Unit = {
    // TODO add another credential for bigquery writes
    val creds: GoogleCredentials = c.bqKeyFile match {
      case Some(f) =>
        GoogleCredentials.fromStream(new ByteArrayInputStream(Files.readAllBytes(Paths.get(f)))).createScoped(BigQueryScope)
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
      .setCredentials(creds)
      .setProjectId(c.bqProject)
      .build()
      .getService

    val gcs: Storage = StorageOptions.newBuilder()
      .setCredentials(storageCreds)
      .setProjectId(c.bqProject)
      .build()
      .getService

    ExternalTableManager.loadParts(project = c.bqProject,
                                   dataset = c.bqDataset,
                                   tableName = c.bqTable,
                                   tableMetadata = table,
                                   partitions = partitions,
                                   bigquery = bigquery,
                                   gcs = gcs)
  }
}
