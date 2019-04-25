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

import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery._
import org.apache.spark.sql.types.{DateType, StructType}

object NativeTableManager {
  def getExistingPartitions(tableId: TableId, bigQuery: BigQuery): TableResult = {
    val tableSpec = tableId.getDataset + "." + tableId.getTable
    tableId.toString + "$__PARTITIONS_SUMMARY__"
    bigQuery.query(QueryJobConfiguration.newBuilder(
      s"""SELECT
         |  partition_id,
         |  TIMESTAMP(creation_time/1000) AS creation_time
         |FROM [$tableSpec]""".stripMargin)
      .setUseLegacySql(true)
      .setPriority(Priority.INTERACTIVE)
      .build())
  }

  def createTable(c: Config, schema: StructType, destTableId: TableId, bigquery: BigQuery): Table ={
    require(c.clusterColumns.nonEmpty, "destination table does not exist, clusterColumns must not be empty")
    require(c.partitionColumn.nonEmpty, "destination table does not exist, partitionColumn must not be empty")
    val destTableSchema = if (c.partitionColumn.map(_.toLowerCase).contains("none")) {
      Mapping.convertStructType(schema.add(c.unusedColumnName, DateType))
    } else {
      Mapping.convertStructType(schema)
    }

    val destTableDefBuilder = StandardTableDefinition.newBuilder()
      .setLocation(c.bqLocation)
      .setSchema(destTableSchema)

    if (c.partitionColumn.contains("none") && c.clusterColumns.exists(_ != "none")) {
      // Only set null partition column if both partition column and cluster columns are provided
      destTableDefBuilder
        .setTimePartitioning(TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setField(c.unusedColumnName)
          .build())
    } else {
      c.partitionColumn match {
        case Some(partitionCol) if partitionCol != "none" =>
          // Only set partition column if partition column is set
          destTableDefBuilder
            .setTimePartitioning(TimePartitioning
              .newBuilder(TimePartitioning.Type.DAY)
              .setField(partitionCol)
              .build())
        case _ =>
          // Don't set a partition column if partition column is none
      }
    }

    if (c.clusterColumns.map(_.toLowerCase) != Seq("none")) {
      import scala.collection.JavaConverters.seqAsJavaListConverter
      destTableDefBuilder.setClustering(Clustering.newBuilder()
        .setFields(c.clusterColumns.map(_.toLowerCase).asJava).build())
    }

    val tableInfo = TableInfo.newBuilder(destTableId, destTableDefBuilder.build())
      .build()
    bigquery.create(tableInfo)
  }
}
