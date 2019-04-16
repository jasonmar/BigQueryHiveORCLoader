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
import com.google.cloud.bigquery.{BigQuery, QueryJobConfiguration, TableId, TableResult}

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
}
