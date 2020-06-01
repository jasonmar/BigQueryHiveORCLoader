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

package com.google.cloud.bigquery

import com.google.cloud.imf.bqhiveloader.Logging

object RangePartitioningUtil extends Logging {
  private def addRangePartitioning(rangeField: String,
                           start: Long,
                           end: Long,
                           interval: Long,
                           tableInfo: TableInfo): TableInfo = {
    val defBuilder = tableInfo.getDefinition[StandardTableDefinition].toBuilder

    val rp = new RangePartitioning.Builder()
      .setField(rangeField)
      .setRange(RangePartitioning.Range.newBuilder().setStart(start).setEnd(end).build)
    defBuilder.setRangePartitioning(rp.build)

    tableInfo.toBuilder.setDefinition(defBuilder.build).build
  }

  def createTable(projectId: String,
                  datasetId: String,
                  tableInfo: TableInfo,
                  bigquery: BigQuery,
                  rangeField: String,
                  start: Long,
                  end: Long,
                  interval: Long): Table = {
    val tableWithRangePartition =
      addRangePartitioning(rangeField, start, end, interval, tableInfo)

    val table = bigquery.create(tableWithRangePartition)
    val tableSpec = Seq(table.getTableId.getProject,
                        table.getTableId.getDataset,
                        table.getTableId.getTable).mkString(",")
    logger.info(s"Created table `$tableSpec`")
    table
  }
}
