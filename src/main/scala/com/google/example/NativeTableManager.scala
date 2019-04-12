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
