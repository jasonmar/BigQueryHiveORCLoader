package com.google.example

import com.google.cloud.bigquery.{BigQuery, ExternalTableDefinition, FormatOptions, Schema, TableId, TableInfo}

object ExternalTableManager {
  def defaultExpiration: Long = System.currentTimeMillis() + 1000*60*60*24*2 // 2 days

  def createExternalTable(project: String,
                          dataset: String,
                          table: String,
                          schema: Schema,
                          sources: java.util.List[String],
                          bigquery: BigQuery): TableInfo = {

    val tableId = TableId.of(project, dataset, table)

    val tableDefinition = ExternalTableDefinition
      .newBuilder(sources, schema, FormatOptions.orc()).build()

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinition)
      .setExpirationTime(defaultExpiration)
      .build()

    bigquery.create(tableInfo)

    tableInfo
  }
}
