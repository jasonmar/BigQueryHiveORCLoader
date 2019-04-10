package com.google.example

import org.apache.hadoop.hive.metastore.api.FieldSchema
import com.google.cloud.bigquery.{BigQuery, Field, Schema, StandardSQLTypeName, TableInfo}
import com.google.example.MetastoreQuery.TableMetadata

object Mapping {
  import scala.collection.JavaConverters.seqAsJavaListConverter

  def createExternalTable(project: String,
                          dataset: String,
                          table: String,
                          meta: TableMetadata,
                          bigquery: BigQuery): TableInfo = {
    val sources: java.util.List[String] = meta.partitions.map(_.getSd.getLocation).asJava
    val schema = convertSchema(meta)
    ExternalTableManager.createExternalTable(project, dataset, table, schema, sources, bigquery)
  }

  def convertSchema(meta: TableMetadata): Schema = {
    Schema.of(meta.fields.map(convert):_*)
  }

  def convert(hive: FieldSchema): Field = {
    Field.newBuilder(hive.getName, TypeMap(hive.getType))
      .setMode(Field.Mode.NULLABLE)
      .setDescription(hive.getComment)
      .build()
  }

  val TypeMap: Map[String,StandardSQLTypeName] = Map(
    "TINYINT" -> StandardSQLTypeName.INT64,
    "SMALLINT" -> StandardSQLTypeName.INT64,
    "INT" -> StandardSQLTypeName.INT64,
    "INTEGER" -> StandardSQLTypeName.INT64,
    "BIGINT" -> StandardSQLTypeName.INT64,
    "FLOAT" -> StandardSQLTypeName.FLOAT64,
    "DOUBLE" -> StandardSQLTypeName.FLOAT64,
    "DOUBLE PRECISION" -> StandardSQLTypeName.FLOAT64,
    "DECIMAL" -> StandardSQLTypeName.NUMERIC,
    "NUMERIC" -> StandardSQLTypeName.NUMERIC,
    "BOOLEAN" -> StandardSQLTypeName.BOOL,
    "BINARY" -> StandardSQLTypeName.BYTES,
    "TIMESTAMP" -> StandardSQLTypeName.TIMESTAMP,
    "DATE" -> StandardSQLTypeName.DATE,
    "INTERVAL" -> StandardSQLTypeName.DATETIME,
    "STRING" -> StandardSQLTypeName.STRING,
    "VARCHAR" -> StandardSQLTypeName.STRING,
    "CHAR" -> StandardSQLTypeName.STRING,
    "STRUCT" -> StandardSQLTypeName.STRUCT
  )
}
