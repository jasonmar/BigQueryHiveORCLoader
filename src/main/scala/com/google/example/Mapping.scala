package com.google.example

import java.util.Collections

import com.google.cloud.bigquery._
import com.google.example.MetastoreQuery.TableMetadata
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition}

import scala.collection.JavaConverters.asScalaBufferConverter

object Mapping {
  def createExternalTables(project: String,
                           dataset: String,
                           table: String,
                           meta: TableMetadata,
                           bigquery: BigQuery): Seq[TableInfo] = {
    meta.partitions.map{part =>
      createExternalTableForPartition(
        project, dataset, table, part, bigquery)
    }
  }

  def createExternalTableForPartition(project: String,
                          dataset: String,
                          table: String,
                          part: Partition,
                          bigquery: BigQuery): TableInfo = {
    val extTableName = table + "_" + part.getValues.asScala.mkString("_")

    ExternalTableManager.createExternalTable(
      project,
      dataset,
      extTableName,
      convertFields(part.getSd.getCols),
      Collections.singletonList(part.getSd.getLocation),
      bigquery)
  }

  def convertFields(fields: java.util.List[FieldSchema]): Schema = {
    Schema.of(fields.asScala.map(convertField):_*)
  }

  def convertField(field: FieldSchema): Field = {
    Field.newBuilder(field.getName, TypeMap(field.getType))
      .setMode(Field.Mode.NULLABLE)
      .setDescription(field.getComment)
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
