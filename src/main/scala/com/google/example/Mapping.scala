package com.google.example

import com.google.cloud.bigquery._
import com.google.example.MetastoreQuery.TableMetadata
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition}

import scala.collection.JavaConverters.{asScalaBufferConverter,seqAsJavaListConverter}

object Mapping {
  def createExternalTables(project: String,
                           dataset: String,
                           table: String,
                           meta: TableMetadata,
                           bigquery: BigQuery): Seq[TableInfo] = {
    meta.partitions
      .groupBy(partitionKey)
      .toArray
      .sortBy(_._1._1)
      .map{case (partKey, parts) =>
        createExternalTableForPartition(
          project, dataset, table,
          partKey._1, partKey._2,
          parts, bigquery)
      }.toSeq
  }

  def partitionKey(part: Partition): (String,Schema) = {
    val colNames = part.getSd.getCols.asScala.map(_.getName)
    val colValues = part.getValues.asScala
    val schema = convertFields(part.getSd.getCols)
    val values = colNames.zip(colValues)
      .sorted
      .map{case (k,v) => s"$k=$v"}
      .mkString(",")
    (values, schema)
  }

  def bigQueryTableName(s: String): String = {
    s.replace('=','_').filter(c =>
      (c >= '0' && c <= '9') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= 'a' && c <= 'z') ||
      c == '_'
    )
  }

  def createExternalTableForPartition(project: String,
                          dataset: String,
                          table: String,
                          partValues: String,
                          schema: Schema,
                          parts: Seq[Partition],
                          bigquery: BigQuery): TableInfo = {
    val extTableName = bigQueryTableName(table + "_" + partValues)

    ExternalTableManager.createExternalTable(
      project,
      dataset,
      extTableName,
      schema,
      parts.map(_.getSd.getLocation).asJava,
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
