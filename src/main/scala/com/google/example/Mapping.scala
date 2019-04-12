package com.google.example

import java.util.Collections

import com.google.cloud.bigquery._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}

object Mapping {
  def createExternalTable(project: String,
                          dataset: String,
                          table: String,
                          tableMetadata: CatalogTable,
                          part: CatalogTablePartition,
                          bigquery: BigQuery): TableInfo = {
    val extTableName = validBigQueryTableName(table + "_" + part.spec.values.mkString("_"))
    val partCols = tableMetadata.partitionColumnNames.toSet

    val partSchema = StructType(tableMetadata.schema.filterNot(x => partCols.contains(x.name)))

    ExternalTableManager.createExternalTable(
      project,
      dataset,
      extTableName,
      schema = convertStructType(partSchema),
      sources = Collections.singletonList(part.location.toString),
      bigquery = bigquery)
  }

  def validBigQueryTableName(s: String): String = {
    s.replace('=','_')
      .filter(c =>
        (c >= '0' && c <= '9') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= 'a' && c <= 'z') ||
        c == '_'
      ).take(1024)
  }

  def convertStructType(fields: StructType): Schema = {
    Schema.of(fields.map(convertStructField):_*)
  }

  def convertStructField(field: StructField): Field = {
    Field.newBuilder(field.name, convertTypeName(field.dataType.typeName))
      .setMode(if (field.nullable) Field.Mode.NULLABLE else Field.Mode.REQUIRED)
      .build()
  }

  def convertTypeName(dataTypeName: String): StandardSQLTypeName = {
    dataTypeName match {
      case x if x.startsWith("array") => StandardSQLTypeName.ARRAY
      case x if x == IntegerType.typeName => StandardSQLTypeName.INT64
      case x if x == ShortType.typeName => StandardSQLTypeName.INT64
      case x if x == LongType.typeName => StandardSQLTypeName.INT64
      case x if x == FloatType.typeName => StandardSQLTypeName.FLOAT64
      case x if x == DoubleType.typeName => StandardSQLTypeName.FLOAT64
      case x if x.startsWith("decimal") => StandardSQLTypeName.NUMERIC
      case x if x == BooleanType.typeName => StandardSQLTypeName.BOOL
      case x if x == ByteType.typeName => StandardSQLTypeName.BYTES
      case x if x == TimestampType.typeName => StandardSQLTypeName.TIMESTAMP
      case x if x == DateType.typeName => StandardSQLTypeName.DATE
      case x if x == StringType.typeName => StandardSQLTypeName.STRING
      case x if x.startsWith("struct") => StandardSQLTypeName.STRUCT
      case _ =>
        throw new RuntimeException(s"Unexpected DataType '$dataTypeName'")
    }
  }
}
