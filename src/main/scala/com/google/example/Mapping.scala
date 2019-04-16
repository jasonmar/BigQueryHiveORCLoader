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

import com.google.cloud.bigquery._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}

object Mapping {
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
      case x if x == StringType.typeName => StandardSQLTypeName.STRING
      case x if x == IntegerType.typeName => StandardSQLTypeName.INT64
      case x if x == LongType.typeName => StandardSQLTypeName.INT64
      case x if x == DoubleType.typeName => StandardSQLTypeName.FLOAT64
      case x if x == DateType.typeName => StandardSQLTypeName.DATE
      case x if x == TimestampType.typeName => StandardSQLTypeName.TIMESTAMP
      case x if x == FloatType.typeName => StandardSQLTypeName.FLOAT64
      case x if x.startsWith("array") => StandardSQLTypeName.ARRAY
      case x if x == ShortType.typeName => StandardSQLTypeName.INT64
      case x if x.startsWith("decimal") => StandardSQLTypeName.NUMERIC
      case x if x == BooleanType.typeName => StandardSQLTypeName.BOOL
      case x if x == ByteType.typeName => StandardSQLTypeName.BYTES
      case x if x.startsWith("struct") => StandardSQLTypeName.STRUCT
      case _ =>
        throw new RuntimeException(s"Unexpected DataType '$dataTypeName'")
    }
  }
}
