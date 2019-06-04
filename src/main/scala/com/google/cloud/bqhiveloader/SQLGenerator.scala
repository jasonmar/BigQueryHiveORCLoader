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

package com.google.cloud.bqhiveloader

import com.google.cloud.bigquery.TableId
import com.google.cloud.bqhiveloader.ExternalTableManager.format
import com.google.cloud.bqhiveloader.MetaStore.Partition
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object SQLGenerator {
  def generateSelectFromExternalTable(extTable: TableId,
                                      schema: StructType,
                                      partition: Partition,
                                      unusedColumnName: String,
                                      formats: Map[String,String] = Map.empty,
                                      renameOrcCols: Boolean = false,
                                      dropColumns: Set[String] = Set.empty,
                                      keepColumns: Set[String] = Set.empty): String = {
    // Columns from partition values
    // Examples:
    // PARSE_DATE('%Y-%m-%d','2019-06-04') as date
    // 'US' as country
    val partVals = partition.values
      .map{x =>
        val (colName, colValue) = x
        if (colName == unusedColumnName) {
          s"NULL as $colName"
        } else if (formats.contains(colName)){
          format(colName, colValue, formats(colName))
        } else {
          schema.find(_.name == colName) match {
            case Some(field) if field.dataType.typeName == IntegerType.typeName || field.dataType.typeName == LongType.typeName =>
              s"$colValue as $colName"
            case Some(field) if field.dataType.typeName == "date" =>
              s"PARSE_DATE('%Y-%m-%d','$colValue') as $colName"
            case Some(field) if field.dataType.typeName == "timestamp" =>
              s"DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S','$colValue')) as $colName"
            case _ =>
              s"'$colValue' as $colName"
          }
        }
      }

    val partColNames: Set[String] = partition.values.map(_._1).toSet

    // Columns from partition values
    val renamed = if (renameOrcCols) {
      // handle positional column naming
      schema
        .filterNot(field => partColNames.contains(field.name))
        .zipWithIndex
        .map{x => (x._1.name, s"_col${x._2} as ${x._1.name}")}
    } else {
      schema
        .filterNot(field => partColNames.contains(field.name))
        .map{x => (x.name, s"${x.name}") }
    }

    // handle drop/keep
    val fields = renamed
      .filterNot(field => dropColumns.contains(field._1))
      .filter(field => keepColumns.isEmpty || keepColumns.contains(field._1))
      .map(_._2)

    val tableSpec = extTable.getProject + "." + extTable.getDataset + "." + extTable.getTable
    s"""select
       |  ${(partVals ++ fields).mkString(",\n  ")}
       |from `$tableSpec`""".stripMargin
  }
}
