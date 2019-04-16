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

import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery._
import com.google.example.Mapping.convertStructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.types.StructType

object ExternalTableManager {
  def defaultExpiration: Long = System.currentTimeMillis() + 1000*60*60*24*2 // 2 days

  def create(tableId: TableId,
             schema: Schema,
             sources: Seq[String],
             bigquery: BigQuery): TableInfo = {

    import scala.collection.JavaConverters.seqAsJavaListConverter

    val tableDefinition = ExternalTableDefinition
      .newBuilder(sources.map(_ + "/part*").asJava, null, FormatOptions.orc())
      .build()

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinition)
      .setExpirationTime(defaultExpiration)
      .build()

    bigquery.create(tableInfo)

    tableInfo
  }

  def createExternalTable(project: String,
                          dataset: String,
                          table: String,
                          tableMetadata: CatalogTable,
                          part: CatalogTablePartition,
                          bigquery: BigQuery): TableInfo = {
    val extTableName = validBigQueryTableName(table + "_" + part.spec.values.mkString("_"))
    val partCols = tableMetadata.partitionColumnNames.toSet

    val partSchema = StructType(tableMetadata.schema.filterNot(x => partCols.contains(x.name)))

    create(TableId.of(project, dataset, extTableName),
           schema = convertStructType(partSchema),
           sources = Seq(part.location.toString),
           bigquery = bigquery)
  }

  def loadParts(project: String,
                dataset: String,
                tableName: String,
                catalogTable: CatalogTable,
                parts: Seq[CatalogTablePartition],
                bigquery: BigQuery): Seq[TableResult] = {
    parts.map { part =>
      val extTable = createExternalTable(
        project, dataset, tableName,
        catalogTable, part, bigquery)

      val renameOrcCols = hasOrcPositionalColNames(extTable.getTableId, bigquery)

      loadPart(TableId.of(project, dataset, tableName),
        catalogTable.schema,
        catalogTable.partitionColumnNames,
        part.spec.values.toSeq,
        extTable.getTableId,
        bigquery,
        renameOrcCols = renameOrcCols)
    }
  }

  def loadPart(destTableId: TableId,
               schema: StructType,
               partColNames: Seq[String],
               partValues: Seq[String],
               extTableId: TableId,
               bigquery: BigQuery,
               overwrite: Boolean = false,
               batch: Boolean = true,
               renameOrcCols: Boolean = false): TableResult = {
    val sql = generateSelectFromEternalTable(extTableId, partColNames, schema, partValues, renameOrcCols)

    val query = QueryJobConfiguration
      .newBuilder(sql)
      .setCreateDisposition(CreateDisposition.CREATE_NEVER)
      .setWriteDisposition(if (!overwrite) WriteDisposition.WRITE_APPEND else WriteDisposition.WRITE_TRUNCATE)
      .setDestinationTable(destTableId)
      .setPriority(if (batch) Priority.BATCH else Priority.INTERACTIVE)
      .setUseLegacySql(false)
      .setUseQueryCache(false)
      .build()

    val jobId = validJobId(s"load_${destTableId.getTable}_${partValues.mkString("_")}_${System.currentTimeMillis()/1000}")

    bigquery.query(query, JobId.newBuilder()
      .setProject(extTableId.getProject)
      .setLocation("US")
      .setJob(jobId)
      .build())
  }

  def hasOrcPositionalColNames(table: TableId, bigquery: BigQuery): Boolean = {
    import scala.collection.JavaConverters.asScalaIteratorConverter
    bigquery.getTable(table)
      .getDefinition[ExternalTableDefinition]
      .getSchema
      .getFields.iterator.asScala
      .forall(f => f.getName.startsWith("_col"))
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

  def validJobId(s: String): String = {
    s.filter(c =>
      (c >= '0' && c <= '9') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= 'a' && c <= 'z') ||
      c == '_' || c == '-'
    ).take(1024)
  }

  def generateSelectFromEternalTable(extTable: TableId,
                                     partColNames: Seq[String],
                                     schema: StructType,
                                     partValues: Seq[String],
                                     renameOrcCols: Boolean = false) = {
    val partCols = partColNames.toSet

    val fields = schema
      .filterNot(field => partCols.contains(field.name))

    val partVals = partColNames
      .zip(partValues)
      .map{x => s"'${x._2}' as ${x._1}"}

    val data = if (renameOrcCols) {
      // handle positional column naming
      fields.zipWithIndex.map{x => s"_col${x._2+1} as ${x._1.name}"}
    } else {
      fields.map{x => s"${x.name}"}
    }

    val tableSpec = extTable.getProject + "." + extTable.getDataset + "." + extTable.getTable
    s"""select
       |  ${(partVals ++ data).mkString(",\n  ")}
       |from `$tableSpec`""".stripMargin
  }

  def findParts(db: String, table: String, partCol: String, target: String, partFilters: Map[String,String], spark: SparkSession): Seq[CatalogTablePartition] = {
    val cat = spark.sessionState.catalog.externalCatalog
    val colNames = cat.getTable(db, table).partitionColumnNames
    cat.listPartitions(db, table)
      .filter{partition =>
        val partMap = colNames.zip(partition.spec.values).toMap
        filterPartition(partMap, partFilters) &&
        partMap
          .exists(y => y._1 == partCol & y._2 == target)
      }
  }

  def filterPartition(partValues: Map[String,String], partFilters: Map[String,String]): Boolean = {
    for ((k,v) <- partFilters) {
      if (!partValues.get(k).contains(v)) return false
    }
    true
  }
}
