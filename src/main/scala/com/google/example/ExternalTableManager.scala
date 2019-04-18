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
import com.google.example.MetaStore.{Partition, TableMetadata}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

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
                          tableMetadata: TableMetadata,
                          part: Partition,
                          bigquery: BigQuery): TableInfo = {
    val extTableName = validBigQueryTableName(table + "_" + part.values.mkString("_"))
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
                catalogTable: TableMetadata,
                parts: Seq[Partition],
                bigquery: BigQuery): Seq[TableResult] = {
    parts.map { part =>
      val extTable = createExternalTable(
        project, dataset, tableName,
        catalogTable, part, bigquery)

      val renameOrcCols = hasOrcPositionalColNames(extTable.getTableId, bigquery)

      loadPart(TableId.of(project, dataset, tableName),
        catalogTable.schema,
        catalogTable.partitionColumnNames,
        part.values,
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

  // TODO convert week number to date
  // TODO add format option
  def generateSelectFromEternalTable(extTable: TableId,
                                     partColNames: Seq[String],
                                     schema: StructType,
                                     partValues: Seq[String],
                                     renameOrcCols: Boolean = false): String = {
    val partCols = partColNames.toSet

    val fields = schema
      .filterNot(field => partCols.contains(field.name))

    val partVals = partColNames
      .zip(partValues)
      .map{x =>
        schema.find(_.name == x._1) match {
          case Some(field) if field.dataType.typeName == IntegerType.typeName || field.dataType.typeName == LongType.typeName =>
            s"${x._2} as ${x._1}"
          case _ =>
            s"'${x._2}' as ${x._1}"
        }
      }

    val data = if (renameOrcCols) {
      // handle positional column naming
      fields.zipWithIndex.map{x => s"_col${x._2} as ${x._1.name}"}
    } else {
      fields.map{x => s"${x.name}"}
    }

    val tableSpec = extTable.getProject + "." + extTable.getDataset + "." + extTable.getTable
    s"""select
       |  ${(partVals ++ data).mkString(",\n  ")}
       |from `$tableSpec`""".stripMargin
  }

  def findParts(db: String, table: String, partFilters: String, spark: SparkSession): Seq[CatalogTablePartition] = {
    val cat = spark.sessionState.catalog.externalCatalog
    val colNames = cat.getTable(db, table).partitionColumnNames
    cat.listPartitions(db, table)
      .filter{partition =>
        val partMap = colNames.zip(partition.spec.values).toMap
        filterPartition(partMap, partFilters)
      }
  }

  def filterPartition(partValues: Map[String,String], whereClause: String): Boolean = {
    for (x <- partValues) {
      parseFilters(whereClause).get(x._1) match {
        case Some(filters) if filters.exists{!_(x)} =>
          return false
        case _ =>
      }
    }
    true
  }

  sealed trait PartFilter {
    def apply(part: (String, String)): Boolean
  }

  case class Equals(l: String, r: String) extends PartFilter {
    override def apply(part: (String, String)): Boolean = {
      l == part._1 && (r == "*" || r == part._2)
    }
  }

  case class GreaterThan(l: String, r: String) extends PartFilter {
    override def apply(part: (String, String)): Boolean = {
      l == part._1 && r > part._2
    }
  }

  case class LessThan(l: String, r: String) extends PartFilter {
    override def apply(part: (String, String)): Boolean = {
      l == part._1 && r < part._2
    }
  }

  case class GreaterThanOrEq(l: String, r: String) extends PartFilter {
    override def apply(part: (String, String)): Boolean = {
      l == part._1 && r >= part._2
    }
  }

  case class LessThanOrEq(l: String, r: String) extends PartFilter {
    override def apply(part: (String, String)): Boolean = {
      l == part._1 && r <= part._2
    }
  }

  case class In(l: String, r: Set[String]) extends PartFilter {
    override def apply(part: (String, String)): Boolean = {
      l == part._1 && r.contains(part._2)
    }
  }

  def parseFilters(partFilters: String): Map[String,Seq[PartFilter]] = {
    if (partFilters.nonEmpty){
      partFilters.replaceAllLiterally(" and ", " AND ")
        .replaceAllLiterally(" And ", " AND ")
        .replaceAllLiterally(" in ", " IN ")
        .replaceAllLiterally(" In ", " IN ")
        .split(" AND ")
        .flatMap(parseFilter)
        .groupBy(_._1)
        .mapValues{_.map{_._2}.toSeq}
    } else Map.empty
  }

  def parseFilter(expr: String): Option[(String,PartFilter)] = {
    if (expr.contains('=')) {
      expr.split('=').map(_.trim) match {
        case Array(l,r) => Option((l, Equals(l, r)))
        case _ => None
      }
    } else if (expr.contains("<=")) {
      expr.split("<=").map(_.trim) match {
        case Array(l,r) => Option((l, LessThanOrEq(l, r)))
        case _ => None
      }
    } else if (expr.contains(">=")) {
      expr.split(">=").map(_.trim) match {
        case Array(l,r) => Option((l, GreaterThanOrEq(l, r)))
        case _ => None
      }
    } else if (expr.contains('<')) {
      expr.split('<').map(_.trim) match {
        case Array(l,r) => Option((l, LessThan(l, r)))
        case _ => None
      }
    } else if (expr.contains('>')) {
      expr.split('>').map(_.trim) match {
        case Array(l,r) => Option((l, GreaterThan(l, r)))
        case _ => None
      }
    } else if (expr.contains(" IN ")) {
      expr.split(" IN ").map(_.trim) match {
        case Array(l,r) =>
          val set = r.stripPrefix("(").stripSuffix(")")
            .split(',').map(_.trim).toSet
          Option((l, In(l, set)))
        case _ => None
      }
    } else None
  }
}
