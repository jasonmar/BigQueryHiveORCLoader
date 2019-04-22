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
import com.google.cloud.storage.Storage
import com.google.example.Mapping.convertStructType
import com.google.example.MetaStore.{Partition, TableMetadata}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object ExternalTableManager {
  sealed trait StorageFormat
  case object Orc extends StorageFormat
  case object Parquet extends StorageFormat
  case object Avro extends StorageFormat

  def parseStorageFormat(s: String): StorageFormat = {
    s.toLowerCase match {
      case "orc" => Orc
      case "parquet" => Parquet
      case "avro" => Avro
      case _ => throw new IllegalArgumentException("invalid storage format")
    }
  }

  def defaultExpiration: Long = System.currentTimeMillis() + 1000*60*60*24*2 // 2 days

  def create(tableId: TableId,
             schema: Schema,
             sources: Seq[String],
             storageFormat: StorageFormat,
             bigquery: BigQuery): TableInfo = {

    import scala.collection.JavaConverters.seqAsJavaListConverter

    val tableDefinition = storageFormat match {
      case Orc =>
        ExternalTableDefinition
          .newBuilder(sources.asJava, null, FormatOptions.orc())
          .build()
      case Parquet =>
        ExternalTableDefinition
          .newBuilder(sources.asJava, schema, FormatOptions.parquet())
          .build()
      case Avro =>
        ExternalTableDefinition
          .newBuilder(sources.asJava, schema, FormatOptions.avro())
          .build()
    }

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinition)
      .setExpirationTime(defaultExpiration)
      .build()

    bigquery.create(tableInfo)

    tableInfo
  }

  def resolveLocations(part: Partition, gcs: Storage): Seq[String] = {
    val bucket = part.location.stripPrefix("gs://").takeWhile(_ != '/')
    val path = part.location.stripPrefix("gs://").dropWhile(_ != '/')
    val options = Seq(
      Storage.BlobListOption.prefix(path+"/"),
      Storage.BlobListOption.fields(Storage.BlobField.NAME)
    )
    import scala.collection.JavaConverters.iterableAsScalaIterableConverter
    gcs.list(bucket, options:_*).iterateAll().asScala.toArray
      .filterNot(_.getName.startsWith("."))
      .map{obj => s"gs://$bucket/${obj.getName}"}
  }

  def createExternalTable(project: String,
                          dataset: String,
                          table: String,
                          tableMetadata: TableMetadata,
                          part: Partition,
                          storageFormat: StorageFormat,
                          bigquery: BigQuery,
                          gcs: Storage): TableInfo = {
    val extTableName = validBigQueryTableName(table + "_" + part.values.mkString("_"))
    val partCols = tableMetadata.partitionColumnNames.toSet

    val partSchema = StructType(tableMetadata.schema.filterNot(x => partCols.contains(x.name)))

    val sources = resolveLocations(part, gcs)

    create(TableId.of(project, dataset, extTableName),
           schema = convertStructType(partSchema),
           sources = sources,
           storageFormat = Orc,
           bigquery = bigquery)
  }

  def loadParts(project: String,
                dataset: String,
                tableName: String,
                tableMetadata: TableMetadata,
                partitions: Seq[Partition],
                storageFormat: StorageFormat,
                bigquery: BigQuery,
                gcs: Storage): Seq[TableResult] = {
    partitions.map { part =>
      val extTable = createExternalTable(
        project, dataset, tableName,
        tableMetadata, part, storageFormat, bigquery, gcs)

      val renameOrcCols = hasOrcPositionalColNames(extTable.getTableId, bigquery)

      loadPart(
        destTableId = TableId.of(project, dataset, tableName),
        schema = tableMetadata.schema,
        partition = part,
        extTableId = extTable.getTableId,
        bigquery = bigquery,
        renameOrcCols = renameOrcCols)
    }
  }

  def loadPart(destTableId: TableId,
               schema: StructType,
               partition: Partition,
               extTableId: TableId,
               bigquery: BigQuery,
               overwrite: Boolean = false,
               batch: Boolean = true,
               renameOrcCols: Boolean = false): TableResult = {
    val sql = generateSelectFromExternalTable(extTableId, schema, partition, renameOrcCols)

    val query = QueryJobConfiguration
      .newBuilder(sql)
      .setCreateDisposition(CreateDisposition.CREATE_NEVER)
      .setWriteDisposition(if (!overwrite) WriteDisposition.WRITE_APPEND else WriteDisposition.WRITE_TRUNCATE)
      .setDestinationTable(destTableId)
      .setPriority(if (batch) Priority.BATCH else Priority.INTERACTIVE)
      .setUseLegacySql(false)
      .setUseQueryCache(false)
      .build()

    bigquery.query(query, JobId.newBuilder()
      .setProject(extTableId.getProject)
      .setLocation("US")
      .setJob(jobid(destTableId, partition))
      .build())
  }

  def jobid(table: TableId, partition: Partition): String = {
    Seq(
      "load",
      table.getDataset,
      table.getTable,
      partition.values.map(_._2).mkString("_"),
      (System.currentTimeMillis()/1000).toString
    ).mkString("_")
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
  def generateSelectFromExternalTable(extTable: TableId,
                                      schema: StructType,
                                      partition: Partition,
                                      renameOrcCols: Boolean = false): String = {
    val fields = partition.values.map(_._1)

    val partVals = partition.values
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
      fields.zipWithIndex.map{x => s"_col${x._2} as ${x._1}"}
    } else {
      fields
    }

    val tableSpec = extTable.getProject + "." + extTable.getDataset + "." + extTable.getTable
    s"""select
       |  ${(partVals ++ data).mkString(",\n  ")}
       |from `$tableSpec`""".stripMargin
  }
}
