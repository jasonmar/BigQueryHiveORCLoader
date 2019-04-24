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

import java.sql.Blob

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

  def defaultExpiration: Long = System.currentTimeMillis() + 1000*60*60*6 // 6 hours

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

    val tableExists = scala.Option(bigquery.getTable(tableInfo.getTableId))
      .map(_.exists).contains(true)
    if (tableExists)
      bigquery.create(tableInfo)

    tableInfo
  }

  def resolveLocations(part: Partition, gcs: Storage): Seq[String] ={
    listObjects(part.location, gcs)
  }

  def listObjects(gsUri: String, gcs: Storage): Seq[String] = {
    val bucket = gsUri.stripPrefix("gs://").takeWhile(_ != '/')
    val path = gsUri.stripPrefix(s"gs://$bucket/").stripSuffix("/")
    val prefix = s"$path/"
    val options = Seq(
      Storage.BlobListOption.prefix(prefix),
      Storage.BlobListOption.fields(Storage.BlobField.NAME)
    )
    import scala.collection.JavaConverters.iterableAsScalaIterableConverter
    val blobs = gcs.list(bucket, options:_*)
      .iterateAll().asScala.toArray
      .filterNot{obj =>
        val fileName = obj.getName.stripPrefix(prefix)
        fileName.startsWith(".") || fileName.endsWith("/") || fileName.isEmpty
      }

    val hasDotFiles = blobs.exists{obj =>
      obj.getName.stripPrefix(prefix).startsWith(".")
    }

    val partPrefix = blobs.forall{obj =>
      obj.getName.stripPrefix(prefix).startsWith("part")
    }

    val numPrefix = blobs.forall{obj =>
      obj.getName.stripPrefix(prefix).startsWith("0")
    }

    if (!hasDotFiles) {
      Seq(s"$path/*")
    } else if (partPrefix) {
      Seq(s"$path/part*")
    } else if (numPrefix) {
      Seq(s"$path/0*")
    } else {
      val locations = blobs.filterNot{obj =>
        val fileName = obj.getName.stripPrefix(prefix)
        fileName.startsWith(".") || fileName.endsWith("/") || fileName.isEmpty
      }
        .map{obj => s"gs://$bucket/${obj.getName}"}

      require(locations.length <= 500, "partition has more than 500 objects - remove files beginning with '.' to enable * wildcard matching")

    locations
    }
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
    if (sources.nonEmpty)
      System.out.println(s"Creating external table with sources '${sources.mkString(", ")}'")
    else
      throw new RuntimeException(s"No sources found for ${part.location}")

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
                unusedColumnName: String,
                partColFormats: Map[String,String],
                storageFormat: StorageFormat,
                bigqueryCreate: BigQuery,
                bigqueryWrite: BigQuery,
                overwrite: Boolean,
                batch: Boolean,
                gcs: Storage): Seq[TableResult] = {
    partitions.map { part =>
      val extTable = createExternalTable(
        project, dataset, tableName,
        tableMetadata, part, storageFormat, bigqueryCreate, gcs)

      val renameOrcCols = hasOrcPositionalColNames(extTable.getTableId, bigqueryCreate)

      loadPart(
        destTableId = TableId.of(project, dataset, tableName),
        schema = tableMetadata.schema,
        partition = part,
        extTableId = extTable.getTableId,
        unusedColumnName = unusedColumnName,
        partColFormats = partColFormats,
        bigqueryWrite = bigqueryWrite,
        batch = batch,
        overwrite = overwrite,
        renameOrcCols = renameOrcCols)
    }
  }

  def loadPart(destTableId: TableId,
               schema: StructType,
               partition: Partition,
               extTableId: TableId,
               unusedColumnName: String,
               partColFormats: Map[String,String],
               bigqueryWrite: BigQuery,
               overwrite: Boolean = false,
               batch: Boolean = true,
               renameOrcCols: Boolean = false): TableResult = {
    val sql = generateSelectFromExternalTable(
      extTable = extTableId,
      schema = schema,
      partition = partition,
      unusedColumnName = unusedColumnName,
      formats = partColFormats,
      renameOrcCols = renameOrcCols)

    val query = QueryJobConfiguration
      .newBuilder(sql)
      .setCreateDisposition(CreateDisposition.CREATE_NEVER)
      .setWriteDisposition(if (!overwrite) WriteDisposition.WRITE_APPEND else WriteDisposition.WRITE_TRUNCATE)
      .setDestinationTable(destTableId)
      .setPriority(if (batch) Priority.BATCH else Priority.INTERACTIVE)
      .setUseLegacySql(false)
      .setUseQueryCache(false)
      .build()

    bigqueryWrite.query(query, JobId.newBuilder()
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

  /** Interprets partition column value string according to a known format
    *
    * @param colName name of column to be formatted
    * @param colValue value to be formatted
    * @param colFormat format string used to identify conversion function
    * @return SQL fragment "$colValue as '$colName'"
    */
  def format(colName: String, colValue: String, colFormat: String): String = {
    if (colFormat == "YYYYMM") {
      val year = colValue.substring(0,4)
      val month = colValue.substring(4,6)
      s"'$year-$month-01' as $colName"
    } else if (colFormat.toLowerCase == "week") {
      // TODO convert week number to date
      throw new NotImplementedError("conversion of week number to date not implemented")
    } else {
      throw new IllegalArgumentException(s"unsupported colFormat '$colFormat'")
    }
  }

  def generateSelectFromExternalTable(extTable: TableId,
                                      schema: StructType,
                                      partition: Partition,
                                      unusedColumnName: String,
                                      formats: Map[String,String] = Map.empty,
                                      renameOrcCols: Boolean = false): String = {
    // Columns from partition values
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
            case _ =>
              s"'$colValue' as $colName"
          }
        }
      }

    val partColNames: Set[String] = partition.values.map(_._1).toSet

    // Columns from partition values
    val data = if (renameOrcCols) {
      // handle positional column naming
      schema
        .filterNot(x => partColNames.contains(x.name))
        .zipWithIndex
        .map{x => s"_col${x._2} as ${x._1.name}"}
    } else {
      schema.filterNot(field => partColNames.contains(field.name))
        .map{x => s"${x.name}"}
    }

    val tableSpec = extTable.getProject + "." + extTable.getDataset + "." + extTable.getTable
    s"""select
       |  ${(partVals ++ data).mkString(",\n  ")}
       |from `$tableSpec`""".stripMargin
  }
}
