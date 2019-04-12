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
      .newBuilder(sources.map(_ + "/part*").asJava, null, FormatOptions.orc()).build()

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

      loadPart(TableId.of(project, dataset, tableName),
        catalogTable.schema,
        catalogTable.partitionColumnNames,
        part.spec.values.toSeq,
        extTable.getTableId,
        bigquery)
    }
  }

  def loadPart(destTableId: TableId,
               schema: StructType,
               partColNames: Seq[String],
               partValues: Seq[String],
               extTableId: TableId,
               bigquery: BigQuery,
               batch: Boolean = true): TableResult = {

    val sql = genSql2(extTableId, partColNames, schema, partValues)

    val query = QueryJobConfiguration
      .newBuilder(sql)
      .setCreateDisposition(CreateDisposition.CREATE_NEVER)
      .setWriteDisposition(WriteDisposition.WRITE_APPEND)
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

  def genSql(extTable: TableId, table: CatalogTable, part: CatalogTablePartition): String = {
    genSql2(extTable, table.partitionColumnNames, table.schema, part.spec.values.toSeq)
  }

  def genSql2(extTable: TableId,
              partColNames: Seq[String],
              schema: StructType,
              partValues: Seq[String]) = {
    val partCols = partColNames.toSet

    val renamedCols = schema
      .filterNot(field => partCols.contains(field.name))
      .zipWithIndex
      .map{x =>
        s"${x._1.name} as ${x._1.name}"
      }

    val partVals = partColNames
      .zip(partValues)
      .map{x => s"'${x._2}' as ${x._1}"}

    val tableSpec = extTable.getProject + "." + extTable.getDataset + "." + extTable.getTable
    s"""select
       |  ${partVals.mkString("", ",\n  ",",")}
       |  ${renamedCols.mkString(",\n  ")}
       |from `$tableSpec`""".stripMargin
  }

  def findParts(db: String, table: String, partCol: String, target: String, spark: SparkSession): Seq[CatalogTablePartition] = {
    val cat = spark.sessionState.catalog.externalCatalog
    val colNames = cat.getTable(db, table).partitionColumnNames
    cat.listPartitions(db, table)
      .filter{partition =>
        colNames.zip(partition.spec.values)
          .exists(y => y._1 == partCol & y._2 == target)
      }
  }
}
