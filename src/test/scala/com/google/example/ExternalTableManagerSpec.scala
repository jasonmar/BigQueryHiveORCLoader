package com.google.example

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, StandardTableDefinition, TableId, TableInfo}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.util.Random
import sys.env

class ExternalTableManagerSpec extends FlatSpec with BeforeAndAfterAll{
  private var client: Option[BigQuery] = None

  val TestSchema = StructType(Seq(
    StructField("date", StringType),
    StructField("region", StringType),
    StructField("id", StringType),
    StructField("x", IntegerType),
    StructField("y", LongType),
    StructField("z", DoubleType)
  ))

  val TestORCSchema = StructType(Seq(
    StructField("id", StringType),
    StructField("x", IntegerType),
    StructField("y", LongType),
    StructField("z", DoubleType)
  ))

  val TestProject = "myproject"
  val TestBucket = "mybucket"
  val TestTable = s"test_${Random.nextInt(9999)}"

  val PartColNames = Seq("date", "region")
  val PartValues = Seq("2019-04-11", "US")
  val LoadDataset = "load_tmp"
  val TargetDataset = "load_target"
  val ExtTableId = TableId.of(TestProject,LoadDataset,TestTable+"_ext")
  val TargetTableId = TableId.of(TestProject,TargetDataset,TestTable)

  override def beforeAll(): Unit = {
    client = Option(BigQueryOptions
      .getDefaultInstance
      .toBuilder
      .setProjectId(TestProject)
      .build()
      .getService)
  }


  def getBigQuery: BigQuery = client.get

  "ExternalTableManager" should "Generate SQL" in {
    val extTable = TableId.of("project", "dataset", "table")
    val generatedSql = ExternalTableManager.genSql2(extTable, PartColNames, TestSchema, PartValues)
    val expectedSql =
      """select
        |  '2019-04-11' as date,
        |  'US' as region,
        |  id as id,
        |  x as x,
        |  y as y,
        |  z as z
        |from `project.dataset.table`""".stripMargin
    assert(generatedSql == expectedSql)
  }

  it should "define external table" in {
    val schema = Mapping.convertStructType(TestORCSchema)
    val locations = Seq(
      s"gs://$TestBucket/test/US_2019-04-11_part_11.snappy.orc"
    )
    val createdTable = ExternalTableManager.create(
      ExtTableId,
      schema,
      locations,
      getBigQuery)
    System.out.println(createdTable)
  }

  it should "select into new table" in {
    val bq = getBigQuery
    if (!bq.getTable(TargetTableId).exists()){
      val tbl = bq.create(TableInfo.of(TargetTableId, StandardTableDefinition.of(Mapping.convertStructType(TestSchema))))
      System.out.println(tbl.getTableId)
    }
    while (!bq.getTable(TargetTableId).exists()){
      Thread.sleep(2000)
    }
    ExternalTableManager.loadPart(
      TargetTableId,
      TestSchema,
      PartColNames,
      PartValues,
      ExtTableId,
      bq,
      batch = false
    )
  }
}
