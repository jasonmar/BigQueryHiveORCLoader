package com.google.example

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, TableId}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class ExternalTableManagerSpec extends FlatSpec with BeforeAndAfterAll{
  private var client: Option[BigQuery] = None

  override def beforeAll(): Unit = {
    client = Option(BigQueryOptions.getDefaultInstance.getService)
  }

  def getBigQuery: BigQuery = client.get

  "ExternalTableManager" should "Generate SQL" in {
    val extTable = TableId.of("project", "dataset")
    val partColNames = Seq("date", "region")
    val partValues = Seq("2019-04-11", "US")
    val schema = StructType(Seq(
      StructField("date", StringType),
      StructField("region", StringType),
      StructField("id", StringType),
      StructField("x", IntegerType),
      StructField("y", LongType),
      StructField("z", DoubleType)
    ))
    val generatedSql = ExternalTableManager.genSql2(extTable, partColNames, schema, partValues)
    val expectedSql =
      """select
        |  date as '2019-04-11',
        |  region as 'US',
        |  _col1 as id,
        |  _col2 as x,
        |  _col3 as y,
        |  _col4 as z
        |from project.dataset""".stripMargin
    assert(generatedSql == expectedSql)
  }
}
