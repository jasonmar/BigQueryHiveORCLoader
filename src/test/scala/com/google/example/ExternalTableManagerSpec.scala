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

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, StandardTableDefinition, TableId, TableInfo}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}


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

  val TestProject = "retail-poc-demo"
  val TestBucket = "bq_hive_load_demo"
  val TestTable = s"test_1541"

  val PartColNames = Seq("date", "region")
  val PartValues = Seq("2019-04-11", "US")
  val LoadDataset = "load_tmp"
  val TargetDataset = "load_target"
  val ExtTableId = TableId.of(TestProject,LoadDataset,TestTable+"_ext")
  val TargetTableId = TableId.of(TestProject,TargetDataset,TestTable)

  def newClient(): BigQuery = {
    BigQueryOptions
      .getDefaultInstance
      .toBuilder
      .setProjectId(TestProject)
      .build()
      .getService
  }

  override def beforeAll(): Unit = {
    client = Option(newClient())
  }

  def getBigQuery: BigQuery = {
    if (client.isDefined) client.get
    else {
      client = Option(newClient())
      client.get
    }
  }

  "ExternalTableManager" should "Generate SQL" in {
    val extTable = TableId.of("project", "dataset", "table")
    val generatedSql = ExternalTableManager.generateSelectFromEternalTable(extTable, PartColNames, TestSchema, PartValues)
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
    val tbl = bq.create(TableInfo.of(TargetTableId, StandardTableDefinition.of(Mapping.convertStructType(TestSchema))))
    Thread.sleep(5000)
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
