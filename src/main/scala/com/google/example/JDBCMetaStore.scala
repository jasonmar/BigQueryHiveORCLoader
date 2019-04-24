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

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Types}

import com.google.example.MetaStore.{MetaStore, Partition, TableMetadata, mkPartSpec, parsePartitionDesc, parsePartitionTable, parseTableDesc}
import com.google.example.PartitionFilters.PartitionFilter
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object JDBCMetaStore {
  val HiveDriver = "org.apache.hive.jdbc.HiveDriver"

  def connect(jdbcUrl: String): Connection = {
    Class.forName(HiveDriver)
    DriverManager.getConnection(jdbcUrl)
  }

  def convertSqlType(sqlType: Int): DataType = {
    sqlType match {
      case Types.VARCHAR => StringType
      case Types.INTEGER => IntegerType
      case Types.BIGINT => LongType
      case Types.BOOLEAN => BooleanType
      case Types.DOUBLE => DoubleType
      case Types.REAL => DoubleType
      case Types.FLOAT => FloatType
      case Types.CHAR => StringType
      case Types.LONGNVARCHAR => StringType
      case Types.NCHAR => StringType
      case Types.NVARCHAR => StringType
      case Types.SMALLINT => IntegerType
      case Types.TINYINT => IntegerType
      case _ => throw new IllegalArgumentException(s"unsupported type: $sqlType")
    }
  }

  def convertResultSetMetadataToStructType(meta: ResultSetMetaData): StructType = {
    val structFields: Seq[StructField] =
      (1 to meta.getColumnCount).map{i =>
        StructField(
          name = meta.getColumnName(i),
          dataType = convertSqlType(meta.getColumnType(i))
        )
      }
    StructType(structFields)
  }

  def readRowsFromResultSet(rs: ResultSet): Seq[Row] = {
    val buf = ListBuffer.empty[Row]
    while (rs.next()) {
      val colValues: Seq[Any] =
        (1 to rs.getMetaData.getColumnCount)
          .map(rs.getObject)
      val row: Row = Row(colValues: _*)
      buf.append(row)
    }
    buf.result()
  }

  def convertResultSetToDataFrame(rs: ResultSet, spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext.makeRDD(readRowsFromResultSet(rs), numSlices = 1)
    val schema = convertResultSetMetadataToStructType(rs.getMetaData)
    spark.createDataFrame(rdd, schema)
  }

  def query(query: String, con: Connection, spark: SparkSession): DataFrame = {
    val stmt = con.createStatement
    val rs = stmt.executeQuery(query)
    convertResultSetToDataFrame(rs, spark)
  }
}

case class JDBCMetaStore(jdbcUrl: String, spark: SparkSession) extends MetaStore {
  @transient
  private val con = JDBCMetaStore.connect(jdbcUrl)

  private def sql(query: String): DataFrame =
    JDBCMetaStore.query(query, con, spark)

  override def listPartitions(db: String, table: String, partitionFilter: Option[PartitionFilter] = None): Seq[Partition] = {
    parsePartitionTable(sql(s"show partitions $db.$table"))
      .filter{partValues =>
        partitionFilter.exists(_.filterPartition(partValues))
      }
      .flatMap{partValues =>
        val partSpec = mkPartSpec(partValues)
        val df = sql(s"describe formatted $db.$table partition($partSpec)")
        parsePartitionDesc(partValues, df)
      }
  }

  override def getTable(db: String, table: String): TableMetadata = {
    parseTableDesc(sql(s"describe formatted $db.$table"))
  }
}
