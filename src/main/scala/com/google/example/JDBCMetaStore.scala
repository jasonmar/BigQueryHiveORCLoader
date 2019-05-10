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

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Types}

import com.google.example.JDBCMetaStore._
import com.google.example.MetaStore.{MetaStore, Partition, TableMetadata, mkPartSpec, parsePartitionTable}
import com.google.example.PartitionFilters.PartitionFilter
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object JDBCMetaStore extends Logging {
  def connect(jdbcUrl: String): Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
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

  case class ResultSetIterator(private val rs: ResultSet) extends Iterator[Seq[String]] {
    private val n = rs.getMetaData.getColumnCount

    override def hasNext: Boolean = rs.next()

    override def next(): Seq[String] =
      Option((0 until n)
        .map{i =>
          Option(rs.getObject(i+1))
            .map(_.toString)
            .getOrElse("")
        }).orNull
  }

  case class StatementIterator(stmt: PreparedStatement) extends Iterator[ResultSet] {
    private var rs: ResultSet = _
    override def hasNext: Boolean = {
      rs = stmt.getResultSet
      rs != null
    }
    override def next(): ResultSet = rs
  }

  def executeQueries(queries: Seq[String], con: Connection) : Seq[Seq[Seq[String]]] = {
    val stmt = con.prepareStatement(queries.mkString(";\n\n"))
    if (stmt.execute()) {
      val buf = ArrayBuffer.empty[Seq[Seq[String]]]
      StatementIterator(stmt)
        .takeWhile(_ != null)
        .foreach{rs =>
          buf.append(ResultSetIterator(rs)
            .takeWhile(_ != null)
            .toArray.toSeq)
        }
      buf.result().toArray.toSeq
    } else Seq.empty
  }

  def executeQuery(query: String, con: Connection, spark: SparkSession): DataFrame = {
    val stmt = con.createStatement
    val rs = stmt.executeQuery(query)
    convertResultSetToDataFrame(rs, spark)
  }

  def parseTableDesc(df: DataFrame): TableMetadata = {
    val tuples = df.drop("comment")
      .collect()
      .map{row =>
        val colName = Option(row.getString(row.fieldIndex("col_name"))).getOrElse("").trim
        val dataType = Option(row.getString(row.fieldIndex("data_type"))).getOrElse("").trim
        (colName, dataType)
      }
    parseTableDetails(tuples)
  }

  def parseTableDetails(data: Seq[(String,String)]): TableMetadata = {
    val fields = data
      .takeWhile(!_._1.startsWith("# Detailed Table Information"))
      .filterNot(x => x._1.startsWith("#") || x._1.isEmpty)
      .map(Mapping.convertTuple)

    val schema = StructType(fields)

    val partColNames = data.map(_._1)
      .dropWhile(!_.startsWith("# Partition Information"))
      .takeWhile(!_.startsWith("# Detailed Table Information"))
      .filterNot(x => x.startsWith("#") || x.isEmpty)

    val location = data.find(_._1.startsWith("Location")).map(_._2)

    TableMetadata(schema, partColNames, location, data)
  }

  def parsePartitionDesc(partValues: Seq[(String,String)], df: DataFrame): Option[Partition] = {
    val tuples = df.drop("comment")
      .collect()
      .map{row =>
        val colName = Option(row.getString(row.fieldIndex("col_name")))
          .getOrElse("").trim
        val dataType = Option(row.getString(row.fieldIndex("data_type")))
          .getOrElse("").trim
        (colName, dataType)
      }
    parsePartitionDetails(partValues, tuples)
  }

  def parsePartitionDetails(partValues: Seq[(String,String)], data: Seq[(String,String)]): Option[Partition] = {
    data.find(_._1.startsWith("Location"))
      .map{x =>
        val location = x._2
        Partition(partValues, location)
      }
  }
}

case class JDBCMetaStore(jdbcUrl: String, spark: SparkSession) extends MetaStore {
  @transient
  private val con = connect(jdbcUrl)

  private def sql(query: String): DataFrame =
    executeQuery(query, con, spark)

  override def listPartitions(db: String, table: String, partitionFilter: Option[PartitionFilter] = None): Seq[Partition] = {
    val partitions = parsePartitionTable(sql(s"show partitions $db.$table"))
      .filter{partValues =>
        partitionFilter.exists(_.filterPartition(partValues))
      }

    val sqlQueries = partitions
      .map{partValues =>
        val partSpec = mkPartSpec(partValues)
        s"describe formatted $db.$table partition($partSpec)"
      }

    val results: Seq[Seq[(String,String)]] = executeQueries(sqlQueries, con)
      .map{_.map{row => (row.headOption.getOrElse(""), row.lift(1).getOrElse(""))}}

    import spark.implicits._
    partitions
      .zip(results)
      .flatMap{x =>
        val (partValues, describeRows) = x
        val df = describeRows.toDF("col_name", "data_type")
        parsePartitionDesc(partValues, df)
      }
  }

  override def getTable(db: String, table: String): TableMetadata = {
    parseTableDesc(sql(s"describe formatted $db.$table"))
  }
}
