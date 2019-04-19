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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object MetaStore {
  case class TableMetadata(schema: StructType, partitionColumnNames: Seq[String])

  case class Partition(values: Seq[(String,String)], location: String)

  trait PartitionProvider {
    def findParts(db: String, table: String, partFilters: String): Seq[Partition]
    def listPartitions(db: String, table: String): Seq[Partition]
    def getTable(db: String, table: String): TableMetadata
  }

  case class ExternalCatalog(spark: SparkSession) extends PartitionProvider {
    private val cat = spark.sessionState.catalog.externalCatalog
    override def findParts(db: String, table: String, partFilters: String): Seq[Partition] = {
      val filters = PartitionFilters.parse(partFilters)
      listPartitions(db, table).filter(PartitionFilters.filter(_, filters))
    }

    override def listPartitions(db: String, table: String): Seq[Partition] = {
      val colNames = getTable(db, table).partitionColumnNames
      cat.listPartitions(db, table)
        .map{part =>
          val values = colNames.zip(part.spec.values)
          Partition(values, part.location.toString)
        }
    }

    override def getTable(db: String, table: String): TableMetadata = {
      val tbl = cat.getTable(db, table)
      TableMetadata(tbl.schema, tbl.partitionColumnNames)
    }
  }

  // convert region=EU/date=2019-04-11 to "region" -> "EU", "date" -> "2019-04-11"
  def readPartSpec(s: String): Seq[(String,String)] = {
    s.split('/')
      .map{p =>
        p.split('=') match {
          case Array(l,r) =>
            (l,r.stripPrefix("'").stripSuffix("'"))
        }
      }
  }

  def mkPartSpec(partValues: Iterable[(String,String)]): String =
    partValues.map{x => s"${x._1}='${x._2}'"}.mkString(",")

  case class SparkSQL(spark: SparkSession) extends PartitionProvider {
    override def findParts(db: String, table: String, partFilters: String): Seq[Partition] = {
      val filters = PartitionFilters.parse(partFilters)
      listPartitions(db, table).filter(PartitionFilters.filter(_, filters))
    }

    override def listPartitions(db: String, table: String): Seq[Partition] = {
      val partValues: Seq[Seq[(String,String)]] = spark.sql(s"show partitions $table")
        .collect()
        .map{row =>
          val partition = row.getString(row.fieldIndex("partition"))
          readPartSpec(partition)
        }

      partValues.map{partValues =>
        val partSpec = mkPartSpec(partValues)
        val data = spark
          .sql(s"describe formatted $table partition($partSpec)")
          .drop("comment")
          .collect()
          .map{row =>
            val colName = row.getString(row.fieldIndex("col_name"))
            val dataType = row.getString(row.fieldIndex("data_type"))
            (colName, dataType)
          }
        parsePartitionDetails(partValues, data)
      }
    }

    override def getTable(db: String, table: String): TableMetadata = {
      val data = spark.sql(s"describe formatted $table")
        .drop("comment")
        .collect()
        .map{row =>
          val colName = row.getString(row.fieldIndex("col_name"))
          val dataType = row.getString(row.fieldIndex("data_type"))
          (colName, dataType)
        }
      parseTableDetails(data)
    }

    private def parseTableDetails(data: Seq[(String,String)]): TableMetadata = {
      val fields = data.takeWhile(!_._1.startsWith("#"))
        .map(Mapping.convertTuple)
      val schema = StructType(fields)

      val partColNames = data.map(_._1)
        .dropWhile(!_.startsWith("#"))
        .dropWhile(_.startsWith("#"))
        .takeWhile(_.trim.nonEmpty)

      TableMetadata(schema, partColNames)
    }

    private def parsePartitionDetails(partValues: Seq[(String,String)], data: Seq[(String,String)]): Partition = {
      val location = data.filter(_._1 == "Location").head._2
      Partition(partValues, location)
    }
  }
}
