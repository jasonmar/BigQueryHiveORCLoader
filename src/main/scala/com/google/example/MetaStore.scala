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

import com.google.example.ExternalTableManager.filterPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

object MetaStore {
  case class TableMetadata(schema: StructType, partitionColumnNames: Seq[String])

  case class Partition(values: Seq[String], location: String)

  trait PartitionProvider {
    def findParts(db: String, table: String, partFilters: String): Seq[Partition]
    def getTable(db: String, table: String): TableMetadata
  }

  case class ExternalCatalog(spark: SparkSession) extends PartitionProvider {
    private val cat = spark.sessionState.catalog.externalCatalog
    override def findParts(db: String, table: String, partFilters: String): Seq[Partition] = {
      val colNames = cat.getTable(db, table).partitionColumnNames
      cat.listPartitions(db, table)
        .filter{partition =>
          val partMap = colNames.zip(partition.spec.values).toMap
          filterPartition(partMap, partFilters)
        }
        .map{part =>
          Partition(part.spec.values.toArray.toSeq, part.location.toString)
        }
    }

    override def getTable(db: String, table: String): TableMetadata = {
      val tbl = cat.getTable(db, table)
      TableMetadata(tbl.schema, tbl.partitionColumnNames)
    }
  }

  case class SparkSQL(spark: SparkSession) extends PartitionProvider {
    override def findParts(db: String, table: String, partFilters: String): Seq[Partition] = {
      spark.sql(s"describe formatted $table").show(numRows = 100, truncate = false)
      spark.sql(s"show partitions $table").show(numRows = 100, truncate = false)
      //spark.sql(s"describe formatted $table partition('$part')").show(numRows = 100, truncate = false)
      Seq.empty
    }

    override def getTable(db: String, table: String): TableMetadata = {
      TableMetadata(StructType(Seq.empty[StructField]), Seq.empty)
    }
  }

}
