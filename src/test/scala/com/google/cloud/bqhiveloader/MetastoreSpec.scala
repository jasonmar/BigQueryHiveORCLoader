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

package com.google.cloud.bqhiveloader

import com.google.cloud.imf.bqhiveloader.{ExternalTableManager, MetaStore}
import com.google.cloud.imf.bqhiveloader.ExternalTableManager.{Avro, Parquet}
import com.google.cloud.imf.bqhiveloader.MetaStore.TableMetadata
import org.scalatest.FlatSpec

class MetastoreSpec extends FlatSpec {
  "Metastore" should "parse" in {
    val test = Seq(
      ("col1","bigint","None"),
      ("col2","string","None"),
      ("col3","int","None"),
      ("col4","string","None"),
      ("col5","string","None"),
      ("","",""),
      ("# Detailed Table Information","",""),
      ("Database","db",""),
      ("Table","prod",""),
      ("Owner","root",""),
      ("Created","Mon Jul 01 01:00:00 PDT 2019",""),
      ("Last Access","Wed Dec 31 16:00:00 PST 1969",""),
      ("Type","EXTERNAL",""),
      ("Provider","hive",""),
      ("Table Properties","[numFiles=123, transient_lastDdlTime=1555555555, totalSize=123456789]",""),
      ("Location","gs://bucket/dir/subdir",""),
      ("Serde Library","org.apache.hadoop.hive.ql.io.orc.OrcSerde",""),
      ("InputFormat","org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",""),
      ("OutputFormat","org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",""),
      ("Storage Properties","[serialization.format=1]",""),
      ("Partition Provider","Catalog","")
    )
    val result: TableMetadata = MetaStore.parseTableDetails(test.map(x => (x._1, x._2)))
    assert(result.schema.fields.length == 5)
    assert(result.location.contains("gs://bucket/dir/subdir"))
    assert(result.format == ExternalTableManager.Orc)
  }

  it should "identify parquet" in {
    val example = Seq(
      ("col1","bigint","None"),
      ("col2","string","None"),
      ("col3","int","None"),
      ("col4","string","None"),
      ("col5","string","None"),
      ("","",""),
      ("# Detailed Table Information","",""),
      ("Database","db",""),
      ("Table","prod",""),
      ("Owner","root",""),
      ("Created","Mon Jul 01 01:00:00 PDT 2019",""),
      ("Last Access","Wed Dec 31 16:00:00 PST 1969",""),
      ("Type","EXTERNAL",""),
      ("Provider","hive",""),
      ("Table Properties","[numFiles=123, transient_lastDdlTime=1555555555, totalSize=123456789]",""),
      ("Location","gs://bucket/dir/subdir",""),
      ("SerDe Library", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", ""),
      ("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", ""),
      ("OutputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", ""),
      ("Storage Properties","[serialization.format=1]",""),
      ("Partition Provider","Catalog","")
    )
    val tbl: TableMetadata = MetaStore.parseTableDetails(example.map(x => (x._1,x._2)))
    assert(tbl.format == Parquet)
  }

  it should "identify avro" in {
    val example = Seq(
      ("col1","bigint","None"),
      ("col2","string","None"),
      ("col3","int","None"),
      ("col4","string","None"),
      ("col5","string","None"),
      ("","",""),
      ("# Detailed Table Information","",""),
      ("Database","db",""),
      ("Table","prod",""),
      ("Owner","root",""),
      ("Created","Mon Jul 01 01:00:00 PDT 2019",""),
      ("Last Access","Wed Dec 31 16:00:00 PST 1969",""),
      ("Type","EXTERNAL",""),
      ("Provider","hive",""),
      ("Table Properties","[numFiles=123, transient_lastDdlTime=1555555555, totalSize=123456789]",""),
      ("Location","gs://bucket/dir/subdir",""),
      ("SerDe Library", "org.apache.hadoop.hive.serde2.avro.AvroSerDe", ""),
      ("InputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", ""),
      ("OutputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat", ""),
      ("Storage Properties","[serialization.format=1]",""),
      ("Partition Provider","Catalog","")
    )
    val tbl: TableMetadata = MetaStore.parseTableDetails(example.map(x => (x._1,x._2)))
    assert(tbl.format == Avro)
  }
}
