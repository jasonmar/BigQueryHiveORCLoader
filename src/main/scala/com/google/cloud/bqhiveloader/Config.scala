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

object Config {
  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("BQHiveLoader") {
      head("BQHiveLoader", "0.1")

      note("BigQuery Hive Loader is a command-line utility for loading Hive partitions into BigQuery\n")

      opt[Boolean]("partitioned")
        .action{(x, c) => c.copy(partitioned = x, partFilters = "*")}
        .text("(optional) flag indicating that table is not partitioned (default: true)")

      opt[String]("partFilters")
        .action{(x, c) => c.copy(partFilters = x)}
        .text("(optional) Partition filter expression. Example: 'date > 2019-04-18 AND region IN (A,B,C) AND part = *'")

      opt[String]("partitionColumn")
        .action{(x, c) => c.copy(partitionColumn = Option(x.toLowerCase))}
        .text("(optional) Partition column name (default: None)")

      opt[String]("refreshPartition")
        .action{(x, c) => c.copy(refreshPartition = Option(x))}
        .text("BigQuery partition ID to refresh, formatted YYYYMMDD (default: None)")
        .validate{s =>
          if (s.matches("""^\d{8}$"""))
            success
          else
            failure(s"refreshPartition '$s' does not match format YYYYMMDD")
        }

      opt[String]("tempDataset")
        .action{(x, c) => c.copy(tempDataset = x)}
        .text("Temporary BigQuery Dataset name where Hive partitions will be stored prior to select into the refresh partition (required if refreshPartition is set)")

      opt[Seq[String]]("clusterCols")
        .action{(x, c) => c.copy(clusterColumns = x.map(_.toLowerCase))}
        .text("(optional) Cluster columns if creating BigQuery table (default: None)")

      opt[Seq[String]]("drop")
        .action{(x, c) => c.copy(dropColumns = x.map(_.toLowerCase).toSet)}
        .text("(optional) Comma-separated list of columns to be ignored (default: None)")

      opt[Seq[String]]("keep")
        .action{(x, c) => c.copy(keepColumns = x.map(_.toLowerCase).toSet)}
        .text("(optional) Comma-separated list of columns to be loaded; all others will be ignored (default: None)")

      opt[Map[String,String]]("partColFormats")
        .action{(x, c) => c.copy(partColFormats = x.toSeq)}
        .text("(optional) Partition Column format to be used to parse INTEGER or STRING type partition column as DATE. Provided as comma separated key/value pairs col=fmt. Example: 'date=%Y-%m-%d,month=YYYYMM' (default: None)")

      opt[String]("hiveJdbcUrl")
        .action{(x, c) => c.copy(hiveJdbcUrl = x)}
        .text("Hive JDBC connection string (required)")

      opt[String]("hiveDbName")
        .required()
        .action{(x, c) => c.copy(hiveDbName = x)}
        .text("Hive database name containing partitions to be loaded (required)")

      opt[String]("hiveTableName")
        .required()
        .action{(x, c) => c.copy(hiveTableName = x)}
        .text("Hive table name containing partitions to be loaded (required)")

      opt[String]("hiveMetastoreType")
        .action{(x, c) => c.copy(hiveMetastoreType = x)}
        .text("(optional) Hive Metastore type (default: jdbc)")

      opt[String]("hiveStorageFormat")
        .action{(x, c) => c.copy(hiveStorageFormat = Option(x))}
        .text("(optional) Hive storage format (default: orc)")
        .validate{s =>
          if (!Set("orc", "parquet", "avro").contains(s.toLowerCase))
            failure(s"unrecognized storage format '$s'")
          else success
        }

      opt[String]("bqProject")
        .required()
        .action{(x, c) => c.copy(bqProject = x)}
        .text("BigQuery destination project (required)")

      opt[String]("bqDataset")
        .required()
        .action{(x, c) => c.copy(bqDataset = x)}
        .text("BigQuery destination dataset (required)")

      opt[String]("bqTable")
        .required()
        .action{(x, c) => c.copy(bqTable = x)}
        .text("BigQuery destination table (required)")

      opt[String]("bqKeyFile")
        .action{(x, c) => c.copy(bqKeyFile = Option(x))}
        .text("(optional) BigQuery keyfile path for all BigQuery operations. Ignored if bqCreateTableKeyFile and bqWriteKeyFile are provided")

      opt[String]("bqCreateTableKeyFile")
        .action{(x, c) => c.copy(bqCreateTableKeyFile = Option(x))}
        .text("BigQuery keyfile path for external table creation. (required if bqKeyFile not set)")

      opt[String]("bqWriteKeyFile")
        .action{(x, c) => c.copy(bqWriteKeyFile = Option(x))}
        .text("BigQuery keyfile path for write to destination table. (required if bqKeyFile not set)")

      opt[String]("bqLocation")
        .action{(x, c) => c.copy(bqLocation = x)}
        .text("(optional) BigQuery Location (default: US)")

      opt[Boolean]( "bqOverwrite")
        .action{(x, c) => c.copy(bqOverwrite = x)}
        .text("(optional) BigQuery overwrite flag. Ignored if refreshPartition is set. WARNING: ALL data in the table will be deleted. (default: false)")

      opt[Boolean]( "bqBatch")
        .action{(x, c) => c.copy(bqBatch = x)}
        .text("(optional) BigQuery batch mode flag. Enable to allow BigQuery to manage Job concurrency level. Disable to cause jobs to be run immediately. (default: true)")

      opt[String]("gcsKeyFile")
        .action{(x, c) => c.copy(gcsKeyFile = Option(x))}
        .text("GCS keyfile path for object listing (required)")

      opt[String]("krbKeyTab")
        .action{(x, c) => c.copy(krbKeyTab = Option(x))}
        .text("(optional) Kerberos keytab location (path/to/krb5.keytab)")

      opt[String]("krbPrincipal")
        .action{(x, c) => c.copy(krbPrincipal = Option(x))}
        .text("(optional) Kerberos principal (user@realm or service/host@realm)")

      opt[Boolean]("dryRun")
        .action{(x, c) => c.copy(dryRun = x)}
        .text("(optional) When specified, requests are logged and not submitted to BigQuery (default: false)")

      help("help")
        .text("prints this usage text")

      checkConfig{c =>
        val bqKey = c.bqKeyFile.isDefined
        val bqCreateKey = c.bqCreateTableKeyFile.isDefined
        val bqWriteKey = c.bqWriteKeyFile.isDefined

        if (bqKey && bqCreateKey)
          failure("Can't set both bqKeyFile and bqCreateTableKeyFile")
        else if (bqKey && bqWriteKey)
          failure("Can't set both bqKeyFile and bqWriteKeyFile")
        else if (c.refreshPartition.isDefined && c.tempDataset.isEmpty)
          failure("must provide tempDataset if with refresh partition")
        else if (c.keepColumns.nonEmpty && c.dropColumns.nonEmpty)
          failure("only one of keep,drop may be provided")
        else if (c.keepColumns.nonEmpty && c.keepColumns.exists(!ExternalTableManager.validBigQueryColumnName(_)))
          failure("keepColumns contains invalid column names")
        else if (c.dropColumns.nonEmpty && c.dropColumns.exists(!ExternalTableManager.validBigQueryColumnName(_)))
          failure("dropColumns contains invalid column names")
        else success
      }
    }
}

case class Config(partitioned: Boolean = true,
                  dryRun: Boolean = false,
                  partFilters: String = "",
                  partitionColumn: Option[String] = None,
                  clusterColumns: Seq[String] = Seq.empty,
                  dropColumns: Set[String] = Set.empty,
                  keepColumns: Set[String] = Set.empty,
                  partColFormats: Seq[(String,String)] = Seq.empty,
                  unusedColumnName: String = "unused",
                  hiveDbName: String = "",
                  hiveTableName: String = "",
                  hiveMetastoreType: String = "jdbc",
                  hiveJdbcUrl: String = "",
                  hiveStorageFormat: Option[String] = None,
                  bqProject: String = "",
                  bqDataset: String = "",
                  bqTable: String = "",
                  bqLocation: String = "US",
                  refreshPartition: Option[String] = None,
                  tempDataset: String = "",
                  bqOverwrite: Boolean = false,
                  bqBatch: Boolean = true,
                  bqKeyFile: Option[String] = None,
                  bqCreateTableKeyFile: Option[String] = None,
                  bqWriteKeyFile: Option[String] = None,
                  gcsKeyFile: Option[String] = None,
                  krbKeyTab: Option[String] = None,
                  krbPrincipal: Option[String] = None
                 )
