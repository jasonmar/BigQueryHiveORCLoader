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

object Config {
  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("BQHiveLoader") {
      head("BQHiveLoader", "0.1")

      opt[Boolean]("partitioned")
        .action{(x, c) => c.copy(partitioned = x, partFilters = "*")}
        .text("flag indicating that table is not partitioned")

      opt[String]("partFilters")
        .action{(x, c) => c.copy(partFilters = x)}
        .text("Partition filter expression specified as date > 2019-04-18 AND region IN (A,B,C) AND part = *")

      opt[String]("partitionColumn")
        .action{(x, c) => c.copy(partitionColumn = Option(x.toLowerCase))}
        .text("Partition column name")

      opt[Seq[String]]("clusterCols")
        .action{(x, c) => c.copy(clusterColumns = x.map(_.toLowerCase))}
        .text("Cluster columns if creating BigQuery table")

      opt[Map[String,String]]("partColFormats")
        .action{(x, c) => c.copy(partColFormats = x.toSeq)}
        .text("Partition Column Formats (example: 'month,YYYYMM')")

      opt[String]("hiveJdbcUrl")
        .action{(x, c) => c.copy(hiveJdbcUrl = x)}
        .text("Hive JDBC URL")

      opt[String]("hiveDbName")
        .required()
        .action{(x, c) => c.copy(hiveDbName = x)}
        .text("Hive source database name")

      opt[String]("hiveTableName")
        .required()
        .action{(x, c) => c.copy(hiveTableName = x)}
        .text("Hive source table name")

      opt[String]("hiveMetastoreType")
        .action{(x, c) => c.copy(hiveMetastoreType = x)}
        .text("Hive Metastore type (default: jdbc)")

      opt[String]("hiveStorageFormat")
        .action{(x, c) => c.copy(hiveStorageFormat = Option(x))}
        .text("Hive storage format (default: orc)")
        .validate{s =>
          if (!Set("orc", "parquet", "avro").contains(s.toLowerCase))
            failure(s"unrecognized storage format '$s'")
          else success
        }

      opt[String]("bqProject")
        .required()
        .action{(x, c) => c.copy(bqProject = x)}
        .text("BigQuery destination project")

      opt[String]("bqDataset")
        .required()
        .action{(x, c) => c.copy(bqDataset = x)}
        .text("BigQuery destination dataset")

      opt[String]("bqTable")
        .required()
        .action{(x, c) => c.copy(bqTable = x)}
        .text("BigQuery destination table")

      opt[String]("bqKeyFile")
        .action{(x, c) => c.copy(bqKeyFile = Option(x))}
        .text("BigQuery keyfile path")

      opt[String]("bqCreateTableKeyFile")
        .action{(x, c) => c.copy(bqCreateTableKeyFile = Option(x))}
        .text("BigQuery keyfile path for external table creation")

      opt[String]("bqWriteKeyFile")
        .action{(x, c) => c.copy(bqWriteKeyFile = Option(x))}
        .text("BigQuery keyfile path for write to destination table")

      opt[String]("bqLocation")
        .action{(x, c) => c.copy(bqLocation = x)}
        .text("BigQuery Location (default: US)")

      opt[Boolean]( "bqOverwrite")
        .action{(x, c) => c.copy(bqOverwrite = x)}
        .text("BigQuery overwrite flag (default: false)")

      opt[Boolean]( "bqBatch")
        .action{(x, c) => c.copy(bqBatch = x)}
        .text("BigQuery batch mode flag (default: true)")

      opt[String]("gcsKeyFile")
        .action{(x, c) => c.copy(gcsKeyFile = Option(x))}
        .text("GCS keyfile path for object listing")

      opt[String]("krbKeyTab")
        .action{(x, c) => c.copy(krbKeyTab = Option(x))}
        .text("Kerberos keytab location (path/to/krb5.keytab)")

      opt[String]("krbPrincipal")
        .action{(x, c) => c.copy(krbPrincipal = Option(x))}
        .text("Kerberos user principal (<serviceName>/<hostname>@KerberosRealmName)")

      note("Loads Hive external ORC tables into BigQuery")

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
        else success
      }
    }
}

case class Config(partitioned: Boolean = true,
                  partFilters: String = "",
                  partitionColumn: Option[String] = None,
                  clusterColumns: Seq[String]= Seq.empty,
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
                  bqOverwrite: Boolean = false,
                  bqBatch: Boolean = true,
                  bqKeyFile: Option[String] = None,
                  bqCreateTableKeyFile: Option[String] = None,
                  bqWriteKeyFile: Option[String] = None,
                  gcsKeyFile: Option[String] = None,
                  krbKeyTab: Option[String] = None,
                  krbPrincipal: Option[String] = None
                 )
