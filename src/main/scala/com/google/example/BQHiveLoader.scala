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

import com.google.example.MetaStore.{ExternalCatalogMetaStore, JDBCMetaStore, Partition, SparkSQLMetaStore}
import org.apache.spark.sql.SparkSession

object BQHiveLoader {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"

  case class Config(partFilters: String = "",
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
                    krbPrincipal: Option[String] = None,
                    krbServiceName: Option[String] = Option("bqhiveorcloader")
  )

  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("BQHiveLoader") {
      head("BQHiveLoader", "0.1")

      opt[String]("partFilters")
        .action{(x, c) => c.copy(partFilters = x)}
        .text("partition filters specified as date > 2019-04-18 AND region IN A,B,C AND part = *")

      opt[String]('c', "partitionColumn")
        .action{(x, c) => c.copy(partitionColumn = Option(x))}
        .text("name of partition column")

      opt[Seq[String]]("clusterCols")
        .action{(x, c) => c.copy(clusterColumns = x)}
        .text("Cluster columns if creating BigQuery table")

      opt[Map[String,String]]("partColFormats")
        .action{(x, c) => c.copy(partColFormats = x.toSeq)}
        .text("Partition Column Formats (example: 'month,YYYYMM')")

      opt[String]("hiveJdbcUrl")
        .action{(x, c) => c.copy(hiveJdbcUrl = x)}
        .text("Hive JDBC URL")

      opt[String]('h', "hiveDbName")
        .required()
        .action{(x, c) => c.copy(hiveDbName = x)}
        .text("source Hive database name")

      opt[String]('s', "hiveTableName")
        .required()
        .action{(x, c) => c.copy(hiveTableName = x)}
        .text("source Hive table name")

      opt[String]("bqKeyFile")
        .action{(x, c) => c.copy(bqKeyFile = Option(x))}
        .text("path to keyfile for BigQuery")

      opt[String]("bqCreateTableKeyFile")
        .action{(x, c) => c.copy(bqCreateTableKeyFile = Option(x))}
        .text("path to keyfile for BigQuery external table creation")

      opt[String]("bqWriteKeyFile")
        .action{(x, c) => c.copy(bqWriteKeyFile = Option(x))}
        .text("path to keyfile for BigQuery writes")

      opt[String]("gcsKeyFile")
        .action{(x, c) => c.copy(gcsKeyFile = Option(x))}
        .text("path to keyfile for GCS")

      opt[String]("bqProject")
        .required()
        .action{(x, c) => c.copy(bqProject = x)}
        .text("destination BigQuery project")

      opt[String]("bqDataset")
        .required()
        .action{(x, c) => c.copy(bqDataset = x)}
        .text("destination BigQuery dataset")

      opt[String]("bqTable")
        .required()
        .action{(x, c) => c.copy(bqTable = x)}
        .text("destination BigQuery table")

      opt[String]("bqLocation")
        .action{(x, c) => c.copy(bqLocation = x)}
        .text("BigQuery Location (default: US)")

      opt[Boolean]( "bqOverwrite")
        .action{(x, c) => c.copy(bqOverwrite = x)}
        .text("BigQuery overwrite flag (default: false)")

      opt[Boolean]( "bqBatch")
        .action{(x, c) => c.copy(bqBatch = x)}
        .text("BigQuery batch mode flag (default: true)")

      opt[String]("hiveMetastoreType")
        .action{(x, c) => c.copy(hiveMetastoreType = x)}
        .text("Metastore type (default: jdbc)")

      opt[String]("hiveStorageFormat")
        .action{(x, c) => c.copy(hiveStorageFormat = Option(x))}
        .text("Storage Format (default: orc)")
        .validate{s =>
          if (!Set("orc", "parquet", "avro").contains(s.toLowerCase))
            failure(s"unrecognized storage format '$s'")
          else success
        }

      opt[String]("keyTab")
        .action{(x, c) => c.copy(krbKeyTab = Option(x))}
        .text("Kerberos keytab location (path/to/krb5.keytab)")

      opt[String]("principal")
        .action{(x, c) => c.copy(krbPrincipal = Option(x))}
        .text("Kerberos user principal (user/host.example.com@EXAMPLE.COM)")

      opt[String]("serviceName")
        .action{(x, c) => c.copy(krbServiceName = Option(x))}
        .text("Kerberos service name")

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

  def main(args: Array[String]): Unit = {
    Parser.parse(args, Config()) match {
      case Some(config) =>
        for {
          keytab <- config.krbKeyTab
          principal <- config.krbPrincipal
          serviceName <- config.krbServiceName
        } yield {
          Kerberos.configureJaas("BQHiveLoader", keytab, principal, serviceName)
        }

        val spark = SparkSession
          .builder()
          .appName("BQHiveORCLoader")
          .enableHiveSupport
          .getOrCreate()

        run(config, spark)

      case _ =>
        System.err.println("Invalid args")
        System.exit(1)
    }
  }

  def run(config: Config, spark: SparkSession): Unit = {
    val metaStore = {
      config.hiveMetastoreType match {
        case "jdbc" =>
          JDBCMetaStore(config.hiveJdbcUrl, spark)
        case "sql" =>
          SparkSQLMetaStore(spark)
        case "external" =>
          ExternalCatalogMetaStore(spark)
        case x =>
          throw new IllegalArgumentException(s"unsupported metastore type '$x'")
      }
    }
    val table = metaStore.getTable(config.hiveDbName, config.hiveTableName)
    val partitions: Seq[Partition] = metaStore.filterPartitions(config.hiveDbName, config.hiveTableName, config.partFilters)

    val sc = spark.sparkContext
    sc.runJob(rdd = sc.makeRDD(Seq(config),1),
      func = SparkJobs.loadPartitionsJob(table, partitions))
  }
}
