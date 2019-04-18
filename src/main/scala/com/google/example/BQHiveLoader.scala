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

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Paths}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.google.example.ExternalTableManager.{Partition, TableMetadata}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BQHiveLoader {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"

  case class Config(hiveDbName: String = "",
                    hiveTableName: String = "",
                    partCol: Option[String] = None,
                    bqProject: String = "",
                    bqDataset: String = "",
                    bqTable: String = "",
                    bqLocation: String = "US",
                    bqKeyFile: Option[String] = None,
                    gcsKeyFile: Option[String] = None,
                    krbKeyTab: Option[String] = None,
                    krbPrincipal: Option[String] = None,
                    krbServiceName: Option[String] = Option("bqhiveorcloader"),
                    partFilters: String = "",
                    useYarn: Boolean = false)

  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("BQHiveLoader") {
      head("BQHiveLoader", "0.1")

      opt[String]('h', "hiveDbName")
        .required()
        .action{(x, c) => c.copy(hiveDbName = x)}
        .text("source Hive database name")

      opt[String]('s', "hiveTableName")
        .required()
        .action{(x, c) => c.copy(hiveTableName = x)}
        .text("source Hive table name")

      opt[String]('c', "dateColumn")
        .action{(x, c) => c.copy(partCol = Option(x))}
        .text("name of date partition column")

      opt[String]("bqKeyFile")
        .action{(x, c) => c.copy(bqKeyFile = Option(x))}
        .text("path to keyfile for BigQuery")

      opt[String]("gcsKeyFile")
        .action{(x, c) => c.copy(gcsKeyFile = Option(x))}
        .text("path to keyfile for GCS")

      opt[String]('p', "project")
        .required()
        .action{(x, c) => c.copy(bqProject = x)}
        .text("destination BigQuery project")

      opt[String]('b',"dataset")
        .required()
        .action{(x, c) => c.copy(bqDataset = x)}
        .text("destination BigQuery dataset")

      opt[String]('d',"table")
        .required()
        .action{(x, c) => c.copy(bqTable = x)}
        .text("destination BigQuery table")

      opt[String]('w', "partFilters")
        .action{(x, c) => c.copy(partFilters = x)}
        .text("partition filters specified as date > 2019-04-18 AND region IN A,B,C AND part = *")

      opt[String]('l', "bqLocation")
        .action{(x, c) => c.copy(bqLocation = x)}
        .text("BigQuery Location (default: US)")

      opt[String]("keyTab")
        .action{(x, c) => c.copy(krbKeyTab = Option(x))}
        .text("Kerberos keytab location (path/to/krb5.keytab)")

      opt[String]("principal")
        .action{(x, c) => c.copy(krbPrincipal = Option(x))}
        .text("Kerberos user principal (user/host.example.com@EXAMPLE.COM)")

      opt[String]("serviceName")
        .action{(x, c) => c.copy(krbServiceName = Option(x))}
        .text("Kerberos service name")

      opt[Boolean]("yarn")
        .action{(x, c) => c.copy(useYarn = x)}
        .text("flag to enable yarn (default: false)")

      note("Loads Hive external ORC tables into BigQuery")

      help("help")
        .text("prints this usage text")
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

        val sparkConf = new SparkConf()

        val spark = SparkSession
          .builder()
          .master(if (config.useYarn) "yarn" else "local")
          .appName("BQHiveORCLoader")
          .config(sparkConf)
          .enableHiveSupport
          .getOrCreate()

        run(config, spark)

      case _ =>
        System.err.println("Invalid args")
        System.exit(1)
    }
  }

  def run(config: Config, spark: SparkSession): Unit = {
    val table1 = spark.sessionState.catalog.externalCatalog
      .getTable(config.hiveDbName, config.hiveTableName)
    val table = TableMetadata(table1.schema, table1.partitionColumnNames)

    val targetParts: Seq[Partition] =
      ExternalTableManager
        .findParts(config.hiveDbName, config.hiveTableName, config.partFilters, spark)
        .map{part =>
          Partition(part.spec.values.toArray.toSeq, part.location.toString)
        }

    spark.sparkContext.parallelize(Seq(config))
      .foreach{c =>
        val creds: GoogleCredentials = c.bqKeyFile match {
          case Some(f) =>
            GoogleCredentials.fromStream(new ByteArrayInputStream(Files.readAllBytes(Paths.get(f))))
          case _ =>
            GoogleCredentials.getApplicationDefault
        }
        val bigquery: BigQuery = BigQueryOptions.newBuilder()
          .setLocation(c.bqLocation)
          .setCredentials(creds.createScoped(BigQueryScope, StorageScope))
          .build()
          .getService
        ExternalTableManager.loadParts(c.bqProject, c.bqDataset, c.bqTable, table, targetParts, bigquery)
      }
  }
}
