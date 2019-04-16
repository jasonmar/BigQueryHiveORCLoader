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

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BQHiveLoader {
  val BigQueryScope = "https://www.googleapis.com/auth/bigquery"
  val StorageScope = "https://www.googleapis.com/auth/devstorage.read_write"

  case class Config(metastoreUri: Option[String] = None,
                    metastoreDb: Option[String] = None,
                    hiveDbName: String = "",
                    hiveTableName: String = "",
                    partCol: String = "",
                    targetPart: String = "",
                    project: String = "",
                    dataset: String = "",
                    table: String = "",
                    bigQueryLocation: String = "US",
                    krbKeyTab: Option[String] = None,
                    krbPrincipal: Option[String] = None,
                    krbServiceName: Option[String] = Option("bqhiveorcloader"),
                    partFilters: Map[String,String] = Map.empty,
                    kerberosEnabled: Boolean = false)

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
        .required()
        .action{(x, c) => c.copy(partCol = x)}
        .text("name of date partition column")

      opt[String]('t', "targetPartition")
        .required()
        .action{(x, c) => c.copy(targetPart = x)}
        .text("target date partition value")

      opt[String]('p', "project")
        .required()
        .action{(x, c) => c.copy(project = x)}
        .text("destination BigQuery project")

      opt[String]('b',"dataset")
        .required()
        .action{(x, c) => c.copy(dataset = x)}
        .text("destination BigQuery dataset")

      opt[String]('d',"table")
        .required()
        .action{(x, c) => c.copy(table = x)}
        .text("destination BigQuery table")

      opt[String]('m',"metastoreUri")
        .action{(x, c) => c.copy(metastoreUri = Option(x))}
        .text("Hive MetaStore thrift URI (thrift://localhost:9083)")

      opt[String]('j',"metastoreDb")
        .action{(x, c) => c.copy(metastoreDb = Option(x))}
        .text("Hive MetaStore DB connection string (jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true)")

      opt[Map[String,String]]("partFilters")
        .action{(x, c) => c.copy(partFilters = x)}
        .text("partition filters specified as k1=v1,k2=v2,...")

      opt[String]("bigQueryLocation")
        .action{(x, c) => c.copy(bigQueryLocation = x)}
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

      opt[Boolean]("kerberos")
        .action{(x,c) => c.copy(kerberosEnabled = x)}
        .text("flag to enable kerberos")

      note("Loads Hive external ORC tables into BigQuery")

      help("help")
        .text("prints this usage text")
    }

  def main(args: Array[String]): Unit = {
    Parser.parse(args, Config()) match {
      case Some(config) =>
        if (config.kerberosEnabled) {
          for {
            keytab <- config.krbKeyTab
            principal <- config.krbPrincipal
            serviceName <- config.krbServiceName
          } yield {
            Kerberos.configureJaas("BQHiveLoader", keytab, principal, serviceName)
          }
        }

        val sparkConf = new SparkConf()
        config.metastoreUri.foreach{x => sparkConf.set("hive.metastore.uris", x)}
        config.metastoreDb.foreach{x => sparkConf.set("javax.jdo.option.ConnectionURL", x)}

        val spark = SparkSession
          .builder()
          .master("local")
          .appName("BQHiveORCLoader")
          .config(sparkConf)
          .enableHiveSupport
          .getOrCreate()

        val bigquery = BigQueryOptions.getDefaultInstance.toBuilder
          .setLocation(config.bigQueryLocation)
          .setCredentials(GoogleCredentials.getApplicationDefault.createScoped(BigQueryScope, StorageScope))
          .build()
          .getService

        run(config, spark, bigquery)

      case _ =>
        System.err.println("Invalid args")
        System.exit(1)
    }
  }

  def run(config: Config, spark: SparkSession, bigquery: BigQuery): Unit = {
    val table = spark.sessionState.catalog.externalCatalog
      .getTable(config.hiveDbName, config.hiveTableName)

    val targetParts = ExternalTableManager.findParts(config.hiveDbName, config.hiveTableName, config.partCol, config.targetPart, config.partFilters, spark)

    ExternalTableManager.loadParts(config.project, config.dataset, config.table, table, targetParts, bigquery)
  }
}
