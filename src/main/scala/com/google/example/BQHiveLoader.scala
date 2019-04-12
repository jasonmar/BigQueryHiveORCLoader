package com.google.example

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import org.apache.spark.sql.SparkSession

object BQHiveLoader {
  val LocalMetastoreUri = "thrift://localhost:9083"
  val LocalMetastoreConnection = "jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true"

  case class Config(metastoreUri: String = LocalMetastoreUri,
                    metastoreDb: String = LocalMetastoreConnection,
                    hiveDbName: String = "",
                    hiveTableName: String = "",
                    partCol: String = "",
                    targetPart: String = "",
                    project: String = "",
                    dataset: String = "",
                    table: String = "")

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
        .action{(x, c) => c.copy(metastoreUri = x)}
        .text("Hive MetaStore thrift URI")

      opt[String]('j',"metastoreDb")
        .action{(x, c) => c.copy(metastoreDb = x)}
        .text("Hive MetaStore DB connection string")

      note("Loads Hive external ORC tables into BigQuery")

      help("help")
        .text("prints this usage text")
    }

  def main(args: Array[String]): Unit = {
    Parser.parse(args, Config()) match {
      case Some(config) =>
        val spark = SparkSession
          .builder()
          .master("local")
          .appName("Test Hive Support")
          .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true")
          .enableHiveSupport
          .getOrCreate()

        val bigquery = BigQueryOptions.getDefaultInstance.toBuilder
          .setLocation("US")
          .build()
          .getService

        run(config, spark, bigquery)

      case _ =>
        System.err.println("Invalid args")
        System.exit(1)
    }
  }

  def run(config: Config, spark: SparkSession, bigquery: BigQuery): Unit = {
    val table = spark.sessionState.catalog.externalCatalog.getTable(config.hiveDbName, config.hiveTableName)

    val targetParts = ExternalTableManager.findParts(config.hiveDbName, config.hiveTableName, config.partCol, config.targetPart, spark)

    ExternalTableManager.loadParts(config.project, config.dataset, config.table, table, targetParts, bigquery)
  }
}
