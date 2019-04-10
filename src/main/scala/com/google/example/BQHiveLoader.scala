package com.google.example

import com.google.cloud.bigquery.BigQueryOptions

object BQHiveLoader {
  case class Config(metastoreUri: String = "",
                    hiveDbName: String = "",
                    hiveTableName: String = "",
                    project: String = "",
                    dataset: String = "",
                    table: String = "")

  val Parser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("BQHiveLoader") {
      head("BQHiveLoader", "0.1")

      opt[String]('p', "project")
        .required()
        .action{(x, c) => c.copy(project = x)}
        .text("project is a string property")

      opt[String]('d', "hiveDbName")
        .required()
        .action{(x, c) => c.copy(hiveDbName = x)}
        .text("hiveDbName is a string property")

      opt[String]('t', "hiveTableName")
        .required()
        .action{(x, c) => c.copy(hiveTableName = x)}
        .text("hiveTableName is a string property")

      opt[String]("dataset")
        .required()
        .action{(x, c) => c.copy(dataset = x)}
        .text("dataset is a string property")

      opt[String]("table")
        .required()
        .action{(x, c) => c.copy(table = x)}
        .text("table is a string property")

      opt[String]('m',"metastoreUri")
        .required()
        .action{(x, c) => c.copy(metastoreUri = x)}
        .text("metastoreUri is a string property")

      note("Loads Hive external ORC tables into BigQuery")

      help("help")
        .text("prints this usage text")
    }


  def main(args: Array[String]): Unit = {
    Parser.parse(args, Config()) match {
      case Some(config) =>
        val metastore = MetastoreQuery.connectToMetaStore(config.metastoreUri)
        val bigquery = BigQueryOptions.getDefaultInstance.getService
        val metadata = MetastoreQuery.getMetadata(config.hiveDbName, config.hiveTableName, metastore)
        Mapping.createExternalTable(config.project, config.dataset, config.table, metadata, bigquery)

      case _ =>
        System.err.println("Invalid args")
        System.exit(1)
    }


  }

}
