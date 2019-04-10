package com.google.example

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, IMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition, Table}

import scala.collection.JavaConverters._

object MetastoreQuery {
  def connectToMetaStore(uri: String): IMetaStoreClient = {
    val conf = new HiveConf()
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, uri)
    new HiveMetaStoreClient(conf)
  }

  case class TableMetadata(table: Table,
                           fields: Seq[FieldSchema],
                           partitions: Seq[Partition])

  def getMetadata(dbName: String,
                  tableName: String,
                  meta: IMetaStoreClient): TableMetadata = {
    TableMetadata(
      table = meta.getTable(dbName, tableName),
      fields = meta.getFields(dbName, tableName).asScala,
      partitions = meta.listPartitions(dbName, tableName, 9999).asScala
    )
  }
}
