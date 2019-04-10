# BigQuery Hive Loader

This utility copies data from ORC files into BigQuery tables from a BigQuery Permanent External Table using Federated Formats.
It queries Hive to retrieve partition information and generates SQL to set partition values not stored in the ORC file.


## Method

1. Accept Hive table name, BigQuery destination tablespec and Hive Metastore connection information from arguments
2. Query Hive metastore for schema and partition locations
3. Create BigQuery external table definition
4. Register BigQuery Permanent External Table
5. Generate SQL to query BigQuery Permanent External Table
6. Run Query Job to select from BigQuery Permanent External Table into destination table
7. Un-Register BigQuery Permanent External Table
8. Repeat steps 3-7 as necessary


## Documentation

* https://cloud.google.com/bigquery/external-data-sources
* https://cloud.google.com/bigquery/external-data-cloud-storage
* https://cloud.google.com/bigquery/external-table-definition


### Temporary or Permanent?

Querying of BigQuery Federated Formats requires a Permanent External Table.
This utility will unregister the table after the query is complete and the data is copied to a BigQuery native table.



#### External Table Definition

```json
{
  "schema": {
    "fields": [
      {
        "name": "[FIELD]",
        "type": "[DATA_TYPE]"
      },
      {
        "name": "[FIELD]",
        "type": "[DATA_TYPE]"
      }
    ]
  },
  "sourceFormat": "ORC",
  "sourceUris": [
    "[BUCKET_URI]"
  ]
}
```

#### Hive Metastore API

* Table.getCols (List[SchemaFields]) `id`, `amount`
* Partition.getValues `2019-04-10`,`US`
* Partition.getParameters `totalSize` -> `1234`, `COLUMN_STATS_ACCURATE` -> `{\"BASIC_STATS\":\"true\"}`)
* Partition.getSd StorageDescriptor
* StorageDescriptor.getCols (List[SchemaFields]) `date`, `region`
* StorageDescriptor.getSerdeInfo SerDeInfo
* SerDeInfo.getName `org.apache.hadoop.hive.ql.io.orc.OrcSerde`, `org.apache.hadoop.hive.ql.io.orc.VectorizedOrcSerde
`
