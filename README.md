# SqlSoR by Taboola

SqlSoR is a java library for effective schema pruning when reading nested data structures. 
Back in 2018 we've identified that our spark jobs are reading more data than they need to ingest, not leveraging parquet columnar format that support reading partial schema of nested columns.

This issue is documented in notorious [SPARK-4502](https://issues.apache.org/jira/browse/SPARK-4502), which at that time did not seem close to resolution. As spark issue was finally (partially) resolved, we were already running with SqlSoR in production for several months, reducing up to 95% of input size for spark sql queries which involves highly nested data structures, with structs, arrays and maps interwoven together.

SqlSoR traverse the logical plan of the query, determines the schema that is required for running the query, and then we re-create the query using the generated schema. 
We use SqlSoR as best effort solution, so in case we fail to generate the schema for a certain query, or if the generate schema is incompatible, we fallback to the full schema of the datasource. 

# Example usage

    SparkContext sc = new SparkContext("local", "Example"); 
    sparkSession = new SparkSession(sc);
    Dataset<Row> dataset = sparkSession.read().parquet("/path/to/parquet");
    Dataset<Row> query = dataset.select("my.nested.field").filter("condition = true");
    query.createOrReplaceTempView("myView");
    
    SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
    StructType schema = generator.getSchemaOnReadByAlias("myView"); // or generator.getSchemaOnRead("/path/to/parquet")
    
    Dataset<Row> reducedSchemaDataset = sparkSession.read().schema(schema).parquet("/path/to/parquet");
    Dataset<Row> reducedSchemaQuery = dataset.select("my.nested.field").filter("condition = true");
    // perform action on reducedSchemaQuery
    
