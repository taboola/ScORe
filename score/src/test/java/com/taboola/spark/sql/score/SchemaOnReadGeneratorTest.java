package com.taboola.spark.sql.score;

import static org.apache.spark.sql.functions.broadcast;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import scala.collection.Seq;


public class SchemaOnReadGeneratorTest  {

    public static final String PARQUET_PATH = "src/test/resources/sample-parquet/sample.parquet";
    public static final String SAMPLE_PARQUET = "sample_parquet";

    public static final String JSON_SAMPLE_PATH = "src/test/resources/sample.json";
    public static final String JSON_CRAZY_SAMPLE_PATH = "src/test/resources/crazy-sample.json";
    public static final String SAMPLE_JSON = "sample";
    public static final String CRAZY_SAMPLE_JSON = "crazy_sample";

    private static UDF2<String, Long, String> someUdf;

    static class LazyHolder {
        private static SparkSession sparkSession;
        private static Dataset<Row> parquetDF;
        private static Dataset<Row> jsonDF;
        private static Dataset<Row> json2DF;

        static {
            SparkConf conf = new SparkConf();
            conf.set("spark.default.parallelism", "1");
            conf.set("spark.sql.shuffle.partitions", "1");
            conf.set("spark.ui.enabled", "false");
            conf.set("spark.broadcast.compress", "false");
            conf.set("spark.shuffle.compress", "false");
            conf.set("spark.shuffle.spill.compress", "false");
            //conf.set("spark.executor.heartbeatInterval", "3600"); // for debug
            SparkContext sc = new SparkContext("local", "SchemaOnReadGeneratorTest", conf);
            sparkSession = new SparkSession(sc);

            jsonDF = sparkSession.read().option("multiline", true).json(JSON_SAMPLE_PATH);
            json2DF = sparkSession.read().option("multiline", true).json(JSON_CRAZY_SAMPLE_PATH);
            parquetDF = sparkSession.read().parquet(PARQUET_PATH);

            someUdf = (UDF2<String, Long, String>) (s, aLong) -> s;
            sparkSession.udf().register("someUdf", someUdf, DataTypes.StringType);
        }

        static void init() {
        }
    }

    @BeforeClass
    public static void beforeClass() throws URISyntaxException {
        LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
        ctx.setConfigLocation(SchemaOnReadGeneratorTest.class.getResource("/log4j2-unit-tests.xml").toURI());
        LazyHolder.init();
    }

    @Before
    public void beforeTest() {
        LazyHolder.jsonDF.createOrReplaceTempView(SAMPLE_JSON);
        LazyHolder.json2DF.createOrReplaceTempView(CRAZY_SAMPLE_JSON);
        LazyHolder.parquetDF.createOrReplaceTempView(SAMPLE_PARQUET);
    }

    @Test
    public void test_original() {
        assertSchemasEqual(
                LazyHolder.parquetDF.schema(),
                SchemaOnReadGenerator.generateSchemaOnRead(LazyHolder.parquetDF).getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_fullSchemaWithPredicate() {
        final Dataset<Row> query = LazyHolder.parquetDF.filter("someLong = 5");
        assertSchemasEqual(
                LazyHolder.parquetDF.schema(),
                SchemaOnReadGenerator.generateSchemaOnRead(query).getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_fullSchemaWithSubQuery() {
        LazyHolder.parquetDF.filter("someLong = 5").createOrReplaceTempView("filtered");
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someStr from filtered");
        assertSchemasEqual(
                DataTypes.createStructType(
                        Lists.newArrayList(DataTypes.createStructField("someLong", DataTypes.LongType, true),
                                DataTypes.createStructField("someStr", DataTypes.StringType, true))
                ),
        SchemaOnReadGenerator.generateSchemaOnRead(query).getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_fullSchemaWithSubQueryNoAlias() {
        final Dataset<Row> query = LazyHolder.parquetDF.select("someLong", "someStr").filter("someLong = 5").select("someStr");
        assertSchemasEqual(
                DataTypes.createStructType(
                        Lists.newArrayList(DataTypes.createStructField("someLong", DataTypes.LongType, true),
                                DataTypes.createStructField("someStr", DataTypes.StringType, true))
                ),
                SchemaOnReadGenerator.generateSchemaOnRead(query).getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_getDuration() {
        Supplier<Long> clockSupplier = new Supplier<Long>() {
            long initialValue = 0L;

            @Override
            public Long get() {
                long currTime = initialValue;
                initialValue += 10L;
                return currTime;
            }
        };

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(LazyHolder.parquetDF, clockSupplier);
        assertEquals(10L, generator.getDuration());
    }

    @Test
    public void test_simpleQueryApi() {
        final Dataset<Row> query = LazyHolder.parquetDF.select("someLong", "nestedStruct.str", "struct.col2");
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col2", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_simpleQuerySql() {
        final String queryStr = "SELECT someLong, nestedStruct.str, struct.col2 FROM sample_parquet";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))                                  
                                ), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col2", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_simpleQuerySqlCaseInsensitive() {
        final String queryStr = "SELECT SomeStr, Struct.Col2, nestedstruct.sTr FROM sample_parquet";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col2", DataTypes.LongType, true))
                                ), true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_withNestedArrays() {
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("crazyStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("repeatedStuff",
                                                        DataTypes.createArrayType(
                                                                DataTypes.createStructType(
                                                                        Lists.newArrayList(
                                                                                DataTypes.createStructField("anotherRepeatedStuff",
                                                                                        DataTypes.createArrayType(DataTypes.createStructType(
                                                                                                Lists.newArrayList(DataTypes.createStructField("innerField2", DataTypes.StringType, true))
                                                                                        )), true))
                                                                )), true))
                                ), true)

                )
        );

        final String[] inputPath = {JSON_CRAZY_SAMPLE_PATH};

        String queryStr = "SELECT crazyStruct.repeatedStuff[0].anotherRepeatedStuff[0].innerField2 FROM crazy_sample";
        Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));

        queryStr = "SELECT crazyStruct.repeatedStuff.anotherRepeatedStuff[0].innerField2 FROM crazy_sample";
        query = LazyHolder.sparkSession.sql(queryStr);
        generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));

        queryStr = "SELECT crazyStruct.repeatedStuff[0].anotherRepeatedStuff.innerField2 FROM crazy_sample";
        query = LazyHolder.sparkSession.sql(queryStr);
        generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));
    }

    @Test
    public void test_nestedStruct() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT nestedStruct.childStruct.col2 FROM sample");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                                        )), true)
                        )), true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_lateralViewExplode() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someStr, arrayVal FROM sample LATERAL VIEW EXPLODE(someStrArray) as arrayVal");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someStrArray",
                        DataTypes.createArrayType(DataTypes.StringType), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_lateralViewPosExplode() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someStr, arrayIdx, arrayVal FROM sample LATERAL VIEW POSEXPLODE(someStrArray) as arrayIdx, arrayVal");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someStrArray",
                        DataTypes.createArrayType(DataTypes.StringType), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_lateralViewExplodeComplex() {


        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someStr, arrayVal.col1 FROM sample LATERAL VIEW EXPLODE(someComplexArray) as arrayVal");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(DataTypes.createStructField("col1", DataTypes.LongType, true)))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_lateralViewExplodeComplexOverAggregation() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("WITH base AS " +
                "(SELECT someStr, FIRST(someComplexArray) as complexArray FROM sample GROUP BY someStr) " +
                "SELECT complex.col1 FROM base LATERAL VIEW EXPLODE(complexArray) as complex");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true)))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_structAggregationOverWindowSubQuery() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("WITH base AS " +
                "(SELECT someStr, FIRST(someComplexArray) OVER w as complexArray FROM sample " +
                "WINDOW w AS (PARTITION BY someStr ORDER BY someStr Desc)) " +
                "SELECT FIRST(complexArray) as myArray FROM base GROUP BY someStr");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                                        DataTypes.createStructField("col2", DataTypes.LongType, true)
                                ))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_windowWithComplexPartition() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("WITH base AS " +
                "(SELECT FIRST(someStr) OVER w as str FROM sample " +
                "WINDOW w AS (PARTITION BY someComplexArray ORDER BY someStr Desc)) " +
                "SELECT str FROM base");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                                        DataTypes.createStructField("col2", DataTypes.LongType, true)
                                ))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_windowWithComplexOrder() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("WITH base AS " +
                "(SELECT FIRST(someStr) OVER w as str FROM sample " +
                "WINDOW w AS (PARTITION BY someStr ORDER BY someComplexArray Desc)) " +
                "SELECT str FROM base");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                                        DataTypes.createStructField("col2", DataTypes.LongType, true)
                                ))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_orderOnComplex() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("WITH base AS " +
                "(SELECT someStr FROM sample ORDER BY someComplexArray) " +
                "SELECT someStr FROM base");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                                        DataTypes.createStructField("col2", DataTypes.LongType, true)
                                ))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_aggregateWithOrderOnComplex() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("WITH base AS " +
                "(SELECT someStr, FIRST(someComplexArray) as cmplx FROM sample " +
                "GROUP BY 1 ORDER BY cmplx) " +
                "SELECT someStr FROM base");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                                        DataTypes.createStructField("col2", DataTypes.LongType, true)
                                ))
                        ), true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }


    @Test
    public void test_consecutiveExplode() {
        final Dataset<Row> query =
                LazyHolder.sparkSession.sql("SELECT someStr, val " +
                        "FROM sample " +
                        "lateral view explode (someArrayOfComplexArrays) as complex " +
                        "lateral view explode (complex.col2) as val ");

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someStr", DataTypes.StringType, true),

                            DataTypes.createStructField("someArrayOfComplexArrays",
                                    DataTypes.createArrayType(
                                            DataTypes.createStructType(
                                                    Lists.newArrayList(
                                                            DataTypes.createStructField("col2", DataTypes.createArrayType(DataTypes.LongType), true))
                                    ), true), true)
                ));

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));

    }

    @Test
    // This test represents the use-case that motivated adding RelationsPreProcessor
    public void test_consecutiveExplodeWithLeftJoin() {
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("table_one");
        dataset.registerTempTable("table_two");
        String queryStr = "WITH base AS (SELECT someStr, val " +
                "FROM table_one " +
                "lateral view explode (someArrayOfComplexArrays) as complex " +
                "lateral view explode (complex.col2) as val) " +
                "SELECT base.someStr, base.val, complex.col1 " +
                "FROM table_two RIGHT JOIN base ON base.someStr = table_two.someStr " +
                "lateral view explode (someArrayOfComplexArrays) as complex " +
                "lateral view explode (complex.col2) as val ";


        final Dataset<Row> query =
                LazyHolder.sparkSession.sql(queryStr);

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someStr", DataTypes.StringType, true),

                        DataTypes.createStructField("someArrayOfComplexArrays",
                                DataTypes.createArrayType(
                                        DataTypes.createStructType(
                                                Lists.newArrayList(
                                                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                        DataTypes.createStructField("col2", DataTypes.createArrayType(DataTypes.LongType), true))
                                        ), true), true)
                ));

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_arrayWithStruct() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someComplexArray[0].col1 FROM sample");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("someComplexArray",
                        DataTypes.createArrayType(
                                DataTypes.createStructType(Lists.newArrayList(
                                        DataTypes.createStructField("col1", DataTypes.LongType, true)
                                ))),
                        true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_nestedArrays() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someArrayOfArrays[0][2] FROM sample");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someArrayOfArrays",
                                DataTypes.createArrayType(
                                        DataTypes.createArrayType(
                                                DataTypes.LongType
                                        )),
                                true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_UDF() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someUdf(someStr, someLong) FROM sample");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                    DataTypes.createStructField("someStr", DataTypes.StringType, true),
                    DataTypes.createStructField("someLong", DataTypes.LongType, true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_unionWithAlias() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someStr FROM sample")
                .union(LazyHolder.sparkSession.sql("SELECT nestedStruct.str FROM crazy_sample"));

        StructType expectedSchema1 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        StructType expectedSchema2 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema1, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
        assertSchemasEqual(expectedSchema2, generator.getSchemaOnRead(JSON_CRAZY_SAMPLE_PATH));
    }

    @Test
    public void test_unionWithoutAlias() {
        final Dataset<Row> query = LazyHolder.jsonDF.select("someStr")
                .union(LazyHolder.json2DF.select("nestedStruct.str"));

        StructType expectedSchema1 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                ));

        StructType expectedSchema2 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema1, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
        assertSchemasEqual(expectedSchema2, generator.getSchemaOnRead(JSON_CRAZY_SAMPLE_PATH));
    }


    @Test
    public void test_explodeSubArray() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT arrayVal FROM sample LATERAL VIEW EXPLODE(struct.subArray) as arrayVal");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("subArray",
                                                        DataTypes.createArrayType(
                                                                DataTypes.LongType
                                                        ),
                                                        true)
                                        )
                                )
                                , true
                        )));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }


    @Test
    public void test_simpleQuerySqlWithAliasReference() {
        final String queryStr = "SELECT sample_parquet.someLong, sample_parquet.struct.col1, sample_parquet.nestedStruct.str FROM sample_parquet";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_simpleQuerySqlWithWhereClauseNotInSelect() {
        final String queryStr = "SELECT sample_parquet.someLong, sample_parquet.nestedStruct.str, sample_parquet.struct.col1 " +
                "FROM sample_parquet " +
                "WHERE struct.col3 = 10";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col3", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_innerQuerySQL() {
        final String queryStr = "WITH base as (SELECT sample_parquet.someLong as myLong,  sample_parquet.struct FROM sample_parquet) " +
                "SELECT myLong FROM base";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),

                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Collections.emptyList()
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_groupBySql() {
        final String queryStr = "SELECT sample_parquet.someLong, sample_parquet.nestedStruct.str, sum(sample_parquet.struct.col1) as total_col1 " +
                "FROM sample_parquet " +
                "GROUP BY 1, sample_parquet.nestedStruct.str";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupBySql_with_count_1_with_order() {
        final String queryStr = "SELECT  nestedStruct.str, count(1) " +
                "FROM sample_parquet " +
                "group by 1 order by 2";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupBySql_with_count_wildcard_with_order() {
        final String queryStr = "SELECT struct.col2, count(*) " +
                "FROM sample_parquet " +
                "group by 1 order by 2 desc ";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col2", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupBySql_with_count_field_with_order() {
        final String queryStr = "SELECT  struct.col1, count(struct.col2) " +
                "FROM sample_parquet " +
                "group by 1 order by 2 desc ";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupByNotInQuerySql() {
        final String queryStr = "SELECT sample_parquet.someLong, sum(sample_parquet.struct.col1) as total_col1 " +
                "FROM sample_parquet " +
                "GROUP BY sample_parquet.someLong, sample_parquet.nestedStruct.str";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_groupBySqlInnerQuery() {
        final String queryStr = "WITH base as (SELECT sample_parquet.someLong, sample_parquet.nestedStruct.str, sum(sample_parquet.struct.col1) as total_col " +
                "FROM sample_parquet " +
                "GROUP BY 1, sample_parquet.nestedStruct.str) " +
                "SELECT someLong, total_col FROM base";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_conditionalAggregateSql() {
        final String queryStr = "WITH base as (SELECT crazy_sample.someLong, crazy_sample.nestedStruct.str, sum(if(struct.condition, crazy_sample.struct.col1, 0)) as total_col1 " +
                "FROM crazy_sample " +
                "GROUP BY 1, crazy_sample.nestedStruct.str) " +
                "SELECT someLong, total_col1 FROM base";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("condition", DataTypes.BooleanType, true))

                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(CRAZY_SAMPLE_JSON));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_CRAZY_SAMPLE_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test()
    public void test_selfJoin() {
        final String queryStr = "SELECT a.someLong, a.someStr, b.struct.col1 " +
                "FROM sample_parquet a JOIN sample_parquet b " +
                "ON a.someLong = b.someLong";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_join() {
        LazyHolder.sparkSession.read().parquet(PARQUET_PATH).createOrReplaceTempView("sample_parquet2");
        final String queryStr = "SELECT a.someLong, a.someStr, b.struct.col2 " +
                "FROM sample_parquet a JOIN sample_parquet2 b " +
                "ON a.someLong = b.someLong";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col2", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias("sample_parquet2"));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_joinWhereInsteadOfOn() {
        LazyHolder.sparkSession.read().parquet(PARQUET_PATH).createOrReplaceTempView("sample_parquet2");
        final String queryStr = "SELECT a.someLong, a.someStr, b.struct.col1 " +
                "FROM sample_parquet a JOIN sample_parquet2 b " +
                "WHERE a.someLong = b.someLong";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias("sample_parquet2"));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_filterPreAliasWithImplicitJoin() {
        final Dataset<Row> base = LazyHolder.parquetDF.filter("someBoolean = true");
        base.createOrReplaceTempView("base");
        List<MyPojo> stringsList = Lists.newArrayListWithCapacity(0);
        final Dataset<Row> stringsDF_one = LazyHolder.sparkSession.createDataFrame(stringsList, MyPojo.class);
        stringsDF_one.registerTempTable("strings_one");


        final String queryStr = "SELECT a.someLong " +
                "FROM base a, strings_one b " +
                "WHERE a.someStr = b.someString";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("someBoolean", DataTypes.BooleanType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
    }


    private static class MyPojo {
        private String someString;

        public String getSomeString() {
            return someString;
        }

        public void setSomeString(String someString) {
            this.someString = someString;
        }
    }


    @Test
    public void test_joinApi() {
        final Dataset<Row> df1 = LazyHolder.parquetDF;
        final Dataset<Row> df2 = LazyHolder.parquetDF;

        final Dataset<Row> query = df1.select(df1.col("someLong"), df1.col("someStr"))
                .join(df2.select(df2.col("someLong"), df2.col("struct.col3")), "someLong")
                .select("someLong", "someStr", "col3");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col3", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
    }

    @Test
    public void test_broadcastJoin() {
        LazyHolder.sparkSession.read().parquet(PARQUET_PATH).createOrReplaceTempView("sample_parquet2");
        final String queryStr = "SELECT /*+ BROADCAST(b) */ a.someLong, a.someStr, b.struct.col1 " +
                "FROM sample_parquet a JOIN sample_parquet2 b " +
                "ON a.someLong = b.someLong";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        query.explain();
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias("sample_parquet2"));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_broadcastJoinApi() {
        final Dataset<Row> df1 = LazyHolder.parquetDF;
        final Dataset<Row> df2 = LazyHolder.parquetDF;

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ), true)
                )
        );

        Dataset<Row> query = broadcast(df1).select(df1.col("someLong"), df1.col("someStr"))
                .join(df2.select(df2.col("someLong"), df2.col("struct.col1")), "someLong")
                .select("someLong", "someStr", "col1");

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));


        query = df1.select(df1.col("someLong"), df1.col("someStr"))
                .join(broadcast(df2).select(df2.col("someLong"), df2.col("struct.col1")), "someLong")
                .select("someLong", "someStr", "col1");

        generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
    }


    @Test
    public void test_simpleWindowFunction() {
        final String queryStr = "SELECT someLong, " +
                "FIRST(someStr) OVER (PARTITION BY someDouble ORDER BY someBoolean) " +
                "FROM sample_parquet";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someDouble", DataTypes.DoubleType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("someBoolean", DataTypes.BooleanType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_veryComplexWindowThatLacksWindowColumnsFromProject() {
        final String queryStr =
                    "SELECT someLong, struct.col1 as col1, \n" +
                    "FIRST_VALUE(struct.col3) OVER w as firstCol3, \n" +
                    "LAST_VALUE(struct.col2) OVER w as lastCol2 \n" +
                    "FROM crazy_sample \n" +
                    "WHERE someBoolean = true \n" +
                    "AND struct.condition = true \n" +
                    "WINDOW w AS (PARTITION BY someStr, someDouble \n" +
                    "ORDER BY struct.col1 ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someBoolean", DataTypes.BooleanType, true),
                        DataTypes.createStructField("someDouble", DataTypes.DoubleType, true),
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true),
                                                DataTypes.createStructField("col3", DataTypes.LongType, true),
                                                DataTypes.createStructField("condition", DataTypes.BooleanType, true)
                                        )
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_CRAZY_SAMPLE_PATH));
    }


    @Test
    public void test_cache() {
        final String queryStr = "SELECT someLong, someStr,  nestedStruct.str as str " +
                "FROM sample_parquet";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr).cache();

        final Dataset<Row> queryOnCache = query.select("someLong", "str");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(queryOnCache);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(SAMPLE_PARQUET));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
        query.unpersist();
    }


    @Test
    public void test_readMultipleFilesWildcards() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().option("multiline",true).json("src/test/resources/sample*.json").select("someLong");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH,
            "src/test/resources/sample2.json"));
    }


    @Test
    public void test_readMultipleFilesUnordered() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/sample-partitioned.parquet/someLong=12345678987654321/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet",
            "src/test/resources/sample-partitioned.parquet/someLong=654646321654987/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet").select("someBoolean");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someBoolean", DataTypes.BooleanType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(
                "src/test/resources/sample-partitioned.parquet/someLong=654646321654987/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet",
            "src/test/resources/sample-partitioned.parquet/someLong=12345678987654321/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet"));

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(
"src/test/resources/sample-partitioned.parquet/someLong=12345678987654321/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet",
           "src/test/resources/sample-partitioned.parquet/someLong=654646321654987/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet"));
    }

    @Test
    public void test_readMultipleFilesDuplicate() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet(PARQUET_PATH,
            PARQUET_PATH).select("someLong");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PARQUET_PATH, PARQUET_PATH));
    }

    @Test
    public void test_readMultipleFilesJoin() {

        final String queryStr = "SELECT a.someLong, a.someStr, b.struct.col3 " +
                "FROM sample_parquet a JOIN sample b " +
                "ON a.someLong = b.someLong";


        StructType schema1 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someStr", DataTypes.StringType, true)
                )
        );

        StructType schema2 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col3", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(LazyHolder.sparkSession.sql(queryStr));
        assertSchemasEqual(schema1, generator.getSchemaOnRead(PARQUET_PATH));
        assertSchemasEqual(schema2, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_variousExpressions() {
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT COALESCE(someStr, nestedStruct.str) FROM sample " +
                "WHERE someLong IS NULL " +
                "OR IF(someDouble < 0, true, false)");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someStr", DataTypes.StringType, true),
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someDouble", DataTypes.DoubleType, true),
                        DataTypes.createStructField("nestedStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(DataTypes.createStructField("str", DataTypes.StringType, true))
                                ), true)
                ));

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_mapWithComplexValue() {
        final StructType crazySchemaWithMap =
            getCrazySchemaWithMap();

        final Dataset<Row> dataset = LazyHolder.sparkSession.read().schema(crazySchemaWithMap).option("multiline", true)
            .json(JSON_CRAZY_SAMPLE_PATH);

        dataset.createOrReplaceTempView("tableWithMapOfArrays");

        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT mapOfArray['someKey'][0].val1," +
                "mapOfArray['someKey'][1].val2 " +
                "FROM tableWithMapOfArrays " +
                "WHERE mapOfArray['someKey'][2].val3 < 200");

        final StructType expectedSchema =
            DataTypes.createStructType(
                Lists.newArrayList(
                    DataTypes.createStructField("mapOfArray", DataTypes.createMapType(
                        DataTypes.StringType,
                        DataTypes.createArrayType(
                            DataTypes.createStructType(
                                Lists.newArrayList(
                                    DataTypes.createStructField("val1", DataTypes.StringType, true),
                                    DataTypes.createStructField("val2", DataTypes.StringType, true),
                                    DataTypes.createStructField("val3", DataTypes.LongType, true)
                                    )
                            )
                        )), true)
                )
            );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_CRAZY_SAMPLE_PATH));
    }

    private StructType getCrazySchemaWithMap() {
        return DataTypes.createStructType(
            Lists.newArrayList(
                DataTypes.createStructField("someStr", DataTypes.StringType, true),
                DataTypes.createStructField("someLong", DataTypes.LongType, true),
                DataTypes.createStructField("someDouble", DataTypes.DoubleType, true),
                DataTypes.createStructField("someBoolean", DataTypes.BooleanType, true),
                DataTypes.createStructField("someStrArray", DataTypes.createArrayType(DataTypes.StringType), true),
                DataTypes.createStructField("someComplexArray", DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)), true),
                DataTypes.createStructField("struct", DataTypes.createStructType(
                    Lists.newArrayList(
                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                        DataTypes.createStructField("col2", DataTypes.LongType, true),
                        DataTypes.createStructField("col3", DataTypes.LongType, true),
                        DataTypes.createStructField("condition", DataTypes.BooleanType, true),
                        DataTypes.createStructField("subArray", DataTypes.createArrayType(DataTypes.LongType), true)
                    )
                ), true),
                DataTypes.createStructField("nestedStruct", DataTypes.createStructType(
                    Lists.newArrayList(
                        DataTypes.createStructField("childStruct", DataTypes.createStructType(
                            Lists.newArrayList(
                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                            )
                        ), true),
                        DataTypes.createStructField("str", DataTypes.StringType, true)
                    )
                ), true),
                DataTypes.createStructField("someArrayOfArrays", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)), true),
                DataTypes.createStructField("someArrayOfComplexArrays", DataTypes.createArrayType(DataTypes.createStructType(
                    Lists.newArrayList(
                        DataTypes.createStructField("col1", DataTypes.LongType, true),
                        DataTypes.createStructField("col2", DataTypes.createArrayType(DataTypes.LongType), true)
                    )
                )), true),
                DataTypes.createStructField("mapOfArray", DataTypes.createMapType(
                    DataTypes.StringType,
                    DataTypes.createArrayType(
                        DataTypes.createStructType(
                            Lists.newArrayList(
                                DataTypes.createStructField("val1", DataTypes.StringType, true),
                                DataTypes.createStructField("val2", DataTypes.StringType, true),
                                DataTypes.createStructField("val3", DataTypes.LongType, true),
                                DataTypes.createStructField("val4", DataTypes.StringType, true)
                            )
                        )
                    )), true)
            )
        );
    }

    @Test
    public void test_parquetFolder() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/sample-parquet").select("someLong");
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead("src/test/resources/sample-parquet/sample.parquet"));
    }

    @Test
    public void test_explodeAndFilterSubQueryWithNestedArrayOfExplodedItem() {
        final String query = "WITH base AS (SELECT someArrayOfComplexArrays FROM sample " +
                "WHERE someArrayOfComplexArrays.col2 IS NOT NULL) " +
                "SELECT " +
                        "item.col1 AS str " +
                        "FROM base " +
                        "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as item ";

        final Dataset<Row> dataset = LazyHolder.sparkSession.sql(query);

        final StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someArrayOfComplexArrays",
                                DataTypes.createArrayType(DataTypes.createStructType(
                                            Lists.newArrayList(
                                                    DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                    DataTypes.createStructField("col2", DataTypes.createArrayType(DataTypes.LongType), true)
                                            )
                                    )), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(dataset);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_explodeAndFilterOverNestedArrayOfExplodedItem() {
        final String query =
                "SELECT " +
                "item.col1 AS rst " +
                "FROM sample " +
                "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as item " +
                "WHERE someArrayOfComplexArrays.col2 IS NOT NULL";

        final Dataset<Row> dataset = LazyHolder.sparkSession.sql(query);

        final StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someArrayOfComplexArrays",
                                DataTypes.createArrayType(DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.createArrayType(DataTypes.LongType), true)
                                        )
                                )), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(dataset);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_explodeDoubleExplodeNoFilter() {
        final String query = "SELECT stuff.justABool AS bool, " +
                        "stuff.longArray as arr " +
                        "FROM crazy_sample " +
                        "LATERAL VIEW EXPLODE(crazyStruct.repeatedStuff) as stuff ";

        final Dataset<Row> dataset = LazyHolder.sparkSession.sql(query);

        final StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("crazyStruct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("repeatedStuff",
                                                        DataTypes.createArrayType(
                                                                DataTypes.createStructType(
                                                                        Lists.newArrayList(
                                                                                DataTypes.createStructField("justABool", DataTypes.BooleanType, true),
                                                                                DataTypes.createStructField("longArray", DataTypes.createArrayType(DataTypes.LongType), true)
                                                                        )
                                                                )
                                                        ), true)
                                        )
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(dataset);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_CRAZY_SAMPLE_PATH));
    }

    @Test
    public void test_parquetFolderPartitioned() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/sample-partitioned.parquet").select("someLong", "someDouble");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someDouble", DataTypes.DoubleType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(
            "src/test/resources/sample-partitioned.parquet/someLong=12345678987654321/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet",
                       "src/test/resources/sample-partitioned.parquet/someLong=654646321654987/part-00000-7431a772-59d3-4edf-8a26-61c7936a0b17.c000.snappy.parquet"));
    }

    @Test
    public void test_dropColumn() {
        final Dataset<Row> df = LazyHolder.jsonDF.drop("someStr", "someArrayOfComplexArrays").select("someLong");
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someLong", DataTypes.LongType, true),
                        DataTypes.createStructField("someDouble", DataTypes.DoubleType, true),
                        DataTypes.createStructField("someBoolean", DataTypes.BooleanType, true),
                        DataTypes.createStructField("someStrArray",
                                DataTypes.createArrayType(DataTypes.StringType), true),
                        DataTypes.createStructField("someComplexArray",
                                DataTypes.createArrayType(DataTypes.createStructType(Collections.emptyList())), true),
                        DataTypes.createStructField("struct", DataTypes.createStructType(Collections.emptyList()), true),
                        DataTypes.createStructField("nestedStruct", DataTypes.createStructType(Collections.emptyList()), true),
                        DataTypes.createStructField("someArrayOfArrays",
                                DataTypes.createArrayType(
                                        DataTypes.createArrayType(
                                                DataTypes.LongType
                                        )
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_narrowSchemaThroughSubquery() {
        final Dataset<Row> df = LazyHolder.sparkSession.sql("WITH base AS( SELECT struct as myStruct FROM " + SAMPLE_JSON +
                " ) SELECT myStruct.col1 FROM base");
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                    Lists.newArrayList(DataTypes.createStructField("col1", DataTypes.LongType, true))
                                ),true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_complexTypeArray() {
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT someArrayOfComplexArrays FROM " + SAMPLE_JSON);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("someArrayOfComplexArrays",
                            DataTypes.createArrayType(
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.createArrayType(DataTypes.LongType), true),
                                                DataTypes.createStructField("col3", DataTypes.LongType, true))
                                ), true), true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_structFullAndSubStruct() {
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT nestedStruct, nestedStruct.str FROM " + SAMPLE_JSON);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                                        )), true),
                                DataTypes.createStructField("str", DataTypes.StringType, true)
                        )), true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }


    @Test
    public void test_groupStructPartByFullStruct() {
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct.str) FROM " + SAMPLE_JSON + " GROUP BY nestedStruct");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                                        )), true),
                                DataTypes.createStructField("str", DataTypes.StringType, true)
                        )), true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_groupFullStructByStructPart() {
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct) FROM " + SAMPLE_JSON + " GROUP BY nestedStruct.str");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                                        )), true),
                                DataTypes.createStructField("str", DataTypes.StringType, true)
                        )), true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_groupStructPartByFullStruct_subQuery() {
        Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct.str) as str FROM " + SAMPLE_JSON + " GROUP BY nestedStruct");
        df.createOrReplaceTempView("base");

        df = LazyHolder.sparkSession.sql("select str from base");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                                        )), true),
                                DataTypes.createStructField("str", DataTypes.StringType, true)
                        )), true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }

    @Test
    public void test_groupFullStructByStructPart_subQuery() {
        Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct) as mystruct FROM " + SAMPLE_JSON + " GROUP BY nestedStruct.str");

        df.createOrReplaceTempView("base");

        df = LazyHolder.sparkSession.sql("select mystruct.childStruct.col1 from base");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true)
                                        )), true),
                                DataTypes.createStructField("str", DataTypes.StringType, true)
                        )), true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }


    @Test
    public void test_aliasNameLikeExistingColumn() {
        // NOTE: This is a case that is not properly handled by schema on read, we don't really care if `struct` is used as alias for other column,
        // we just include `struct` column in schema
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT nestedStruct as struct FROM " + SAMPLE_JSON);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(DataTypes.createStructField("nestedStruct",
                        DataTypes.createStructType(Lists.newArrayList(
                                DataTypes.createStructField("childStruct",
                                        DataTypes.createStructType(Lists.newArrayList(
                                                DataTypes.createStructField("col1", DataTypes.LongType, true),
                                                DataTypes.createStructField("col2", DataTypes.LongType, true)
                                        )), true),
                                DataTypes.createStructField("str", DataTypes.StringType, true)
                        )), true),
                        DataTypes.createStructField("struct",
                                DataTypes.createStructType(
                                        Lists.newArrayList(DataTypes.createStructField("col1", DataTypes.LongType, true),
                                        DataTypes.createStructField("col2", DataTypes.LongType, true),
                                        DataTypes.createStructField("col3", DataTypes.LongType, true),
                                        DataTypes.createStructField("subArray",
                                                DataTypes.createArrayType(
                                                        DataTypes.LongType
                                                ),
                                                true)
                                    )),true)
                ));

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(df);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(JSON_SAMPLE_PATH));
    }


    private void executeQueryWithSchema(String queryStr, StructType schema) {
        LazyHolder.sparkSession.read().schema(schema).parquet(PARQUET_PATH).createOrReplaceTempView(SAMPLE_PARQUET);
        LazyHolder.sparkSession.sql(queryStr).count();
    }

    private void assertSchemasEqual(StructType expectedSchema, StructType actualSchema) {
        final Seq<StructField> diffA = expectedSchema.toSeq().diff(actualSchema.toSeq());
        assertTrue("Missing fields in generated schema: " + diffA.toString(), diffA.isEmpty());
        final Seq<StructField> diffB = actualSchema.toSeq().diff(expectedSchema.toSeq());
        assertTrue("Missing fields in expected schema: " + diffB.toString(), diffB.isEmpty());
    }

}