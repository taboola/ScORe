package com.taboola.spark.sql.schemagen;

import static org.apache.spark.sql.functions.broadcast;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

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
import com.taboola.testing.BaseUnitTest;

import scala.collection.Seq;


public class SchemaOnReadGeneratorTest extends BaseUnitTest {

    public static final String PAGEVIEWS_PARQUET_PATH = "src/test/resources/parquet-folder/pageviews.parquet";
    public static final String PAGEVIEWS = "pageviews";

    public static final String JSON_SAMPLE_PATH = "src/test/resources/sample.json";
    public static final String SAMPLE = "sample";

    private static UDF2<String, Long, String> someUdf;

    static class LazyHolder {
        private static SparkSession sparkSession;
        private static Dataset<Row> parquetDF;
        private static Dataset<Row> jsonDF;

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
            parquetDF = sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH);
            jsonDF = sparkSession.read().option("multiline", true).json(JSON_SAMPLE_PATH);


            someUdf = (UDF2<String, Long, String>) (s, aLong) -> s;
            sparkSession.udf().register("someUdf", someUdf, DataTypes.StringType);

        }

        static void init() {
        }
    }

    @BeforeClass
    public static void beforeClass() {
        LazyHolder.init();
    }

    @Before
    public void beforeTest() {
        LazyHolder.parquetDF.createOrReplaceTempView(PAGEVIEWS);
        LazyHolder.jsonDF.createOrReplaceTempView(SAMPLE);
    }

    @Test
    public void test_original() {
        assertSchemasEqual(
                LazyHolder.parquetDF.schema(),
                SchemaOnReadGenerator.generateSchemaOnRead(LazyHolder.parquetDF).getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_fullSchemaWithPredicate() {
        final Dataset<Row> query = LazyHolder.parquetDF.filter("pv_publisherId = 5");
        assertSchemasEqual(
                LazyHolder.parquetDF.schema(),
                SchemaOnReadGenerator.generateSchemaOnRead(query).getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_fullSchemaWithSubQuery() {
        LazyHolder.parquetDF.filter("pv_publisherId = 5").createOrReplaceTempView("filtered");
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT pv_userId from filtered");
        assertSchemasEqual(
                DataTypes.createStructType(
                        Lists.newArrayList(DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                                DataTypes.createStructField("pv_userId", DataTypes.StringType, true))
                ),
                SchemaOnReadGenerator.generateSchemaOnRead(query).getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_fullSchemaWithSubQueryNoAlias() {
        final Dataset<Row> query = LazyHolder.parquetDF.select("pv_publisherId", "pv_userId").filter("pv_publisherId = 5").select("pv_userId");
        assertSchemasEqual(
                DataTypes.createStructType(
                        Lists.newArrayList(DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                                DataTypes.createStructField("pv_userId", DataTypes.StringType, true))
                ),
                SchemaOnReadGenerator.generateSchemaOnRead(query).getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
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
        assertEquals(10l, generator.getDuration());
    }

    @Test
    public void test_simpleQueryApi() {
        final Dataset<Row> query = LazyHolder.parquetDF.select("pv_publisherId", "request.listId", "servedItem.cpc");
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_simpleQuerySql() {
        final String queryStr = "SELECT pv_publisherId, request.listId, servedItem.cpc FROM pageviews";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_simpleQuerySqlCaseInsensitive() {
        final String queryStr = "SELECT pv_PublisherId, requesT.listId, servedItem.cPc FROM pageviews";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_withNestedArrays() {
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("rankerVerboseDataList",
                                                        DataTypes.createArrayType(
                                                                DataTypes.createStructType(
                                                                        Lists.newArrayList(
                                                                                DataTypes.createStructField("blockedItemsList",
                                                                                        DataTypes.createArrayType(DataTypes.createStructType(
                                                                                                Lists.newArrayList(DataTypes.createStructField("blockReason", DataTypes.StringType, true))
                                                                                        )), true))
                                                                )), true))
                                ), true)

                )
        );

        final String[] inputPath = {PAGEVIEWS_PARQUET_PATH};

        String queryStr = "SELECT request.rankerVerboseDataList[0].blockedItemsList[0].blockReason FROM pageviews";
        Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));

        queryStr = "SELECT request.rankerVerboseDataList.blockedItemsList[0].blockReason FROM pageviews";
        query = LazyHolder.sparkSession.sql(queryStr);
        generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));

        queryStr = "SELECT request.rankerVerboseDataList[0].blockedItemsList.blockReason FROM pageviews";
        query = LazyHolder.sparkSession.sql(queryStr);
        generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));
    }


    @Test
    public void test_withNestedMap() {
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("multiRequestTrcMetrics_dynamicMetricsMap",
                                                        DataTypes.createMapType(
                                                                DataTypes.StringType,
                                                                DataTypes.createArrayType(DataTypes.LongType)
                                                        ), true)

                                        )), true)
                ));


        final String[] inputPath = {PAGEVIEWS_PARQUET_PATH};

        String queryStr = "SELECT request.multiRequestTrcMetrics_dynamicMetricsMap['key'][0] " +
                "FROM pageviews";
        Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(inputPath));
    }

    @Test
    public void test_nestedStruct() {
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");

        final Dataset<Row> dataset2 = LazyHolder.sparkSession.read().option("multiline", true).json("src/test/resources/sample2.json");
        dataset2.registerTempTable("sample2");

        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT someStr FROM sample")
                .union(LazyHolder.sparkSession.sql("SELECT nestedStruct.str FROM sample2"));

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
        assertSchemasEqual(expectedSchema2, generator.getSchemaOnRead("src/test/resources/sample2.json"));
    }

    @Test
    public void test_unionWithoutAlias() {
        final Dataset<Row> query = LazyHolder.jsonDF.select("someStr")
                .union(LazyHolder.sparkSession.read().option("multiline", true).json("src/test/resources/sample2.json").select("nestedStruct.str"));

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
        assertSchemasEqual(expectedSchema2, generator.getSchemaOnRead("src/test/resources/sample2.json"));
    }


    @Test
    public void test_explodeSubArray() {
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final String queryStr = "SELECT pageviews.pv_publisherId, pageviews.request.listId, pageviews.servedItem.cpc FROM pageviews";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_simpleQuerySqlWithWhereClauseNotInSelect() {
        final String queryStr = "SELECT pageviews.pv_publisherId, pageviews.request.listId, pageviews.servedItem.cpc " +
                "FROM pageviews " +
                "WHERE servedItem.itemProviderType = 'SP'";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true),
                                                DataTypes.createStructField("itemProviderType", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_innerQuerySQL() {
        final String queryStr = "WITH base as (SELECT pageviews.pv_publisherId as publisherId,  pageviews.servedItem FROM pageviews) " +
                "SELECT publisherId FROM base";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),

                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Collections.emptyList()
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_groupBySql() {
        final String queryStr = "SELECT pageviews.pv_publisherId, pageviews.request.listId, sum(pageviews.servedItem.cpc) as total_cpc " +
                "FROM pageviews " +
                "GROUP BY 1, pageviews.request.listId";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupBySql_with_count_1_with_order() {
        final String queryStr = "SELECT  servedItem.itemId, count(1) " +
                "FROM pageviews " +
                "group by 1 order by 2";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("itemId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupBySql_with_count_star_with_order() {
        final String queryStr = "SELECT  servedItem.itemId, count(*) " +
                "FROM pageviews " +
                "group by 1 order by 2 desc ";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("itemId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupBySql_with_count_field_with_order() {
        final String queryStr = "SELECT  servedItem.itemId, count(servedItem.itemProviderType) " +
                "FROM pageviews " +
                "group by 1 order by 2 desc ";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("itemId", DataTypes.LongType, true),
                                                DataTypes.createStructField("itemProviderType", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_groupByNotInQuerySql() {
        final String queryStr = "SELECT pageviews.pv_publisherId, sum(pageviews.servedItem.cpc) as total_cpc " +
                "FROM pageviews " +
                "GROUP BY pageviews.pv_publisherId, pageviews.request.listId";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_groupBySqlInnerQuery() {
        final String queryStr = "WITH base as (SELECT pageviews.pv_publisherId, pageviews.request.listId, sum(pageviews.servedItem.cpc) as total_cpc " +
                "FROM pageviews " +
                "GROUP BY 1, pageviews.request.listId) " +
                "SELECT pv_publisherId, total_cpc FROM base";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_conditionalAggregateSql() {
        final String queryStr = "WITH base as (SELECT pageviews.pv_publisherId, pageviews.request.listId, sum(if(servedItem.clicked, pageviews.servedItem.cpc, 0)) as total_cpc " +
                "FROM pageviews " +
                "GROUP BY 1, pageviews.request.listId) " +
                "SELECT pv_publisherId, total_cpc FROM base";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("clicked", DataTypes.BooleanType, true),
                                                DataTypes.createStructField("cpc", DataTypes.DoubleType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test()
    public void test_selfJoin() {
        final String queryStr = "SELECT a.pv_publisherId, a.pv_userId, b.servedItem.syndicatorId " +
                "FROM pageviews a JOIN pageviews b " +
                "ON a.pv_publisherId = b.pv_publisherId";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_join() {
        LazyHolder.sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH).createOrReplaceTempView("pageviews2");
        final String queryStr = "SELECT a.pv_publisherId, a.pv_userId, b.servedItem.syndicatorId " +
                "FROM pageviews a JOIN pageviews2 b " +
                "ON a.pv_publisherId = b.pv_publisherId";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias("pageviews2"));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_joinWhereInsteadOfOn() {
        LazyHolder.sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH).createOrReplaceTempView("pageviews2");
        final String queryStr = "SELECT a.pv_publisherId, a.pv_userId, b.servedItem.syndicatorId " +
                "FROM pageviews a JOIN pageviews2 b " +
                "WHERE a.pv_publisherId = b.pv_publisherId";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias("pageviews2"));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_filterPreAliasWithImplicitJoin() {
        final Dataset<Row> base = LazyHolder.parquetDF.filter("request_available = true");
        base.createOrReplaceTempView("base");
        List<MyPojo> stringsList = Lists.newArrayListWithCapacity(0);
        final Dataset<Row> stringsDF_one = LazyHolder.sparkSession.createDataFrame(stringsList, MyPojo.class);
        stringsDF_one.registerTempTable("strings_one");
        final Dataset<Row> stringsDF_two = LazyHolder.sparkSession.createDataFrame(stringsList, MyPojo.class);
        stringsDF_two.registerTempTable("strings_two");


        final String queryStr = "SELECT a.pv_publisherId " +
                "FROM base a, strings_one b " +
                "WHERE a.pv_userId = b.pv_userAgent";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("request_available", DataTypes.BooleanType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }


    private static class MyPojo {
        private String pv_userAgent;

        public String getPv_userAgent() {
            return pv_userAgent;
        }

        public void setPv_userAgent(String pv_userAgent) {
            this.pv_userAgent = pv_userAgent;
        }
    }

    @Test
    public void test_joinApi() {
        final Dataset<Row> df1 = LazyHolder.parquetDF;
        final Dataset<Row> df2 = LazyHolder.parquetDF;

        final Dataset<Row> query = df1.select(df1.col("pv_publisherId"), df1.col("pv_userId"))
                .join(df2.select(df2.col("pv_publisherId"), df2.col("servedItem.syndicatorId")), "pv_publisherId")
                .select("pv_publisherId", "pv_userId", "syndicatorId");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_broadcastJoin() {
        LazyHolder.sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH).createOrReplaceTempView("pageviews2");
        final String queryStr = "SELECT /*+ BROADCAST(b) */ a.pv_publisherId, a.pv_userId, b.servedItem.syndicatorId " +
                "FROM pageviews a JOIN pageviews2 b " +
                "ON a.pv_publisherId = b.pv_publisherId";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        query.explain();
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias("pageviews2"));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }


    @Test
    public void test_broadcastJoinApi() {
        final Dataset<Row> df1 = LazyHolder.parquetDF;
        final Dataset<Row> df2 = LazyHolder.parquetDF;

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        Dataset<Row> query = broadcast(df1).select(df1.col("pv_publisherId"), df1.col("pv_userId"))
                .join(df2.select(df2.col("pv_publisherId"), df2.col("servedItem.syndicatorId")), "pv_publisherId")
                .select("pv_publisherId", "pv_userId", "syndicatorId");

        SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));


        query = df1.select(df1.col("pv_publisherId"), df1.col("pv_userId"))
                .join(broadcast(df2).select(df2.col("pv_publisherId"), df2.col("servedItem.syndicatorId")), "pv_publisherId")
                .select("pv_publisherId", "pv_userId", "syndicatorId");

        generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }


    @Test
    public void test_simpleWindowFunction() {
        final String queryStr = "SELECT pv_publisherId, " +
                "FIRST(pv_userAgent) OVER (PARTITION BY pv_userId ORDER BY pv_browserName) " +
                "FROM pageviews";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_browserName", DataTypes.StringType, true),
                        DataTypes.createStructField("pv_userAgent", DataTypes.StringType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
    }

    @Test
    public void test_veryComplexWindowThatLacksWindowColumnsFromProject() {
        final String queryStr =
                    "SELECT pv_publisherId, servedItem.itemId as itemId, \n" +
                    "FIRST_VALUE(servedItem.itemCreateTime) OVER w as itemCreateTime, \n" +
                    "LAST_VALUE(servedItem.syndicatorId) OVER w as syndicatorId \n" +
                    "FROM pageviews \n" +
                    "WHERE pv_available = true \n" +
                    "AND servedItem.clicked = true \n" +
                    "WINDOW w AS (PARTITION BY pv_userId, pv_browserName \n" +
                    "ORDER BY servedItem.campaignId ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)";
        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr);
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_available", DataTypes.BooleanType, true),
                        DataTypes.createStructField("pv_browserName", DataTypes.StringType, true),
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("campaignId", DataTypes.LongType, true),
                                                DataTypes.createStructField("clicked", DataTypes.BooleanType, true),
                                                DataTypes.createStructField("itemCreateTime", DataTypes.LongType, true),
                                                DataTypes.createStructField("itemId", DataTypes.LongType, true),
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true)
                                        )
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }


    @Test
    public void test_cache() {
        final String queryStr = "SELECT pv_publisherId, pv_userId, pv_browserName, pv_userAgent, request.listId as listId " +
                "FROM pageviews";

        final Dataset<Row> query = LazyHolder.sparkSession.sql(queryStr).cache();

        final Dataset<Row> queryOnCache = query.select("pv_publisherId", "listId");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true),
                        DataTypes.createStructField("pv_browserName", DataTypes.StringType, true),
                        DataTypes.createStructField("pv_userAgent", DataTypes.StringType, true),
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("listId", DataTypes.StringType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(queryOnCache);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnReadByAlias(PAGEVIEWS));
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        executeQueryWithSchema(queryStr, expectedSchema);
        query.unpersist();
    }


    @Test
    public void test_readMultipleFiles() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH,
                "src/test/resources/parquet-folder/pageviews_2.parquet").select("pv_publisherId");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH,
                "src/test/resources/parquet-folder/pageviews_2.parquet"));
    }

    @Test
    public void test_readMultipleFilesWildcards() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/parquet-folder/*.parquet").select("pv_publisherId");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH,
                "src/test/resources/parquet-folder/pageviews_2.parquet"));
    }


    @Test
    public void test_readMultipleFilesUnordered() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/parquet-folder/pageviews_2.parquet",
                PAGEVIEWS_PARQUET_PATH).select("pv_publisherId");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(
                "src/test/resources/parquet-folder/pageviews_2.parquet",
                PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_readMultipleFilesDuplicate() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH,
                PAGEVIEWS_PARQUET_PATH).select("pv_publisherId");


        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH, PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_readMultipleFilesJoin() {
        LazyHolder.sparkSession.read().parquet(PAGEVIEWS_PARQUET_PATH).createOrReplaceTempView("pageviews");
        LazyHolder.sparkSession.read().parquet("src/test/resources/parquet-folder/pageviews_2.parquet").createOrReplaceTempView("pageviews2");

        final String queryStr = "SELECT a.pv_publisherId, a.pv_userId, b.servedItem.syndicatorId " +
                "FROM pageviews a JOIN pageviews2 b " +
                "ON a.pv_publisherId = b.pv_publisherId";


        StructType schema1 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("pv_userId", DataTypes.StringType, true)
                )
        );

        StructType schema2 = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true),
                        DataTypes.createStructField("servedItem",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("syndicatorId", DataTypes.LongType, true))
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(LazyHolder.sparkSession.sql(queryStr));
        assertSchemasEqual(schema1, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
        assertSchemasEqual(schema2, generator.getSchemaOnRead("src/test/resources/parquet-folder/pageviews_2.parquet"));
    }

    @Test
    public void test_variousExpressions() {
        final Dataset<Row> dataset = LazyHolder.jsonDF;
        dataset.registerTempTable("sample");
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
        final Dataset<Row> query = LazyHolder.sparkSession.sql("SELECT pv_performanceMeasurements['key'][0].measureName," +
                "pv_performanceMeasurements['key'][1].mode " +
                "FROM pageviews " +
                "WHERE pv_performanceMeasurements['key'][2].duration < 200");

        final StructType expectedSchema =
                DataTypes.createStructType(
                        Lists.newArrayList(
                                DataTypes.createStructField("pv_performanceMeasurements",
                                        DataTypes.createMapType(
                                                DataTypes.StringType,
                                                DataTypes.createArrayType(DataTypes.createStructType(
                                                        Lists.newArrayList(
                                                                DataTypes.createStructField("duration", DataTypes.LongType, true),
                                                                DataTypes.createStructField("measureName", DataTypes.StringType, true),
                                                                DataTypes.createStructField("mode", DataTypes.StringType, true)
                                                        )
                                                ))
                                        ), true)
                        )
                );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);
        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_parquetFolder() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/parquet-folder").select("pv_publisherId");
        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead("src/test/resources/parquet-folder/pageviews.parquet", "src/test/resources/parquet-folder/pageviews_2.parquet"));
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
        final String query =
                "SELECT " +
                        "event.rst AS rst, " +
                        "event.vRevenue as vRevenue " +
                        "FROM pageviews " +
                        "LATERAL VIEW EXPLODE(request.videoEvents_events) as event ";

        final Dataset<Row> dataset = LazyHolder.sparkSession.sql(query);

        final StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("request",
                                DataTypes.createStructType(
                                        Lists.newArrayList(
                                                DataTypes.createStructField("videoEvents_events",
                                                        DataTypes.createArrayType(
                                                                DataTypes.createStructType(
                                                                        Lists.newArrayList(
                                                                                DataTypes.createStructField("rst", DataTypes.LongType, true),
                                                                                DataTypes.createStructField("vRevenue", DataTypes.DoubleType, true)
                                                                        )
                                                                )
                                                        ), true)
                                        )
                                ), true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(dataset);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead(PAGEVIEWS_PARQUET_PATH));
    }

    @Test
    public void test_parquetFolderPartitioned() {
        final Dataset<Row> query = LazyHolder.sparkSession.read().parquet("src/test/resources/parquet-folder-partitioned").select("pv_publisherId");

        StructType expectedSchema = DataTypes.createStructType(
                Lists.newArrayList(
                        DataTypes.createStructField("pv_publisherId", DataTypes.LongType, true)
                )
        );

        final SchemaOnReadGenerator generator = SchemaOnReadGenerator.generateSchemaOnRead(query);

        assertSchemasEqual(expectedSchema, generator.getSchemaOnRead("src/test/resources/parquet-folder-partitioned/pv_publisherId=6/pageviews_2.parquet",
                "src/test/resources/parquet-folder-partitioned/pv_publisherId=1231231231231235/pageviews.parquet"));
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
        final Dataset<Row> df = LazyHolder.sparkSession.sql("WITH base AS( SELECT struct as myStruct FROM " + SAMPLE  +
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
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT someArrayOfComplexArrays FROM " + SAMPLE );
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
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT nestedStruct, nestedStruct.str FROM " + SAMPLE );

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
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct.str) FROM " + SAMPLE + " GROUP BY nestedStruct");

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
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct) FROM " + SAMPLE + " GROUP BY nestedStruct.str");

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
        Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct.str) as str FROM " + SAMPLE + " GROUP BY nestedStruct");
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
        Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT first(nestedStruct) as mystruct FROM " + SAMPLE + " GROUP BY nestedStruct.str");

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
        final Dataset<Row> df = LazyHolder.sparkSession.sql("SELECT nestedStruct as struct FROM " + SAMPLE);

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
        LazyHolder.sparkSession.read().schema(schema).parquet(PAGEVIEWS_PARQUET_PATH).createOrReplaceTempView(PAGEVIEWS);
        LazyHolder.sparkSession.sql(queryStr).count();
    }

    private void assertSchemasEqual(StructType expectedSchema, StructType actualSchema) {
        final Seq<StructField> diffA = expectedSchema.toSeq().diff(actualSchema.toSeq());
        assertTrue("Missing fields in generated schema: " + diffA.toString(), diffA.isEmpty());
        final Seq<StructField> diffB = actualSchema.toSeq().diff(expectedSchema.toSeq());
        assertTrue("Missing fields in expected schema: " + diffB.toString(), diffB.isEmpty());
    }

}