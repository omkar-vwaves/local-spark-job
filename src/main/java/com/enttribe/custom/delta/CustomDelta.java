package com.enttribe.custom.delta;

// import java.nio.file.Files;
// import java.nio.file.Paths;
// import java.util.Arrays;
// import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// import org.apache.spark.sql.types.DataTypes;
// import org.apache.spark.sql.types.StructType;
// import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

public class CustomDelta extends Processor {

    // public static void main(String[] args) throws Exception {

    // try {

    // SparkConf conf = getSparkConfiguration();
    // JavaSparkContext sc = getSparkContext(conf);

    // SparkSession spark = SparkSession.builder()
    // .config(conf)
    // .appName("CustomDelta")
    // .master("local[*]")
    // .getOrCreate();

    // startProcess(spark);

    // sc.stop();

    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        Dataset<Row> inputDF = this.dataFrame;
        startProcess(jobContext.sqlctx().sparkSession(), inputDF);

        return null;
    }

    private static Dataset<Row> startProcess(SparkSession spark, Dataset<Row> inputDF) {
        try {
            System.out.println("Starting Delta Processing");

            // Register the input DataFrame as a temp view
            inputDF.createOrReplaceTempView("inputDF");

            // 1. Select and cache relevant columns
            String parsedDFSql = """
                SELECT PT, mapKey, counterValueMap FROM inputDF
            """;
            Dataset<Row> parsedDF = spark.sql(parsedDFSql);
            parsedDF.createOrReplaceTempView("parsedDF");
            parsedDF.cache();
            System.out.println("Parsed DataFrame from UDF:");
            parsedDF.show(100, false);
            System.out.println("--------------------------------");

            // 2. Extract baseKey from mapKey (remove trailing ##\d+)
            String withBaseKeyDFSql = """
                SELECT *, regexp_replace(mapKey, '##\\d+$', '') AS baseKey FROM parsedDF
            """;
            Dataset<Row> withBaseKeyDF = spark.sql(withBaseKeyDFSql);
            withBaseKeyDF.createOrReplaceTempView("withBaseKeyDF");

            // 3. Explode counterValueMap into rows: PT, baseKey, counterName, counterValue
            String explodedCountersDFSql = """
                SELECT PT, baseKey, explode(counterValueMap) as kv FROM withBaseKeyDF
            """;
            Dataset<Row> explodedCountersDF = spark.sql(explodedCountersDFSql)
                .selectExpr("PT", "baseKey", "kv.key as counterName", "kv.value as counterValue");
            explodedCountersDF.createOrReplaceTempView("explodedCountersDF");

            // 4. Add type columns
            String withTypesDFSql = """
                SELECT *, cast(counterValue as double) as counterValueDouble, cast(counterValue as string) as counterValueString FROM explodedCountersDF
            """;
            Dataset<Row> withTypesDF = spark.sql(withTypesDFSql);
            withTypesDF.createOrReplaceTempView("withTypesDF");

            // 5. Numeric counters: where counterValueDouble is not null
            String numericCountersDFSql = """
                SELECT * FROM withTypesDF WHERE counterValueDouble IS NOT NULL
            """;
            Dataset<Row> numericCountersDF = spark.sql(numericCountersDFSql);
            numericCountersDF.createOrReplaceTempView("numericCountersDF");

            // 6. Non-numeric counters: where counterValueDouble is null and counterValueString is not null
            String nonNumericCountersDFSql = """
                SELECT * FROM withTypesDF WHERE counterValueDouble IS NULL AND counterValueString IS NOT NULL
            """;
            Dataset<Row> nonNumericCountersDF = spark.sql(nonNumericCountersDFSql);
            nonNumericCountersDF.createOrReplaceTempView("nonNumericCountersDF");

            // 7. Group by and aggregate numeric counters (average)
            String averagedCountersDFSql = """
                SELECT PT, baseKey, counterName, avg(counterValueDouble) as aggValue FROM numericCountersDF GROUP BY PT, baseKey, counterName
            """;
            Dataset<Row> averagedCountersDF = spark.sql(averagedCountersDFSql);
            averagedCountersDF.createOrReplaceTempView("averagedCountersDF");

            // 8. Group by and aggregate non-numeric counters (last non-null value)
            String lastNonNullCountersDFSql = """
                SELECT PT, baseKey, counterName, last(counterValueString, true) as aggValue FROM nonNumericCountersDF GROUP BY PT, baseKey, counterName
            """;
            Dataset<Row> lastNonNullCountersDF = spark.sql(lastNonNullCountersDFSql);
            lastNonNullCountersDF.createOrReplaceTempView("lastNonNullCountersDF");

            // 9. Union both
            String allCountersDFSql = """
                SELECT * FROM averagedCountersDF
                UNION ALL
                SELECT * FROM lastNonNullCountersDF
            """;
            Dataset<Row> allCountersDF = spark.sql(allCountersDFSql);
            allCountersDF.createOrReplaceTempView("allCountersDF");

            // 10. Reconstruct the map for each (PT, baseKey)
            String countersMapDFSql = """
                SELECT PT, baseKey, map_from_entries(collect_list(named_struct('key', counterName, 'value', aggValue))) as countersMap
                FROM allCountersDF
                GROUP BY PT, baseKey
            """;
            Dataset<Row> countersMapDF = spark.sql(countersMapDFSql);
            countersMapDF.createOrReplaceTempView("countersMapDF");

            // 11. Reconstruct the final parsedCounterMap per PT
            String parsedCounterMapDFSql = """
                SELECT PT, map_from_entries(collect_list(named_struct('key', baseKey, 'value', countersMap))) as parsedCounterMap
                FROM countersMapDF
                GROUP BY PT
            """;
            Dataset<Row> parsedCounterMapDF = spark.sql(parsedCounterMapDFSql);
            parsedCounterMapDF.createOrReplaceTempView("parsedCounterMapDF");

            System.out.println("Aggregated parsedCounterMap DataFrame:");
            parsedCounterMapDF.show(100, false);
            parsedCounterMapDF.printSchema();
            System.out.println("--------------------------------");

            // 12. Explode parsedCounterMap
            String explodedMapDFSql = """
                SELECT PT, explode(parsedCounterMap) as kv FROM parsedCounterMapDF
            """;
            Dataset<Row> explodedMapDF = spark.sql(explodedMapDFSql)
                .selectExpr("PT", "kv.key as mapKey", "kv.value as counterValueMap");
            explodedMapDF.createOrReplaceTempView("explodedMapDF");

            System.out.println("Exploded parsedCounterMap DataFrame:");
            explodedMapDF.show(100, false);
            explodedMapDF.printSchema();
            System.out.println("--------------------------------");

            // 13. Clean up mapKey
            String cleanedDFSql = """
                SELECT PT, concat(regexp_extract(mapKey, '^(.*##\\d{14})', 1), '##') as mapKey, counterValueMap FROM explodedMapDF
            """;
            Dataset<Row> cleanedDF = spark.sql(cleanedDFSql);

            System.out.println("Final Cleaned DataFrame (PT, mapKey, counterValueMap):");
            cleanedDF.show(100, false);
            cleanedDF.printSchema();
            System.out.println("--------------------------------");

            return cleanedDF;

        } catch (Exception e) {
            System.out.println("Exception While Starting Process, Message: " + e.getMessage() + " | Error: " + e);
            return inputDF;
        }
    }

    // private static SparkConf getSparkConfiguration() {
    //     try {
    //         return new SparkConf()
    //                 .setAppName("CustomDelta")
    //                 .setMaster("local[*]")
    //                 .set("spark.driver.memory", "1g")
    //                 .set("spark.task.maxFailures", "10")
    //                 .set("spark.stage.maxAttempts", "10")
    //                 .set("spark.network.timeout", "1800s")
    //                 .set("spark.executor.heartbeatInterval", "120s")
    //                 .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    //                 .set("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED")
    //                 .set("spark.executor.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED");

    //     } catch (Exception e) {
    //         System.out.println(
    //                 "Exception While Configuring Spark Properties, Message: " + e.getMessage() + " | Error: " + e);
    //         return null;
    //     }
    // }

    // private static JavaSparkContext getSparkContext(SparkConf conf) {
    //     try {
    //         return new JavaSparkContext(conf);
    //     } catch (Exception e) {
    //         System.out.println(
    //                 "Exception While Creating Java Spark Context, Message: " + e.getMessage() + " | Error: " + e);
    //         return null;
    //     }
    // }

}
