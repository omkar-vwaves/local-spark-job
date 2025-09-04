package com.enttribe.pm.job.report.common;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.CacheDatasetCustom;
import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCReadCustom;

public class ReportMain {

        public static JobContext jobContext = null;

        private static Logger logger = LoggerFactory.getLogger(ReportMain.class);

        public static void main(String[] args) {

                long startTime = System.currentTimeMillis();
                logger.info("🚀 ReportMain Execution Started!");

                SparkConf conf = getSparkConf();

                SparkSession session = SparkSession.builder()
                                .config(conf)
                                .getOrCreate();

                jobContext = new JobContextImpl(session);
                jobContext = setParametersToJobContext(jobContext);

                Dataset<Row> dataFrame = session.emptyDataFrame();

                try {

                        /*
                         * READ OTF REPORT CONFIGURATION
                         */

                        String READ_OTF_REPORT_CONFIGURATION_QUERY = "SELECT REPORT_WIDGET_ID_PK, GENERATED_REPORT_ID, DOMAIN, VENDOR, TECHNOLOGY, REPORT_MEASURE, REPORT_NAME, FREQUENCY, GENERATED_TYPE, CONFIGURATION FROM REPORT_WIDGET WHERE REPORT_WIDGET_ID_PK = ${REPORT_WIDGET_ID_PK}";

                        READ_OTF_REPORT_CONFIGURATION_QUERY = READ_OTF_REPORT_CONFIGURATION_QUERY.replace(
                                        "${REPORT_WIDGET_ID_PK}",
                                        "16064"); // 16216

                        logger.info("📊 READ OTF REPORT CONFIGURATION QUERY: {}", READ_OTF_REPORT_CONFIGURATION_QUERY);

                        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

                        dataFrame = new JDBCReadCustom(dataFrame, 1, "READ OTF REPORT CONFIGURATION",
                                        SPARK_PM_JDBC_DRIVER, SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                                        SPARK_PM_JDBC_PASSWORD,
                                        READ_OTF_REPORT_CONFIGURATION_QUERY, "REPORT_WIDGET", null, null, null, null,
                                        null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5);
                        logger.info("🚀 READ OTF REPORT CONFIGURATION Executed Successfully! ✅");

                        /*
                         * READ CONFIG UDF
                         */

                        ReadConfigUDF readConfigUDF = new ReadConfigUDF();
                        readConfigUDF.jobContext = jobContext;
                        jobContext.sqlctx().udf().register(readConfigUDF.getName(), readConfigUDF,
                                        readConfigUDF.getReturnType());

                        String READ_CONFIG_UDF_QUERY = "SELECT ReadReportConfig(CAST(REPORT_WIDGET_ID_PK AS STRING), CAST(GENERATED_REPORT_ID AS STRING), DOMAIN, VENDOR, TECHNOLOGY, REPORT_MEASURE, REPORT_NAME, FREQUENCY, GENERATED_TYPE, CONFIGURATION) AS REPORT_DETAILS FROM REPORT_WIDGET";

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 1, "READ CONFIG UDF",
                                        READ_CONFIG_UDF_QUERY, "UDF_RESULT", null)
                                        .executeAndGetResultDataframe(jobContext);

                        /*
                         * CACHE UDF RESULT
                         */

                        dataFrame = new CacheDatasetCustom(dataFrame, 1, "CACHE UDF RESULT", "memoryAndDisk",
                                        "CACHE_UDF_RESULT")
                                        .executeAndGetResultDataframe(jobContext);

                        logger.info("🚀 READ CONFIG UDF Executed Successfully! ✅");

                        /*
                         * EXPLODE UDF
                         */

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 1, "READ CONFIG UDF",
                                        "SELECT EXPLODE_RESULT.* FROM (SELECT EXPLODE(REPORT_DETAILS) AS EXPLODE_RESULT FROM UDF_RESULT) tmp",
                                        "EXPLODE_UDF_RESULT", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5);
                        logger.info("🚀 EXPLODE UDF Executed Successfully! ✅");

                        /*
                         * CUSTOM CQL READ
                         */

                        CustomCQLRead customCQLRead = new CustomCQLRead();
                        customCQLRead.dataFrame = dataFrame;
                        dataFrame = customCQLRead.executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5);
                        logger.info("🚀 CUSTOM CQL READ Executed Successfully! ✅");

                        /*
                         * CACHE DATASET
                         */

                        dataFrame = new CacheDatasetCustom(dataFrame, 1, "CACHE DATASET", "memoryAndDisk",
                                        "CACHE_CQL_RESULT")
                                        .executeAndGetResultDataframe(jobContext);

                        String REPORT_MEASURE = jobContext.getParameter("REPORT_MEASURE");

                        if (REPORT_MEASURE.equalsIgnoreCase("Performance Report_ONDEMAND")
                                        || REPORT_MEASURE.equalsIgnoreCase("Performance Report")) {

                                /*
                                 * PROCESS METRIC DF
                                 */

                                ProcessMetricDF processMetricDF = new ProcessMetricDF();
                                processMetricDF.dataFrame = dataFrame;
                                dataFrame = processMetricDF.executeAndGetResultDataframe(jobContext);

                                /*
                                 * CACHE EXPECTED DATAFRAME
                                 */

                                dataFrame = new CacheDatasetCustom(dataFrame, 1, "CACHE EXPECTED DATAFRAME",
                                                "memoryAndDisk",
                                                "CACHE_EXPECTED_DATAFRAME").executeAndGetResultDataframe(jobContext);

                                /*
                                 * METRIC CSV/EXCEL
                                 */

                                WriteMetricReport writeMetricReport = new WriteMetricReport();
                                writeMetricReport.dataFrame = dataFrame;
                                writeMetricReport.executeAndGetResultDataframe(jobContext);
                        }
                        if (REPORT_MEASURE.equalsIgnoreCase("Trend Report_ONDEMAND")
                                        || REPORT_MEASURE.equalsIgnoreCase("Trend Report")) {

                                /*
                                 * PROCESS TREND DF
                                 */

                                ProcessTrendDF processTrendDF = new ProcessTrendDF();
                                processTrendDF.dataFrame = dataFrame;
                                dataFrame = processTrendDF.executeAndGetResultDataframe(jobContext);

                                /*
                                 * CACHE EXPECTED DATAFRAME
                                 */

                                dataFrame = new CacheDatasetCustom(dataFrame, 1, "CACHE EXPECTED DATAFRAME",
                                                "memoryAndDisk",
                                                "CACHE_EXPECTED_DATAFRAME").executeAndGetResultDataframe(jobContext);

                                /*
                                 * WRITE TREND REPORT WITH RAG
                                 */

                                WriteTrendReportWithRag writeTrendReportWithRag = new WriteTrendReportWithRag();
                                writeTrendReportWithRag.dataFrame = dataFrame;
                                writeTrendReportWithRag.executeAndGetResultDataframe(jobContext);
                        }
                        if (REPORT_MEASURE.equalsIgnoreCase("Exception Report_ONDEMAND")
                                        || REPORT_MEASURE.equalsIgnoreCase("Exception Report")) {

                                /*
                                 * PROCESS EXCEPTION DF
                                 */

                                ProcessExceptionDF processExceptionDF = new ProcessExceptionDF();
                                processExceptionDF.dataFrame = dataFrame;
                                dataFrame = processExceptionDF.executeAndGetResultDataframe(jobContext);

                                /*
                                 * CACHE EXPECTED DATAFRAME
                                 */

                                dataFrame = new CacheDatasetCustom(dataFrame, 1, "CACHE EXPECTED DATAFRAME",
                                                "memoryAndDisk",
                                                "CACHE_EXPECTED_DATAFRAME").executeAndGetResultDataframe(jobContext);

                                /*
                                 * WRITE EXCEPTION REPORT
                                 */

                                WriteExceptionReport writeExceptionReport = new WriteExceptionReport();
                                writeExceptionReport.dataFrame = dataFrame;
                                writeExceptionReport.executeAndGetResultDataframe(jobContext);

                        }

                        UpdateReportDetails updateReportDetails = new UpdateReportDetails(dataFrame, 1,
                                        "UPDATE REPORT DETAILS");
                        updateReportDetails.dataFrame = dataFrame;
                        updateReportDetails.executeAndGetResultDataframe(jobContext);

                } catch (Exception e) {
                        logger.error("Exception Occurred in ReportMain: {}", e.getMessage());
                        e.printStackTrace();
                }

                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                logger.info("🚀 ReportMain Execution Completed in {} Minutes {} Seconds", executionTime / 60000,
                                (executionTime % 60000) / 1000);
        }

        private static JobContext setParametersToJobContext(JobContext jobContext) {

                jobContext.setParameters("SPARK_PM_JDBC_URL",
                                "jdbc:mysql://localhost:3306/PERFORMANCE_A_LAB?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
                jobContext.setParameters("SPARK_PM_JDBC_DRIVER", "org.mariadb.jdbc.Driver");
                jobContext.setParameters("SPARK_PM_JDBC_USERNAME", "root");
                jobContext.setParameters("SPARK_PM_JDBC_PASSWORD", "root");

                jobContext.setParameters("SPARK_FM_JDBC_URL",
                                "jdbc:mysql://localhost:3306/FMS?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
                jobContext.setParameters("SPARK_FM_JDBC_DRIVER", "org.mariadb.jdbc.Driver");
                jobContext.setParameters("SPARK_FM_JDBC_USERNAME", "root");
                jobContext.setParameters("SPARK_FM_JDBC_PASSWORD", "root");

                jobContext.setParameters("SPARK_PLATFORM_JDBC_URL",
                                "jdbc:mysql://localhost:3306/PLATFORM?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
                jobContext.setParameters("SPARK_PLATFORM_JDBC_DRIVER", "org.mariadb.jdbc.Driver");
                jobContext.setParameters("SPARK_PLATFORM_JDBC_USERNAME", "root");
                jobContext.setParameters("SPARK_PLATFORM_JDBC_PASSWORD", "root");

                jobContext.setParameters("SPARK_CASSANDRA_HOST", "localhost");
                jobContext.setParameters("SPARK_CASSANDRA_PORT", "9042");
                jobContext.setParameters("SPARK_CASSANDRA_DATACENTER", "datacenter1");
                jobContext.setParameters("SPARK_CASSANDRA_USERNAME", "cassandra");
                jobContext.setParameters("SPARK_CASSANDRA_PASSWORD", "cassandra");
                jobContext.setParameters("SPARK_CASSANDRA_KEYSPACE_PM", "pm");

                jobContext.setParameters("SPARK_MINIO_ENDPOINT_URL", "http://localhost:9000");
                jobContext.setParameters("SPARK_MINIO_ACCESS_KEY", "bootadmin");
                jobContext.setParameters("SPARK_MINIO_SECRET_KEY", "bootadmin");
                jobContext.setParameters("SPARK_MINIO_BUCKET_NAME_PM", "performance");

                jobContext.setParameters("BASE_TRINO_ORC_PATH", "s3a://performance/JOB/ORC/QUARTERLY/");
                jobContext.setParameters("BASE_TRINO_NE_PATH", "s3a://performance/NE_META/");

                return jobContext;

        }

        private static SparkConf getSparkConf() {

                return new SparkConf()
                                .setAppName("ReportMain")
                                .setMaster("local[*]")
                                .set("spark.driver.memory", "8g")
                                .set("spark.sql.shuffle.partitions", "200")
                                .set("spark.default.parallelism", "100")
                                .set("spark.local.dir", "/Users/ent-00356/Documents/spark-local-dir")
                                .set("spark.network.timeout", "600s")
                                .set("spark.sql.files.maxPartitionBytes", "134217728")

                                // S3A configuration
                                .set("spark.hadoop.fs.s3a.access.key", "bootadmin")
                                .set("spark.hadoop.fs.s3a.secret.key", "bootadmin")
                                .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                                .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                                                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                                .set("spark.sql.session.timeZone", "UTC");

        }
}
