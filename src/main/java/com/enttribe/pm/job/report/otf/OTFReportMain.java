package com.enttribe.pm.job.report.otf;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCReadCustom;
import com.enttribe.pm.job.report.common.UpdateReportDetails;

public class OTFReportMain {

        public static JobContext jobContext = null;

        private static Logger logger = LoggerFactory.getLogger(OTFReportMain.class);

        public static void main(String[] args) {

                long startTime = System.currentTimeMillis();
                logger.info("🚀 OTFReportMain Execution Started!");

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

                        String READ_OTF_REPORT_CONFIGURATION_QUERY = "SELECT REPORT_WIDGET_ID_PK, GENERATED_REPORT_ID, UPPER(REPORT_MEASURE) AS REPORT_MEASURE, UPPER(TRIM(REPORT_NAME)) AS REPORT_NAME, UPPER(DOMAIN) AS DOMAIN, UPPER(VENDOR) AS VENDOR, UPPER(TECHNOLOGY) AS TECHNOLOGY, UPPER(FREQUENCY) AS FREQUENCY, UPPER(NODE) AS NODE, CONFIGURATION FROM REPORT_WIDGET WHERE REPORT_MEASURE = 'OTF Report_ONDEMAND' AND DELETED = 0 AND GENERATED_TYPE = 'ON_DEMAND' AND REPORT_WIDGET_ID_PK = 15996";

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

                        logger.info("🚀 READ OTF REPORT CONFIGURATION Executed Successfully! ✅");

                        /*
                         * PARSE OTF REPORT CONFIGURATION
                         */

                        OTFExtractConfiguration otfExtractConfiguration = new OTFExtractConfiguration(dataFrame, 2,
                                        "OTF EXTRACT CONFIGURATION");
                        otfExtractConfiguration.dataFrame = dataFrame;
                        otfExtractConfiguration.executeAndGetResultDataframe(jobContext);

                        logger.info("🚀 OTF Extract Configuration Executed Successfully! ✅");

                        /*
                         * READ OTF MINIO FILES
                         */

                        OTFReadMinioFiles otfReadMinioFiles = new OTFReadMinioFiles(dataFrame, 3,
                                        "OTF READ MINIO FILES");
                        otfReadMinioFiles.dataFrame = dataFrame;
                        dataFrame = otfReadMinioFiles.executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 OTF Read Minio Files Executed Successfully! ✅");

                        /*
                         * RAW FILE COUNTER NODE AGGREGATION
                         */

                        String RAW_FILE_COUNTER_NODE_AGGR_QUERY = jobContext
                                        .getParameter("RAW_FILE_COUNTER_NODE_AGGR_QUERY");

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 6, "RAW FILE COUNTER NODE AGGREGATION",
                                        RAW_FILE_COUNTER_NODE_AGGR_QUERY, "rawFileNodeAggrData", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 RAW FILE COUNTER NODE AGGREGATION Executed Successfully! ✅");

                        /*
                         * QUERY FOR FINAL COUNTER DATA
                         */

                        String FILTER_LEVEL = jobContext.getParameter("AGGREGATION_LEVEL");
                        String FILTER_QUERY_FINAL = jobContext.getParameter("FILTER_QUERY_FINAL");

                        logger.info("📊 FILTER_LEVEL: {}", FILTER_LEVEL);
                        logger.info("📊 FILTER_QUERY_FINAL: {}", FILTER_QUERY_FINAL);

                        String QUERY_FOR_FINAL_COUNTER_DATA = "SELECT CONCAT(metaData['" + FILTER_LEVEL
                                        + "'], COALESCE('',''),'##',finalKey) AS finalKey, quarterKey, hourKey, dateKey, "
                                        + FILTER_QUERY_FINAL + ", metaData FROM rawFileNodeAggrData";

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 7, "QUERY FOR FINAL COUNTER DATA",
                                        QUERY_FOR_FINAL_COUNTER_DATA, "FinalCounterData", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 QUERY FOR FINAL COUNTER DATA Executed Successfully! ✅");

                        /*
                         * COUNTER NODE AGGREGATION QUERY
                         */

                        String COUNTER_NODE_AGGR_QUERY = jobContext.getParameter("COUNTER_NODE_AGGR_QUERY");

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 8, "COUNTER NODE AGGREGATION QUERY",
                                        COUNTER_NODE_AGGR_QUERY, "finalNodeAggrData", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 COUNTER NODE AGGREGATION QUERY Executed Successfully! ✅");

                        /*
                         * COUNTER TIME AGGREGATION QUERY
                         */

                        String COUNTER_TIME_AGGR_QUERY = jobContext.getParameter("COUNTER_TIME_AGGR_QUERY");

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 9, "COUNTER TIME AGGREGATION QUERY",
                                        COUNTER_TIME_AGGR_QUERY, "timeAggrData", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 COUNTER TIME AGGREGATION QUERY Executed Successfully! ✅");

                        /*
                         * COUNTER MAP QUERY
                         */

                        String COUNTER_MAP_QUERY = jobContext.getParameter("COUNTER_MAP_QUERY");

                        String EXECUTE_COUNTER_MAP_QUERY = "SELECT finalKey, " + COUNTER_MAP_QUERY
                                        + " , metaData FROM timeAggrData";

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 10, "COUNTER MAP QUERY",
                                        EXECUTE_COUNTER_MAP_QUERY, "finalAggrDataKPI", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 COUNTER MAP QUERY Executed Successfully! ✅");

                        /*
                         * KPI EVALUATOR
                         */

                        KPIEvaluator kpiEvaluator = new KPIEvaluator();
                        kpiEvaluator.jobContext = jobContext;
                        jobContext.sqlctx().udf().register(kpiEvaluator.getName(), kpiEvaluator,
                                        kpiEvaluator.getReturnType());

                        String frequency = jobContext.getParameter("FREQUENCY");

                        String KPI_EVALUATOR_QUERY = "SELECT formula_calculated_data.record_data.* FROM (SELECT KPIEvaluator(finalKey, rawcounters ,metaData, '"
                                        + frequency
                                        + "') AS record_data FROM finalAggrDataKPI WHERE finalKey IS NOT NULL AND finalKey != '-' AND finalKey != \"\") AS formula_calculated_data";

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 11, "KPI EVALUATOR",
                                        KPI_EVALUATOR_QUERY,
                                        "kpiEvaluatedData", null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5, false);
                        logger.info("🚀 KPI EVALUATOR Executed Successfully! ✅");

                        /*
                         * OTF DF GENERATOR
                         */

                        OTFDFGenerator otfDFGenerator = new OTFDFGenerator(dataFrame, 12, "OTF DF GENERATOR");
                        otfDFGenerator.dataFrame = dataFrame;
                        dataFrame = otfDFGenerator.executeAndGetResultDataframe(jobContext);

                        logger.info("🚀 OTF DF GENERATOR Executed Successfully! ✅");

                        /*
                         * OTF REPORT WRITER
                         */

                        OTFReportWriter otfReportWriter = new OTFReportWriter(dataFrame, 13, "OTF REPORT WRITER");
                        otfReportWriter.dataFrame = dataFrame;
                        otfReportWriter.executeAndGetResultDataframe(jobContext);

                        logger.info("🚀 OTF REPORT WRITER Executed Successfully! ✅");

                        /*
                         * UPDATE REPORT DETAILS
                         */

                        UpdateReportDetails updateReportDetails = new UpdateReportDetails(dataFrame, 14,
                                        "UPDATE REPORT DETAILS");
                        updateReportDetails.dataFrame = dataFrame;
                        updateReportDetails.executeAndGetResultDataframe(jobContext);

                        logger.info("🚀 UPDATE REPORT DETAILS Executed Successfully! ✅");

                } catch (Exception e) {
                        logger.error("Exception Occurred in OTFReportMain: {}", e.getMessage());
                        e.printStackTrace();
                }

                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                logger.info("🚀 OTFReportMain Execution Completed in {} Minutes {} Seconds", executionTime / 60000,
                                (executionTime % 60000) / 1000);
        }

        private static JobContext setParametersToJobContext(JobContext jobContext) {

                jobContext.setParameters("SPARK_PM_JDBC_URL",
                                "jdbc:mysql://localhost:3306/RLTL_OCT?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
                jobContext.setParameters("SPARK_PM_JDBC_DRIVER", "org.mariadb.jdbc.Driver");
                jobContext.setParameters("SPARK_PM_JDBC_USERNAME", "root");
                jobContext.setParameters("SPARK_PM_JDBC_PASSWORD", "root");

                jobContext.setParameters("SPARK_FM_JDBC_URL",
                                "jdbc:mysql://localhost:3306/FMS?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
                jobContext.setParameters("SPARK_FM_JDBC_DRIVER", "org.mariadb.jdbc.Driver");
                jobContext.setParameters("SPARK_FM_JDBC_USERNAME", "root");
                jobContext.setParameters("SPARK_FM_JDBC_PASSWORD", "root");

                jobContext.setParameters("SPARK_PLATFORM_JDBC_URL",
                                "jdbc:mysql://localhost:3308/PLATFORM?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
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

                jobContext.setParameters("BASE_TRINO_ORC_PATH", "s3a://performance/JOB/ORC/");
                jobContext.setParameters("BASE_TRINO_NE_PATH", "s3a://performance/NE_META/");

                return jobContext;

        }

        private static SparkConf getSparkConf() {

                return new SparkConf()
                                .setAppName("OTFReportMain")
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
