package com.enttribe.pm.job.alert.loop;

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

public class FinalPMAlertOpenMain {

        public static JobContext jobContext = null;

        private static Logger logger = LoggerFactory.getLogger(FinalPMAlertOpenMain.class);

        public static void main(String[] args) {

                long startTime = System.currentTimeMillis();
                logger.info("🚀 FinalPMAlertOpenMain Execution Started!");

                SparkConf conf = getSparkConf();

                SparkSession session = SparkSession.builder()
                                .config(conf)
                                .getOrCreate();

                jobContext = new JobContextImpl(session);
                jobContext = setParametersToJobContext(jobContext);

                Dataset<Row> dataFrame = session.emptyDataFrame();

                try {

                        /*
                         * REGISTER UDF
                         */

                        /*
                         * READ OPEN ALERT RULES
                         */

                        String DOMAIN = jobContext.getParameter("DOMAIN");
                        String VENDOR = jobContext.getParameter("VENDOR");
                        String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");
                        String FREQUENCY = jobContext.getParameter("FREQUENCY");

                        logger.info("JOB CONTEXT PARAMETERS ====> DOMAIN: {}, VENDOR: {}, TECHNOLOGY: {}, FREQUENCY: {}",
                                        DOMAIN,
                                        VENDOR, TECHNOLOGY, FREQUENCY);

                        String READ_OPEN_ALERT_RULES_QUERY = "SELECT ALERTID, EXPRESSION, DESCRIPTION, NAME AS ALARM_NAME, DOMAIN, VENDOR, TECHNOLOGY, CONFIGURATION FROM PERFORMANCE_ALERT WHERE DOMAIN = '$DOMAIN' AND VENDOR = '$VENDOR' AND TECHNOLOGY = '$TECHNOLOGY' AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.frequency[0]'))) = '$FREQUENCY' AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.mo[0]'))) LIKE '%AGGREGATED%' AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.geography_l1[0]'))) <> 'CUSTOM' AND DELETED = 0";

                        READ_OPEN_ALERT_RULES_QUERY = READ_OPEN_ALERT_RULES_QUERY.replace("$DOMAIN", DOMAIN);
                        READ_OPEN_ALERT_RULES_QUERY = READ_OPEN_ALERT_RULES_QUERY.replace("$VENDOR", VENDOR);
                        READ_OPEN_ALERT_RULES_QUERY = READ_OPEN_ALERT_RULES_QUERY.replace("$TECHNOLOGY", TECHNOLOGY);
                        READ_OPEN_ALERT_RULES_QUERY = READ_OPEN_ALERT_RULES_QUERY.replace("$FREQUENCY", FREQUENCY);

                        logger.info("READ_OPEN_ALERT_RULES_QUERY ====> {}", READ_OPEN_ALERT_RULES_QUERY);

                        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

                        dataFrame = new JDBCReadCustom(dataFrame, 1, "READ OPEN ALERT RULES",
                                        SPARK_PM_JDBC_DRIVER, SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                                        SPARK_PM_JDBC_PASSWORD,
                                        READ_OPEN_ALERT_RULES_QUERY, "PERFORMANCE_ALERT", null, null, null, null,
                                        null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show();
                        logger.info("++++++++++++[READ OPEN ALERT RULES]++++++++++++");

                        /*
                         * READ ACTIVE ALARM LIBRARY
                         */

                        String READ_ACTIVE_ALARM_LIBRARY_QUERY = "SELECT ALARM_IDENTIFIER, TRIM(CLASSIFICATION) AS CLASSIFICATION, NETYPE, DEFAULT_SEVERITY, EMS_TYPE, ALARM_ID, SERVICE_AFFECTING, PROBABLE_CAUSE, MANUAL_CLEARED, EVENT_TYPE, CORRELATION_ENABLE, ALARM_GROUP FROM ALARM_LIBRARY WHERE DELETED = 0 AND ENABLED = 1 AND DOMAIN = '$DOMAIN' AND VENDOR = '$VENDOR' AND TECHNOLOGY = '$TECHNOLOGY' AND EMS_TYPE = 'PERFORMANCE_ALERT'";

                        READ_ACTIVE_ALARM_LIBRARY_QUERY = READ_ACTIVE_ALARM_LIBRARY_QUERY.replace("$DOMAIN", DOMAIN);
                        READ_ACTIVE_ALARM_LIBRARY_QUERY = READ_ACTIVE_ALARM_LIBRARY_QUERY.replace("$VENDOR", VENDOR);
                        READ_ACTIVE_ALARM_LIBRARY_QUERY = READ_ACTIVE_ALARM_LIBRARY_QUERY.replace("$TECHNOLOGY",
                                        TECHNOLOGY);

                        String SPARK_FM_JDBC_URL = jobContext.getParameter("SPARK_FM_JDBC_URL");
                        String SPARK_FM_JDBC_DRIVER = jobContext.getParameter("SPARK_FM_JDBC_DRIVER");
                        String SPARK_FM_JDBC_USERNAME = jobContext.getParameter("SPARK_FM_JDBC_USERNAME");
                        String SPARK_FM_JDBC_PASSWORD = jobContext.getParameter("SPARK_FM_JDBC_PASSWORD");

                        dataFrame = new JDBCReadCustom(dataFrame, 1, "READ ACTIVE ALARM LIBRARY",

                                        SPARK_FM_JDBC_DRIVER, SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME,
                                        SPARK_FM_JDBC_PASSWORD,
                                        READ_ACTIVE_ALARM_LIBRARY_QUERY, "ALARM_LIBRARY", null, null, null, null,
                                        null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show();
                        logger.info("++++++++++++[READ ACTIVE ALARM LIBRARY]++++++++++++");

                        /*
                         * JOIN RULE AND LIBRARY
                         */

                        String JOIN_QUERY = "SELECT pa.EXPRESSION, pa.DESCRIPTION, al.ALARM_IDENTIFIER, pa.ALARM_NAME, al.CLASSIFICATION, al.NETYPE, al.DEFAULT_SEVERITY, al.EMS_TYPE, al.ALARM_ID, al.SERVICE_AFFECTING, al.PROBABLE_CAUSE, al.MANUAL_CLEARED, al.EVENT_TYPE, pa.DOMAIN, pa.VENDOR, pa.TECHNOLOGY, al.CORRELATION_ENABLE, al.ALARM_GROUP, pa.CONFIGURATION FROM PERFORMANCE_ALERT pa INNER JOIN ALARM_LIBRARY al ON pa.ALERTID = al.ALARM_IDENTIFIER";

                        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 3, "JOIN RULE AND LIBRARY",
                                        JOIN_QUERY,
                                        "JOIN_RESULT",
                                        null)
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.createOrReplaceTempView("JOIN_RESULT");

                        OpenAlertExtractConf openAlertExtractConf = new OpenAlertExtractConf();
                        openAlertExtractConf.dataFrame = dataFrame;
                        dataFrame = openAlertExtractConf.executeAndGetResultDataframe(jobContext);

                        dataFrame = new CacheDatasetCustom(dataFrame, 4, "CACHE CONFIGURATION",
                                        "memoryAndDisk", "CacheExtractedRule")
                                        .executeAndGetResultDataframe(jobContext);

                        String CURRENT_COUNT = jobContext.getParameter("CURRENT_COUNT");
                        String FINAL_COUNT = jobContext.getParameter("FINAL_COUNT");

                        int currentCount = Integer.parseInt(CURRENT_COUNT);
                        int finalCount = Integer.parseInt(FINAL_COUNT);

                        logger.info("Starting Loop From currentCount={} To finalCount={}", currentCount, finalCount);

                        for (int i = currentCount; i < finalCount; i++) {
                                logger.info("Iteration Started: currentCount={}", currentCount);

                                /*
                                 * ALERT CQL READ
                                 */

                                OpenAlertCQLRead openAlertCQLRead = new OpenAlertCQLRead();
                                openAlertCQLRead.dataFrame = dataFrame;
                                dataFrame = openAlertCQLRead.executeAndGetResultDataframe(jobContext);

                                ProcessOpenCQLResult processOpenCQLResult = new ProcessOpenCQLResult();
                                processOpenCQLResult.dataFrame = dataFrame;
                                processOpenCQLResult.executeAndGetResultDataframe(jobContext);
                                currentCount++;

                                jobContext.setParameters("CURRENT_COUNT", String.valueOf(currentCount));
                                logger.info("Iteration Ended: currentCount={}", currentCount);
                        }
                        logger.info("Loop Finished. Final currentCount={}", currentCount);

                } catch (Exception e) {
                        logger.error("Exception Occurred in FinalPMAlertOpenMain: {}", e.getMessage());
                        e.printStackTrace();
                }

                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                logger.info("🚀 FinalPMAlertOpenMain Execution Completed in {} Minutes {} Seconds",
                                executionTime / 60000,
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

                // jobContext.setParameters("BASE_TRINO_ORC_PATH",
                // "s3a://performance/JOB/ORC/QUARTERLY/");
                // jobContext.setParameters("BASE_TRINO_NE_PATH", "s3a://performance/NE_META/");

                // jobContext.setParameters("ROW_KEY_APPENDER", "COMON_Router");
                // jobContext.setParameters("JOB_FREQUENCY", "5 MIN");
                // jobContext.setParameters("TIMESTAMP", "2025-07-20 00:00:00.000000+0000");

                // Final
                jobContext.setParameters("DOMAIN", "TRANSPORT");
                jobContext.setParameters("VENDOR", "CISCO");
                jobContext.setParameters("TECHNOLOGY", "COMMON");
                jobContext.setParameters("FREQUENCY", "FIVEMIN");
                jobContext.setParameters("TIMESTAMP", "2025-07-20 00:05:00.000000+0000");

                return jobContext;

        }

        private static SparkConf getSparkConf() {

                return new SparkConf()
                                .setAppName("FinalPMAlertOpenMain")
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
