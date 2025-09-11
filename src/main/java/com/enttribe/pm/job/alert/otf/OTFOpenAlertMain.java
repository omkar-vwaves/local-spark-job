package com.enttribe.pm.job.alert.otf;

import com.enttribe.custom.processor.CacheDatasetCustom;
import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCReadCustom;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OTFOpenAlertMain {
        public static JobContext jobContext = null;
        private static Logger logger = LoggerFactory.getLogger(OTFOpenAlertMain.class);

        public static void main(String[] args) {
                SparkConf conf = getSparkConf();
                SparkSession session = SparkSession.builder().config(conf).getOrCreate();
                jobContext = new JobContextImpl(session);
                jobContext = setParametersToJobContext(jobContext);
                Dataset<Row> dataFrame = session.emptyDataFrame();

                try {
                        String domain = jobContext.getParameter("DOMAIN");
                        String vendor = jobContext.getParameter("VENDOR");
                        String technology = jobContext.getParameter("TECHNOLOGY");
                        String frequency = jobContext.getParameter("FREQUENCY");

                        String READ_PM_ALERT_CONFIGURATION_QUERY = "SELECT PERFORMANCE_ALERT_ID_PK, CONFIGURATION, EXPRESSION, DOMAIN, VENDOR, NAME, DESCRIPTION, ALERTTYPE, ALERTID,TECHNOLOGY FROM PERFORMANCE_ALERT WHERE DOMAIN = '"
                                        + domain + "' AND VENDOR = '" + vendor + "' AND TECHNOLOGY = '" + technology
                                        + "' AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.frequency[0]'))) = '"
                                        + frequency + "' AND DELETED = 0 AND PERFORMANCE_ALERT_ID_PK IN (1749)";

                        logger.info("READ PM ALERT CONFIGURATION QUERY: {}", READ_PM_ALERT_CONFIGURATION_QUERY);

                        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

                        dataFrame = (new JDBCReadCustom(dataFrame, 1, "READ PM ALERT CONFIGURATION",
                                        SPARK_PM_JDBC_DRIVER,
                                        SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD,
                                        READ_PM_ALERT_CONFIGURATION_QUERY, "PERFORMANCE_ALERT", (String) null,
                                        (String) null, (String) null,
                                        (String) null, (String) null)).executeAndGetResultDataframe(jobContext);

                        dataFrame.show(10, false);
                        logger.info("READ PM ALERT CONFIGURATION Executed Successfully!");

                        String READ_ACTIVE_ALARM_LIBRARY_QUERY = "SELECT * FROM ALARM_LIBRARY WHERE DELETED = 0 AND ENABLED = 1 AND EMS_TYPE = 'PERFORMANCE_ALERT' AND DOMAIN = '"
                                        + domain + "' AND VENDOR = '" + vendor + "' AND TECHNOLOGY = '" + technology
                                        + "'";

                        logger.info("READ ACTIVE ALARM LIBRARY QUERY: {}", READ_ACTIVE_ALARM_LIBRARY_QUERY);

                        String SPARK_FM_JDBC_URL = jobContext.getParameter("SPARK_FM_JDBC_URL");
                        String SPARK_FM_JDBC_DRIVER = jobContext.getParameter("SPARK_FM_JDBC_DRIVER");
                        String SPARK_FM_JDBC_USERNAME = jobContext.getParameter("SPARK_FM_JDBC_USERNAME");
                        String SPARK_FM_JDBC_PASSWORD = jobContext.getParameter("SPARK_FM_JDBC_PASSWORD");


                        dataFrame = (new JDBCReadCustom(dataFrame, 2, "READ ACTIVE ALARM LIBRARY", SPARK_FM_JDBC_DRIVER,
                                        SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME, SPARK_FM_JDBC_PASSWORD,
                                        READ_ACTIVE_ALARM_LIBRARY_QUERY,
                                        "ALARM_LIBRARY", (String) null, (String) null, (String) null, (String) null,
                                        (String) null))
                                        .executeAndGetResultDataframe(jobContext);


                        dataFrame.show(10, false);
                        logger.info("READ ACTIVE ALARM LIBRARY Executed Successfully!");

                        String JOIN_QUERY = "SELECT pa.PERFORMANCE_ALERT_ID_PK, pa.CONFIGURATION, pa.EXPRESSION, pa.DOMAIN, pa.VENDOR, pa.ALERTID, pa.NAME, pa.DESCRIPTION, pa.TECHNOLOGY, al.ALARM_IDENTIFIER, al.ALARM_NAME, al.CLASSIFICATION, al.NETYPE, al.DEFAULT_SEVERITY, al.EMS_TYPE, al.EVENT_TYPE, al.ALARM_ID, al.SERVICE_AFFECTING, al.CORRELATION_ENABLE, al.MANUAL_CLEARED, al.PROBABLE_CAUSE, al.PRIORITY, al.ALARM_LAYER, al.ALARM_GROUP, al.EQUIPMENT_TYPE, al.IS_SOUTH_BOUND_INTEGRATION FROM PERFORMANCE_ALERT pa JOIN ALARM_LIBRARY al ON pa.ALERTID = al.ALARM_IDENTIFIER";


                        logger.info("JOIN QUERY: {}", JOIN_QUERY);
                        dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 3,
                                        "JOIN PERFORMANCE ALERT AND ALARM LIBRARY",
                                        JOIN_QUERY, "JOIN_RESULT", (String) null))
                                        .executeAndGetResultDataframe(jobContext);

                        dataFrame.show(5);
                        logger.info("JOIN PERFORMANCE ALERT AND ALARM LIBRARY Executed Successfully!");

                        dataFrame = (new OTFAlertCloseExtract(dataFrame, 4, "EXTRACT CONFIGURATION"))
                                        .executeAndGetResultDataframe(jobContext);
                        dataFrame.show(5);
                        logger.info("EXTRACT CONFIGURATION Executed Successfully!");

                        String START_INDEX = jobContext.getParameter("START_INDEX");
                        String END_INDEX = jobContext.getParameter("END_INDEX");
                        int startIndex = Integer.parseInt(START_INDEX);
                        int endIndex = Integer.parseInt(END_INDEX);

                        for (int i = startIndex; i < endIndex; ++i) {


                                dataFrame = (new OTFAlertReadMinioFiles(dataFrame, 5, "READ MINIO FILES"))
                                                .executeAndGetResultDataframe(jobContext);

                                dataFrame.show(5, false);
                                logger.info("READ MINIO FILES Executed Successfully!");


                                String RAW_FILE_COUNTER_NODE_AGGR_QUERY = jobContext
                                                .getParameter("RAW_FILE_COUNTER_NODE_AGGR_QUERY");
                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 6,
                                                "RAW FILE COUNTER NODE AGGREGATION",
                                                RAW_FILE_COUNTER_NODE_AGGR_QUERY, "rawFileNodeAggrData", (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5);
                                logger.info("RAW FILE COUNTER NODE AGGREGATION Executed Successfully!");


                                String FILTER_LEVEL = jobContext.getParameter("FILTER_LEVEL" + i);
                                String FILTER_QUERY_FINAL = jobContext.getParameter("FILTER_QUERY_FINAL");
                                String QUERY_FOR_FINAL_COUNTER_DATA = "SELECT CONCAT(metaData['" + FILTER_LEVEL
                                                + "'], COALESCE('',''),'##',finalKey) AS finalKey, "
                                                + FILTER_QUERY_FINAL
                                                + ", metaData FROM rawFileNodeAggrData";
                                logger.info("QUERY FOR FINAL COUNTER DATA: {}", QUERY_FOR_FINAL_COUNTER_DATA);
                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 7, "QUERY FOR FINAL COUNTER DATA",
                                                QUERY_FOR_FINAL_COUNTER_DATA, "FinalCounterData", (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5, false);
                                logger.info("QUERY FOR FINAL COUNTER DATA Executed Successfully!");

                                String COUNTER_NODE_AGGR_QUERY = jobContext.getParameter("COUNTER_NODE_AGGR_QUERY");
                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 8, "COUNTER NODE AGGREGATION QUERY",
                                                COUNTER_NODE_AGGR_QUERY, "finalNodeAggrData", (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5, false);
                                logger.info("COUNTER NODE AGGREGATION QUERY Executed Successfully!");

                                String COUNTER_TIME_AGGR_QUERY = jobContext.getParameter("COUNTER_TIME_AGGR_QUERY");
                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 9, "COUNTER TIME AGGREGATION QUERY",
                                                COUNTER_TIME_AGGR_QUERY, "timeAggrData", (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5);
                                logger.info("COUNTER TIME AGGREGATION QUERY Executed Successfully!");

                                String COUNTER_MAP_QUERY = jobContext.getParameter("COUNTER_MAP_QUERY");
                                String EXECUTE_COUNTER_MAP_QUERY = "SELECT finalKey, " + COUNTER_MAP_QUERY
                                                + " , metaData FROM timeAggrData";
                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 10, "COUNTER MAP QUERY",
                                                EXECUTE_COUNTER_MAP_QUERY,
                                                "finalAggrDataKPI", (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5, false);
                                logger.info("COUNTER MAP QUERY Executed Successfully!");

                                OTFAlertKPIEvaluator kpiEvaluator = new OTFAlertKPIEvaluator();
                                kpiEvaluator.jobContext = jobContext;
                                jobContext.sqlctx().udf().register(kpiEvaluator.getName(), kpiEvaluator,
                                                kpiEvaluator.getReturnType());
                                String KPI_EVALUATOR_QUERY = "SELECT formula_calculated_data.record_data.* FROM (SELECT OTFAlertKPIEvaluator(finalKey, rawcounters ,metaData, '$FREQUENCY') AS record_data FROM finalAggrDataKPI WHERE finalKey IS NOT NULL AND finalKey != '-' AND finalKey != \"\") AS formula_calculated_data";
                                KPI_EVALUATOR_QUERY = KPI_EVALUATOR_QUERY.replace("$FREQUENCY", frequency);
                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 11, "KPI EVALUATOR",
                                                KPI_EVALUATOR_QUERY,
                                                "kpiEvaluatedData", (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                logger.info("KPI EVALUATOR Executed Successfully!");

                                dataFrame = (new CacheDatasetCustom(dataFrame, 12, "CACHE COMPUTED KPI COUNTER RESULT",
                                                "memoryAndDisk",
                                                "cacheKpiEvaluatedData")).executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5);
                                logger.info("CACHE COMPUTED KPI COUNTER RESULT Executed Successfully!");

                                dataFrame = (new ExecuteSparkSQLD1Custom(dataFrame, 13, "GET ONLY BREACHED DATA",
                                                "SELECT * FROM cacheKpiEvaluatedData", "cacheKpiEvaluatedDataTemp",
                                                (String) null))
                                                .executeAndGetResultDataframe(jobContext);
                                OTFAlertBreachedData getOnlyBreachedData = new OTFAlertBreachedData();
                                getOnlyBreachedData.dataFrame = dataFrame;
                                dataFrame = getOnlyBreachedData.executeAndGetResultDataframe(jobContext);
                                dataFrame.show(5, false);
                                logger.info("Get Only Breached Data Executed Successfully!");

                                dataFrame = (new OTFGenerateOpenAlert(dataFrame, 14,
                                                "PROCESS BREACHED DATA FOR EACH CONFIGURATION"))
                                                .executeAndGetResultDataframe(jobContext);
                                logger.info("PROCESS BREACHED DATA FOR EACH CONFIGURATION Executed Successfully!");
                                String updateIndex = String.valueOf(i + 1);
                                jobContext.setParameters("START_INDEX", updateIndex);
                        }
                } catch (Exception var42) {
                        logger.error("Exception Occurred in PM Alert Trigger: {}", var42.getMessage());
                        var42.printStackTrace();
                } finally {
                        if (session != null) {
                                session.close();
                        }
                }
                System.exit(0);
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
                jobContext.setParameters("DOMAIN", "TRANSPORT");
                jobContext.setParameters("VENDOR", "JUNIPER");
                jobContext.setParameters("TECHNOLOGY", "COMMON");
                jobContext.setParameters("EMS_TYPE", "NA");
                jobContext.setParameters("NE_TYPE", "'INTERFACE','ROUTER'");
                jobContext.setParameters("FREQUENCY", "15 MIN");
                jobContext.setParameters("BASE_TRINO_ORC_PATH_DATE", "250825");
                jobContext.setParameters("BASE_TRINO_ORC_PATH_TIME", "0745");
                jobContext.setParameters("BASE_TRINO_ORC_PATH", "s3a://performance/JOB/ORC/");
                jobContext.setParameters("BASE_TRINO_NE_PATH_DATE", "20250825");
                // jobContext.setParameters("BASE_TRINO_NE_PATH_TIME", "0745");
                jobContext.setParameters("BASE_TRINO_NE_PATH", "s3a://performance/NE_META/");
                jobContext.setParameters("TIMESTAMP", "2025-08-25 07:45:00.000000+0000");
                jobContext.setParameters("SPARK_KAFKA_BROKER_ANSIBLE", "localhost:9092");
                jobContext.setParameters("KAFKA_TOPIC_NAME", "pm.alerts.fault");
                return jobContext;
        }

        private static SparkConf getSparkConf() {
                return (new SparkConf()).setAppName("OTFOpenAlertMain").setMaster("local[*]")
                                .set("spark.driver.memory", "8g").set("spark.sql.shuffle.partitions", "200")
                                .set("spark.default.parallelism", "100")
                                .set("spark.local.dir", "/Users/ent-00356/Documents/spark-local-dir")
                                .set("spark.network.timeout", "600s")
                                .set("spark.sql.files.maxPartitionBytes", "134217728")
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
