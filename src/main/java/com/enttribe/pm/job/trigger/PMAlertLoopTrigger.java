package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.SparkSession;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.custom.processor.JDBCReadCustom;
import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.pm.job.alert.AlertUDF;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PMAlertLoopTrigger {

    public static JobContext jobContext = null;

    private static Logger logger = LoggerFactory.getLogger(PMAlertLoopTrigger.class);

    public static void main(String[] args) {
        SparkConf conf = getSparkConf();

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        jobContext = new JobContextImpl(session);
        jobContext = setParametersToJobContext(jobContext);

        Dataset<Row> dataFrame = session.emptyDataFrame();

        try {
            String DOMAIN = jobContext.getParameter("DOMAIN");
            String VENDOR = jobContext.getParameter("VENDOR");
            String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");
            String FREQUENCY = jobContext.getParameter("FREQUENCY");

            /*
             * READ PM ALERT CONFIGURATION
             */

            String READ_PM_ALERT_CONFIGURATION_QUERY = "SELECT PERFORMANCE_ALERT_ID_PK, ALERTID, NAME, EXPRESSION, DOMAIN, VENDOR, TECHNOLOGY, "
                    +
                    "DESCRIPTION, ALERTTYPE, CONFIGURATION " +
                    "FROM PERFORMANCE_ALERT " +
                    "WHERE DOMAIN = '$DOMAIN' " +
                    "AND VENDOR = '$VENDOR' " +
                    "AND TECHNOLOGY = '$TECHNOLOGY' " +
                    "AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.frequency[0]'))) = '$FREQUENCY' "
                    +
                    "AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.mo[0]'))) LIKE '%AGGREGATED%' "
                    +
                    "AND UPPER(JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(CONFIGURATION,'\"',''),\"'\",'\"'),'$.geography_l1[0]'))) <> 'CUSTOM' "
                    +
                    "AND DELETED = 0";

            READ_PM_ALERT_CONFIGURATION_QUERY = READ_PM_ALERT_CONFIGURATION_QUERY.replace("$DOMAIN", DOMAIN);
            READ_PM_ALERT_CONFIGURATION_QUERY = READ_PM_ALERT_CONFIGURATION_QUERY.replace("$VENDOR", VENDOR);
            READ_PM_ALERT_CONFIGURATION_QUERY = READ_PM_ALERT_CONFIGURATION_QUERY.replace("$TECHNOLOGY", TECHNOLOGY);
            READ_PM_ALERT_CONFIGURATION_QUERY = READ_PM_ALERT_CONFIGURATION_QUERY.replace("$FREQUENCY", FREQUENCY);

            logger.info("READ_PM_ALERT_CONFIGURATION_QUERY: {}", READ_PM_ALERT_CONFIGURATION_QUERY);

            String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
            String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
            String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
            String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

            dataFrame = new JDBCReadCustom(dataFrame,
                    1,
                    "READ PM ALERT CONFIGURATION",
                    SPARK_PM_JDBC_DRIVER,
                    SPARK_PM_JDBC_URL,
                    SPARK_PM_JDBC_USERNAME,
                    SPARK_PM_JDBC_PASSWORD,
                    READ_PM_ALERT_CONFIGURATION_QUERY,
                    "PERFORMANCE_ALERT",
                    null,
                    null,
                    null,
                    null,
                    null)
                    .executeAndGetResultDataframe(jobContext);

            dataFrame.show(5);
            logger.info("🚀 READ PM ALERT CONFIGURATION Executed Successfully! ✅");

            /*
             * READ ALARM LIBRARY
             */

            String READ_ALARM_LIBRARY_QUERY = "SELECT AL.ALARM_IDENTIFIER, AL.CLASSIFICATION, AL.NETYPE, AL.DEFAULT_SEVERITY, AL.EMS_TYPE, AL.ALARM_ID, AL.SERVICE_AFFECTING, AL.PROBABLE_CAUSE, AL.MANUAL_CLEARED, AL.EVENT_TYPE, AL.CORRELATION_ENABLE, AL.ALARM_GROUP FROM ALARM_LIBRARY AL WHERE AL.DELETED = 0 AND AL.ENABLED = 1 AND AL.DOMAIN = '$DOMAIN' AND AL.VENDOR = '$VENDOR' AND AL.TECHNOLOGY = '$TECHNOLOGY' AND AL.EMS_TYPE = 'PERFORMANCE_ALERT'";

            String SPARK_FM_JDBC_URL = jobContext.getParameter("SPARK_FM_JDBC_URL");
            String SPARK_FM_JDBC_DRIVER = jobContext.getParameter("SPARK_FM_JDBC_DRIVER");
            String SPARK_FM_JDBC_USERNAME = jobContext.getParameter("SPARK_FM_JDBC_USERNAME");
            String SPARK_FM_JDBC_PASSWORD = jobContext.getParameter("SPARK_FM_JDBC_PASSWORD");

            dataFrame = new JDBCReadCustom(dataFrame, 2, "READ ALARM LIBRARY", SPARK_FM_JDBC_DRIVER,
                    SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME, SPARK_FM_JDBC_PASSWORD,
                    READ_ALARM_LIBRARY_QUERY, "ALARM_LIBRARY", null, null, null, null, null)
                    .executeAndGetResultDataframe(jobContext);

            dataFrame.show(5);
            logger.info("🚀 READ ALARM LIBRARY Executed Successfully! ✅");

            String JOIN_QUERY = "SELECT PA.EXPRESSION, PA.DESCRIPTION, PA.NAME, AL.CLASSIFICATION, AL.NETYPE, AL.DEFAULT_SEVERITY, AL.EMS_TYPE, AL.ALARM_ID, AL.SERVICE_AFFECTING, AL.PROBABLE_CAUSE, AL.MANUAL_CLEARED, AL.EVENT_TYPE, AL.CORRELATION_ENABLE, AL.ALARM_GROUP, PA.CONFIGURATION FROM PERFORMANCE_ALERT PA JOIN ALARM_LIBRARY AL ON AL.ALARM_IDENTIFIER = PA.ALERTID";
            dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                    3,
                    "JOIN RULE AND LIBRARY",
                    JOIN_QUERY,
                    "JOIN_RESULT",
                    null)
                    .executeAndGetResultDataframe(jobContext);

            dataFrame.show(5);
            logger.info("🚀 JOIN RULE AND LIBRARY Executed Successfully! ✅");

            // List<Row> alarmLibraryList = dataFrame.collectAsList();

            // Row performanceAlert = null;

            // for (Row alarmLibrary : alarmLibraryList) {

                // String expression = performanceAlert.getAs("EXPRESSION") != null
                //         ? performanceAlert.getAs("EXPRESSION").toString()
                //         : "";
                // String description = performanceAlert.getAs("DESCRIPTION") != null
                //         ? performanceAlert.getAs("DESCRIPTION").toString()
                //         : "";
                // String alarmName = performanceAlert.getAs("NAME") != null
                //         ? performanceAlert.getAs("NAME").toString()
                //         : "";
                // String classification = alarmLibrary.getAs("CLASSIFICATION") != null
                //         ? alarmLibrary.getAs("CLASSIFICATION").toString().trim()
                //         : "";
                // String netype = alarmLibrary.getAs("NETYPE") != null
                //         ? alarmLibrary.getAs("NETYPE").toString()
                //         : "";
                // String defaultSeverity = alarmLibrary.getAs("DEFAULT_SEVERITY") != null
                //         ? alarmLibrary.getAs("DEFAULT_SEVERITY").toString()
                //         : "";
                // String emsType = alarmLibrary.getAs("EMS_TYPE") != null
                //         ? alarmLibrary.getAs("EMS_TYPE").toString()
                //         : "";
                // String alarmId = alarmLibrary.getAs("ALARM_ID") != null
                //         ? alarmLibrary.getAs("ALARM_ID").toString()
                //         : "";
                // String serviceAffecting = alarmLibrary.getAs("SERVICE_AFFECTING") != null
                //         ? alarmLibrary.getAs("SERVICE_AFFECTING").toString()
                //         : "";
                // String probableCause = alarmLibrary.getAs("PROBABLE_CAUSE") != null
                //         ? alarmLibrary.getAs("PROBABLE_CAUSE").toString()
                //         : "";
                // String manualCleared = alarmLibrary.getAs("MANUAL_CLEARED") != null
                //         ? alarmLibrary.getAs("MANUAL_CLEARED").toString()
                //         : "";
                // String eventType = alarmLibrary.getAs("EVENT_TYPE") != null
                //         ? alarmLibrary.getAs("EVENT_TYPE").toString()
                //         : "";
                // String correlationEnable = alarmLibrary.getAs("CORRELATION_ENABLE") != null
                //         ? alarmLibrary.getAs("CORRELATION_ENABLE").toString()
                //         : "";
                // String alarmGroup = alarmLibrary.getAs("ALARM_GROUP") != null
                //         ? alarmLibrary.getAs("ALARM_GROUP").toString()
                //         : "";
                // String configuration = performanceAlert.getAs("CONFIGURATION") != null
                //         ? performanceAlert.getAs("CONFIGURATION").toString().replace("'", "''")
                //         : "";

                // Test
                // String alarmIdentifier = "";
                // String ALERT_UDF_QUERY = "SELECT AlertUDF('" + expression + "', '" + description + "', '"
                //         + alarmIdentifier + "', '" + alarmName + "', '" + classification + "', '" + netype + "', '"
                //         + defaultSeverity + "', '" + emsType + "', '" + alarmId + "', '" + serviceAffecting + "', '"
                //         + probableCause + "', '" + manualCleared + "', '" + eventType + "', '" + DOMAIN + "', '"
                //         + VENDOR + "', '" + TECHNOLOGY + "', '" + correlationEnable + "', '" + alarmGroup + "', '"
                //         + configuration + "') AS UDF_RESULT";

                String ALERT_UDF_QUERY = "";

                AlertUDF alertUDF = new AlertUDF();
                alertUDF.jobcontext = jobContext;
                jobContext.sqlctx().udf().register(alertUDF.getName(), alertUDF, alertUDF.getReturnType());

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                        3,
                        "ALERT UDF",
                        ALERT_UDF_QUERY,
                        "UDF_RESULT_TABLE",
                        null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 ALERT UDF Executed Successfully! ✅");

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                        4,
                        "EXPLODE UDF RESULT",
                        "SELECT EXPLODE(UDF_RESULT) AS CONFIGURATION FROM UDF_RESULT_TABLE",
                        "EXPLODE_UDF_RESULT",
                        null).executeAndGetResultDataframe(jobContext);

                dataFrame.show(5);
                logger.info("🚀 EXPLODE UDF RESULT Executed Successfully! ✅");
            // }
            // }

        } catch (Exception e) {
            logger.error("Exception Occurred in PM Alert Trigger: {}", e.getMessage());
            e.printStackTrace();
        }

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
        jobContext.setParameters("FREQUENCY", "FIVEMIN");
        jobContext.setParameters("TIMESTAMP", "2025-07-20 00:00:00.000000+0000");

        return jobContext;

    }

    private static SparkConf getSparkConf() {

        return new SparkConf()
                .setAppName("PMAlertLoopTrigger")
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
