package com.enttribe.pm.job.alert;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.JDBCReadCustom;

public class OpenAlertMain {

    public static JobContext jobContext = null;

    private static Logger logger = LoggerFactory.getLogger(OpenAlertMain.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        logger.info("🚀 OpenAlertMain Execution Started!");

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

            AlertUDF alertUDF = new AlertUDF();
            alertUDF.jobcontext = jobContext;
            jobContext.sqlctx().udf().register(alertUDF.getName(), alertUDF, alertUDF.getReturnType());


            /*
             * READ OPEN ALERT RULES
             */

            String READ_OPEN_ALERT_RULES_QUERY = "SELECT ALERTID, EXPRESSION, DESCRIPTION, NAME AS ALARM_NAME, DOMAIN, VENDOR, TECHNOLOGY, CONFIGURATION FROM PERFORMANCE_ALERT WHERE PERFORMANCE_ALERT_ID_PK = '${PERFORMANCE_ALERT_ID_PK}'";

            READ_OPEN_ALERT_RULES_QUERY = READ_OPEN_ALERT_RULES_QUERY.replace("${PERFORMANCE_ALERT_ID_PK}", "1768");

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
            logger.info("PERFORMANCE_ALERT DF Displayed!");

            /*
             * READ ACTIVE ALARM LIBRARY
             */

            
            String READ_ACTIVE_ALARM_LIBRARY_QUERY = "SELECT ALARM_IDENTIFIER, TRIM(CLASSIFICATION) AS CLASSIFICATION, NETYPE, DEFAULT_SEVERITY, EMS_TYPE, ALARM_ID, SERVICE_AFFECTING, PROBABLE_CAUSE, MANUAL_CLEARED, EVENT_TYPE, CORRELATION_ENABLE, ALARM_GROUP FROM ALARM_LIBRARY WHERE ALARM_IDENTIFIER = '${ALERTID}'";

            READ_ACTIVE_ALARM_LIBRARY_QUERY = READ_ACTIVE_ALARM_LIBRARY_QUERY.replace("${ALERTID}", "0001");

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
            logger.info("ALARM_LIBRARY DF Displayed!");


            /*
             * JOIN OPEN ALERT RULES AND ACTIVE ALARM LIBRARY
             */


             String JOIN_OPEN_ALERT_RULES_AND_ACTIVE_ALARM_LIBRARY_QUERY = "SELECT pa.EXPRESSION, pa.DESCRIPTION, al.ALARM_IDENTIFIER, pa.ALARM_NAME, al.CLASSIFICATION, al.NETYPE, al.DEFAULT_SEVERITY, al.EMS_TYPE, al.ALARM_ID, al.SERVICE_AFFECTING, al.PROBABLE_CAUSE, al.MANUAL_CLEARED, al.EVENT_TYPE, pa.DOMAIN, pa.VENDOR, pa.TECHNOLOGY, al.CORRELATION_ENABLE, al.ALARM_GROUP, pa.CONFIGURATION FROM PERFORMANCE_ALERT pa INNER JOIN ALARM_LIBRARY al ON pa.ALERTID = al.ALARM_IDENTIFIER LIMIT 1";

            
             dataFrame = jobContext.sqlctx().sql(JOIN_OPEN_ALERT_RULES_AND_ACTIVE_ALARM_LIBRARY_QUERY);

             dataFrame.createOrReplaceTempView("JOIN_RESULT");

             dataFrame.show();
             logger.info("JOIN_RESULT DF Displayed!");

            /*
             * CALL UDF
             */

             String CALL_UDF_QUERY = "SELECT AlertUDF(CAST(EXPRESSION AS STRING), CAST(DESCRIPTION AS STRING), CAST(ALARM_IDENTIFIER AS STRING), CAST(ALARM_NAME AS STRING), CAST(CLASSIFICATION AS STRING), CAST(NETYPE AS STRING), CAST(DEFAULT_SEVERITY AS STRING), CAST(EMS_TYPE AS STRING), CAST(ALARM_ID AS STRING), CAST(SERVICE_AFFECTING AS STRING), CAST(PROBABLE_CAUSE AS STRING), CAST(MANUAL_CLEARED AS STRING), CAST(EVENT_TYPE AS STRING), CAST(DOMAIN AS STRING), CAST(VENDOR AS STRING), CAST(TECHNOLOGY AS STRING), CAST(CORRELATION_ENABLE AS STRING), CAST(ALARM_GROUP AS STRING), CAST(CONFIGURATION AS STRING)) AS RESULT FROM JOIN_RESULT";

             dataFrame = jobContext.sqlctx().sql(CALL_UDF_QUERY);

             dataFrame.createOrReplaceTempView("UDF_RESULT");

             dataFrame.show();
             logger.info("UDF_RESULT DF Displayed!");

             /*
              * EXPLODE UDF RESULT
              */

              String EXPLODE_UDF_RESULT_QUERY = "SELECT EXPLODE_RESULT.* FROM (SELECT EXPLODE(RESULT) AS EXPLODE_RESULT FROM UDF_RESULT) tmp";

              dataFrame = jobContext.sqlctx().sql(EXPLODE_UDF_RESULT_QUERY);

              dataFrame.show(5);
              logger.info("EXPLODE_UDF_RESULT DF Displayed!");

              /*
               * ALERT CQL READ
               */

               AlertCQLRead alertCQLRead = new AlertCQLRead();
               alertCQLRead.dataFrame = dataFrame;
               dataFrame = alertCQLRead.executeAndGetResultDataframe(jobContext);

               dataFrame.show(5, false);
               logger.info("Alert CQL Read DF Displayed!");

               /*
                * PROCESS CQL RESULT
                */

               ProcessCQLResult processCQLResult = new ProcessCQLResult();
               processCQLResult.dataFrame = dataFrame;
               dataFrame = processCQLResult.executeAndGetResultDataframe(jobContext);

               dataFrame.show(5, false);
               logger.info("Process CQL Result DF Displayed!");
              
        } catch (Exception e) {
            logger.error("Exception Occurred in OpenAlertMain: {}", e.getMessage());
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        logger.info("🚀 OpenAlertMain Execution Completed in {} Minutes {} Seconds", executionTime / 60000,
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

        jobContext.setParameters("BASE_TRINO_ORC_PATH", "s3a://performance/JOB/ORC/QUARTERLY/");
        jobContext.setParameters("BASE_TRINO_NE_PATH", "s3a://performance/NE_META/");


        jobContext.setParameters("ROW_KEY_APPENDER", "COMON_Router");
        jobContext.setParameters("JOB_FREQUENCY", "5 MIN");
        jobContext.setParameters("TIMESTAMP", "2025-07-24 00:00:00.000000+0000");

        return jobContext;

    }

    private static SparkConf getSparkConf() {

        return new SparkConf()
                .setAppName("OpenAlertMain")
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
