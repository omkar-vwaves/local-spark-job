package com.enttribe.custom.processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;

public class ScheduledTrigger {

    private static Logger logger = LoggerFactory.getLogger(ScheduledTrigger.class);
    public static JobContext jobContext = null;

    public static void main(String[] args) {

        SparkConf conf = getSparkConf();

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        jobContext = new JobContextImpl(session);
        jobContext = setParametersToJobContext(jobContext);
        // Dataset<Row> dataFrame = session.emptyDataFrame();

        try {
            Connection connection = getConnection(jobContext);
            // DailyScheduled dailyScheduled = new DailyScheduled();
            // dailyScheduled.runSnippet(null, null, null, connection, null);

            OptionalDailyScheduled optionalDailyScheduled = new OptionalDailyScheduled();
            optionalDailyScheduled.runSnippet(null, null, null, connection, null);
        } catch (Exception e) {
            logger.error("Error in ScheduledTrigger: {}", e.getMessage());
        }

        session.close();
    }

    private static Connection getConnection(JobContext jobContext) {

        Connection connection = null;

        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

        try {
            Class.forName(SPARK_PM_JDBC_DRIVER);
            connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                    SPARK_PM_JDBC_PASSWORD);
            return connection;
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException in ScheduledTrigger: {}", e.getMessage());
        } catch (SQLException e) {
            logger.error("SQLException in ScheduledTrigger: {}", e.getMessage());
        }

        return connection;
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
