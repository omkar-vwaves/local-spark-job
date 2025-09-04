package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PMJobTrigger {

    public static JobContext jobContext = null;

    private static Logger logger = LoggerFactory.getLogger(PMJobTrigger.class);

    public static void main(String[] args) {
        SparkConf conf = getSparkConf();

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        jobContext = new JobContextImpl(session);
        jobContext = setParametersToJobContext(jobContext);

        Dataset<Row> dataFrame = session.emptyDataFrame();

        try {
            dataFrame = TRANSPORT_JUNIPER_COMMON_QUARTERLY_KPI_COMPUTATION.callSubFlow(dataFrame, jobContext);
        } catch (Exception e) {
            logger.error("Job Failed: {}", e.getMessage());
        }

    }

    private static JobContext setParametersToJobContext(JobContext jobContext) {

        jobContext.setParameters("SPARK_PM_JDBC_URL",
                "jdbc:mysql://localhost:3306/PERFORMANCE_RLTL?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false");
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
        jobContext.setParameters("MODULE", "PM");
        jobContext.setParameters("NE_TYPE", "'INTERFACE','ROUTER'");
        jobContext.setParameters("NODES", "INTERFACE,ROUTER");
        jobContext.setParameters("EMS_TYPE", "NA");
        jobContext.setParameters("RAW_FILE_PATH", "performance/JOB/RawFiles/JUNIPER/");
        jobContext.setParameters("RAW_FILE_PATH_SUFFIX", "20250808/0515");
        jobContext.setParameters("PROCESSING_DATE", "250808");
        jobContext.setParameters("PROCESSING_HOUR", "0515");
        jobContext.setParameters("AUDIT_TIME", "2025-08-08 05:15:00");
        jobContext.setParameters("IS_FILE_FILTER_REQUIRED", "true");
        jobContext.setParameters("KPI_ORC_PATH", "s3a://performance/JOB/ORC/");
        jobContext.setParameters("DI_ORC_PATH", "s3a://performance/JOB/DI/");
        jobContext.setParameters("JOB_TYPE", "QUARTERLY");
        jobContext.setParameters("CQL_TABLE_NAME", "combinequarterlypm_test");

        return jobContext;

    }

    private static SparkConf getSparkConf() {

        return new SparkConf()
                .setAppName("PMJobTrigger")
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
