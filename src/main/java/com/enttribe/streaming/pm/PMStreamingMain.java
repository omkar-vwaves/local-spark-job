package com.enttribe.streaming.pm;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import com.enttribe.fm.StructuredStreamingKafkaCustom;
import com.enttribe.sparkrunner.context.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.enttribe.sparkrunner.context.JobContextImpl;
import com.enttribe.sparkrunner.processors.dataset.hdfs.ORCRead;
import com.enttribe.streaming.KafkaShow;

public class PMStreamingMain {

        private static final Logger logger = LogManager.getLogger(PMStreamingMain.class);

        public static void main(String[] args) {

                // ✅ PM Jobs Configuration
                String pmJobsConfiguration = "{"
                                + "\"KAFKA_TOPIC_NAME\": \"pm.juniper.streaming.status\","
                                + "\"DOMAIN\": \"TRANSPORT\","
                                + "\"VENDOR\": \"JUNIPER\","
                                + "\"TECHNOLOGY\": \"COMMON\","
                                + "\"EMS_TYPE\": \"NA\","
                                + "\"FREQUENCY\": \"5 MIN\","
                                + "\"JOB_TYPE\": \"FIVEMIN\","
                                + "\"NE_TYPES\": \"'INTERFACE','ROUTER'\","
                                + "\"NODES\": \"INTERFACE,ROUTER\","
                                + "\"BASE_PATH\": \"s3a://performance/JOB/RawFiles/JUNIPER/FIVEMIN/\","
                                + "\"KPI_ORC_PATH\": \"s3a://performance/JOB/ORC/FIVEMIN/\","
                                + "\"CQL_TABLE_NAME\": \"combine5minpm\""
                                + "}";

                // ✅ Spark Streaming Credentials
                String sparkStreamingCredentials = "{"
                                + "\"PM_SPARK_JDBC_URL\":\"jdbc:mysql://localhost:3306/PERFORMANCE_A_LAB?autoReconnect=true&permitMysqlScheme=true&sessionVariables=sql_mode=''\","
                                + "\"PM_SPARK_JDBC_USERNAME\":\"root\","
                                + "\"PM_SPARK_JDBC_PASSWORD\":\"root\","
                                + "\"SPARK_CASSANDRA_KEYSPACE_PM\":\"pm\","
                                + "\"SPARK_CASSANDRA_HOST\":\"localhost\","
                                + "\"SPARK_CASSANDRA_PORT\":\"9042\","
                                + "\"SPARK_CASSANDRA_USERNAME\":\"cassandra\","
                                + "\"SPARK_CASSANDRA_PASSWORD\":\"cassandra\","
                                + "\"SPARK_CASSANDRA_DATACENTER\":\"datacenter1\","
                                + "\"INPUT_KAFKA_SSL_ENABLED\":\"false\","
                                + "\"KAFKA_INPUT_BOOTSTRAP_SERVERS\":\"localhost:9092\","
                                + "\"INPUT_TRUSTSTORE_LOCATION\":\"/opt/spark/jars/kafka.keystore.jks\","
                                + "\"INPUT_TRUSTSTORE_PASSWORD\":\"kafka123\","
                                + "\"INPUT_KEYSTORE_LOCATION\":\"/opt/spark/jars/kafka.keystore.jks\","
                                + "\"INPUT_KEYSTORE_PASSWORD\":\"kafka123\","
                                + "\"INPUT_KEYSTORE_TYPE\":\"jks\","
                                + "\"INPUT_TRUSTSTORE_TYPE\":\"jks\","
                                + "\"INPUT_IDENTIFICATION_ALGO\":\"\","
                                + "\"OP_KAFKA_SSL_ENABLED\":\"false\","
                                + "\"KAFKA_OP_BOOTSTRAP_SERVERS\":\"localhost:9092\","
                                + "\"OP_TRUSTSTORE_LOCATION\":\"/opt/spark/jars/kafka.keystore.jks\","
                                + "\"OP_TRUSTSTORE_PASSWORD\":\"kafka123\","
                                + "\"OP_KEYSTORE_LOCATION\":\"/opt/spark/jars/kafka.keystore.jks\","
                                + "\"OP_KEYSTORE_PASSWORD\":\"kafka123\","
                                + "\"OP_KEYSTORE_TYPE\":\"jks\","
                                + "\"OP_TRUSTSTORE_TYPE\":\"jks\","
                                + "\"OP_IDENTIFICATION_ALGO\":\"\","
                                + "\"ENVIRONMENT\":\"Telus-A Lab\""
                                + "}";

                // ✅ Spark Configuration
                SparkConf conf = new SparkConf()
                                .setAppName("PM Streaming Job")
                                .setMaster("local[*]")
                                .set("spark.driver.memory", "8g")
                                .set("spark.sql.shuffle.partitions", "200")
                                .set("spark.default.parallelism", "100")
                                .set("spark.local.dir", "/Users/ent-00356/Documents/spark-local-dir")
                                .set("spark.network.timeout", "600s")
                                .set("spark.sql.files.maxPartitionBytes", "134217728")
                                .set("spark.sql.session.timeZone", "UTC")
                                .set("spark.sql.ansi.enabled", "false")

                                // ✅ S3A (MinIO) Configuration
                                .set("spark.hadoop.fs.s3a.access.key", "bootadmin")
                                .set("spark.hadoop.fs.s3a.secret.key", "bootadmin")
                                .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                                .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                                                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                                // ✅ Cassandra Configuration
                                .set("spark.cassandra.connection.host", "localhost")
                                .set("spark.cassandra.connection.port", "9042")
                                .set("spark.cassandra.auth.username", "cassandra")
                                .set("spark.cassandra.auth.password", "cassandra")

                                // ✅ Trigger Configuration for Streaming
                                .set("spark.triggerJson", pmJobsConfiguration)
                                .set("extraConfig", sparkStreamingCredentials)
                                .set("spark.executor.extraJavaOptions", "-DextraConfig=" + sparkStreamingCredentials)
                                .set("spark.kubernetes.driverEnv.extraConfig", sparkStreamingCredentials)
                                .set("spark.driver.extraJavaOptions", "-DextraConfig=" + sparkStreamingCredentials);

                // ✅ Spark Session
                SparkSession session = SparkSession.builder()
                                .config(conf)
                                .getOrCreate();

                // ✅ Extra Config
                System.setProperty("extraConfig", sparkStreamingCredentials);
                logger.info("Extra Config: {}", System.getProperty("extraConfig"));

                // ✅ Job Context
                JobContext jobContext = new JobContextImpl(session);

                jobContext.setParameters("SPARK_KAFKA_BROKER_ANSIBLE", "localhost:9092");
                jobContext.setParameters("SPARK_KAFKA_TOPIC", "pm.juniper.streaming.status");
                jobContext.setParameters("SPARK_KAFKA_GROUP", "pm.juniper.streaming.group");
                jobContext.setParameters("SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER", "10");
                jobContext.setParameters("SPARK_KAFKA_STARTING_OFFSETS", "earliest");
                jobContext.setParameters("SPARK_KAFKA_BATCH_INTERVAL", "1");
                jobContext.setParameters("checkPointLoc", "s3a://performance/checkpoints/FIVEMIN/JUNIPER");
                jobContext.setParameters("outputMode", "Append");
                jobContext.setParameters("triggerType", "Processing Time");

                Dataset<Row> dataFrame = session.emptyDataFrame();

                try {

                        // ✅ Kafka Read
                        StructuredStreamingKafkaCustom structuredStreamingKafka = new StructuredStreamingKafkaCustom();
                        structuredStreamingKafka.dataFrame = dataFrame;
                        structuredStreamingKafka.maxOffsetsPerTrigger = jobContext
                                        .getParameter("SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER");
                        structuredStreamingKafka.startingOffsets = jobContext
                                        .getParameter("SPARK_KAFKA_STARTING_OFFSETS");
                        structuredStreamingKafka.batchInterval = jobContext.getParameter("SPARK_KAFKA_BATCH_INTERVAL");
                        structuredStreamingKafka.group = jobContext.getParameter("SPARK_KAFKA_GROUP");
                        structuredStreamingKafka.brokers = jobContext.getParameter("SPARK_KAFKA_BROKER_ANSIBLE");
                        structuredStreamingKafka.topics = jobContext.getParameter("SPARK_KAFKA_TOPIC");
                        structuredStreamingKafka.processorId = 1;
                        structuredStreamingKafka.processorName = "Kafka Read";
                        structuredStreamingKafka.tempTable = "KafkaResult";

                        logger.info("=====***** 🚀 Starting PM Streaming Application  asdq *****=====");

                        dataFrame = structuredStreamingKafka.executeAndGetResultDataframe(jobContext);

                        logger.info("PM Streaming Started Successfully. Application Will Continue Running...");


                      

                        // ✅ Extract Values from JSON And Call ORCRead Processor
                        // Dataset<Row> transformedDataFrame = dataFrame
                        //         .selectExpr("CAST(value AS STRING) AS InputJson")
                        //         .selectExpr("GET_JSON_OBJECT(InputJson, '$.inputPath') AS InputPath",
                        //                   "GET_JSON_OBJECT(InputJson, '$.timestamp') AS Timestamp",
                        //                   "GET_JSON_OBJECT(InputJson, '$.status') AS Status");

                        // List<Row> rows = transformedDataFrame.collectAsList();
                        // logger.info("Rows: {}", rows);
                        // if (!rows.isEmpty()) {
                        //     Row firstRow = rows.get(0);
                            jobContext.setParameters("InputPath","s3a://performance/JOB/RawFiles/JUNIPER/20250725/0005/");
                        //     jobContext.setParameters("Timestamp", firstRow.getString(1));
                        //     jobContext.setParameters("Status", firstRow.getString(2));
                        // }
                        

                        // ✅ ORC Read Using Extracted InputPath
                        // String inputPath = transformedDataFrame.select("InputPath").first().getString(0);
                        // logger.info("Extracted inputPath: {}", inputPath);

                        // ✅ ORC Read
                        
                        ORCReadStream orcRead = new ORCReadStream(1, "READ ORC", jobContext.getParameter("InputPath"), "ORCRead", jobContext.getParameter("InputPath"));
                        orcRead.dataFrame = dataFrame;
                        dataFrame = orcRead.executeAndGetResultDataframe(jobContext);

                        logger.info("ORC Read Processor Execution Completed...");

                        // KafkaShow kafkaShow = new KafkaShow();
                        // kafkaShow.dataFrame = dataFrame;
                        // kafkaShow.processorId = 2;
                        // kafkaShow.processorName = "Kafka Show";
                        // kafkaShow.noOfRows = "10";
                        // kafkaShow.truncate = "false";
                        // dataFrame = kafkaShow.executeAndGetResultDataframe(jobContext);


                        dataFrame.writeStream()
                        .format("console") // behaves like show()
                        .outputMode("append") // or "update", depending on your use case
                        .option("truncate", false)
                        .option("numRows", 50) // similar to show(50)
                        .start()
                        .awaitTermination();
                        logger.info("Kafka Show Processor Execution Completed...");

                } catch (Exception e) {
                        logger.error(
                                        "⚠️ Exception While Starting FM Parser Process, Message: " + e.getMessage()
                                                        + " | Error: " + e);
                }
        }

}
