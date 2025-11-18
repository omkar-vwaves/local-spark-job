package com.enttribe.fm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import com.enttribe.sparkrunner.context.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.enttribe.sparkrunner.context.JobContextImpl;
import com.enttribe.sparkrunner.processors.streaming.StreamingKafkaForEachBatch;
import com.enttribe.sparkrunner.udf.GetGeographyColumns;
import com.enttribe.sparkrunner.udf.JuniperParser;
// import com.enttribe.sparkrunner.udf.JuniperParserNew;

public class ParserMain {

        private static final Logger logger = LogManager.getLogger(ParserMain.class);

        public static void main(String[] args) {

                // ✅ FM Configuration
                String fmConfiguration = "{"
                                + "\"group\": \"alarm\","
                                + "\"topic\": \"juniper.events\","
                                + "\"domain\": \"'TRANSPORT'\","
                                + "\"vendor\": \"'JUNIPER'\","
                                + "\"emstype\": \"'NA'\","
                                + "\"hbTopic\": \"juniper.nifi.streaming.heartbeat\","
                                + "\"neReloadMin\": 180,"
                                + "\"peReloadMin\": 240,"
                                + "\"alReloadMin\": 240,"
                                + "\"ruleReloadMin\": \"180\","
                                + "\"batchSize\": \"2000\","
                                + "\"dumpTopic\": \"juniper.events.dump\","
                                + "\"emsVendor\": \"juniper\","
                                + "\"isAlertJob\": true,"
                                + "\"isEventJob\": false,"
                                + "\"isEventEnrich\": false,"
                                + "\"notifTopic\": \"juniper.nifi.streaming.notification\","
                                + "\"parserName\": \"JuniperParserNew\","
                                + "\"eventParserName\": \"EventParser\","
                                + "\"alertParserName\": \"AlertParser\","
                                + "\"technology\": \"COMMON\","
                                + "\"instancejob\": \"true\","
                                + "\"isDebugging\": true,"
                                + "\"schemastring\": \"geography_l4_name STRING,geography_l4_id STRING,geography_l3_name STRING, geography_l3_id STRING,geography_l2_name STRING,geography_l2_id STRING,geography_l1_name STRING, geography_l1_id STRING,data_center_type STRING,data_center_name STRING\","
                                + "\"templateName\": \"FM TRANSPORT CISCO PARSER STREAMING\","
                                + "\"batchInterval\": 10,"
                                + "\"checkPointLoc\": \"s3a://nst-fm/checkpoints/juniper-transport-parser\","
                                + "\"discardDomain\": \"'TRANSPORT'\","
                                + "\"discardVendor\": \"'JUNIPER'\","
                                + "\"startingOffset\": \"earliest\","
                                + "\"isHeartBeatFlow\": \"NA\","
                                + "\"notificationjob\": \"false\","
                                + "\"closeInstanceJob\": \"false\","
                                + "\"geographyColumns\": \"GEOGRAPHYL4::geography_l4_name:geography_l4_id,GEOGRAPHYL3::geography_l3_name:geography_l3_id,GEOGRAPHYL2::geography_l2_name:geography_l2_id,GEOGRAPHYL1::geography_l1_name:geography_l1_id,CDC::data_center_type:data_center_name\","
                                + "\"readAlarmLibrary\": \"true\","
                                + "\"readPlannedEvent\": \"true\","
                                + "\"kafkaProduceTopic\": \"juniper.events.fault\","
                                + "\"isNotificationFlow\": \"false\","
                                + "\"readNetworkElement\": \"true\""
                                + "}";

                // ✅ Spark Streaming Credentials
                String sparkStreamingCredentials = "{"
                                + "\"JDBC_URL\":\"jdbc:mysql://localhost:3306/FMS?autoReconnect=true&permitMysqlScheme=true&sessionVariables=sql_mode=''\","
                                + "\"JDBC_USER\":\"root\","
                                + "\"JDBC_PASSWORD\":\"root\","
                                + "\"LCM_JDBC_URL\":\"jdbc:mysql://localhost:3306/LCM_DEV?autoReconnect=true&permitMysqlScheme=true\","
                                + "\"LCM_JDBC_USER\":\"root\","
                                + "\"LCM_JDBC_PASSWORD\":\"root\","
                                + "\"LCM_URL\":\"http://lcm-service-commissioning-service.ansible.svc.cluster.local/commissioning/NetworkElement/populateGplRequestResponse\","
                                + "\"CASSANDRA_KEYSPACE\":\"fms_keyspace\","
                                + "\"CASSANDRA_HOST\":\"localhost\","
                                + "\"CASSANDRA_PORT\":\"9042\","
                                + "\"CASSANDRA_USERNAME\":\"cassandra\","
                                + "\"CASSANDRA_PASSWORD\":\"cassandra\","
                                + "\"CASSANDRA_DATACENTER\":\"datacenter1\","
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
                                + "\"ENVIRONMENT\":\"lab-a Instance\""
                                + "}";

                // ✅ Spark Configuration
                SparkConf conf = new SparkConf()
                                .setAppName("FM Parser Streaming Job")
                                .setMaster("local[*]")
                                .set("spark.driver.memory", "8g")
                                .set("spark.sql.shuffle.partitions", "200")
                                .set("spark.default.parallelism", "100")
                                .set("spark.local.dir", "/Users/ent-00356/Documents/spark-local-dir")
                                .set("spark.network.timeout", "600s")
                                .set("spark.sql.files.maxPartitionBytes", "134217728")
                                .set("spark.sql.session.timeZone", "UTC")
                                .set("spark.sql.ansi.enabled", "false")

                                // ✅ S3A (MinIO) config
                                .set("spark.hadoop.fs.s3a.access.key", "bootadmin")
                                .set("spark.hadoop.fs.s3a.secret.key", "bootadmin")
                                .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                                .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                                                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                                // ✅ Cassandra config
                                .set("spark.cassandra.connection.host", "localhost")
                                .set("spark.cassandra.connection.port", "9042")
                                .set("spark.cassandra.auth.username", "cassandra")
                                .set("spark.cassandra.auth.password", "cassandra")

                                // ✅ Trigger Configuration for Streaming
                                .set("spark.triggerJson", fmConfiguration)
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

                // ✅ UDFs
                JuniperParser juniperParser = new JuniperParser();
                session.udf().register(juniperParser.getName(), juniperParser, juniperParser.getReturnType());

                // EventParser eventParser = new EventParser();
                // session.udf().register(eventParser.getName(), eventParser, eventParser.getReturnType());

                // AlertParser alertParser = new AlertParser();
                // session.udf().register(alertParser.getName(), alertParser, alertParser.getReturnType());


                // JuniperParserNew juniperParserNew = new JuniperParserNew();
                // session.udf().register(juniperParserNew.getName(), juniperParserNew, juniperParserNew.getReturnType());


                GetGeographyColumns getGeographyColumns = new GetGeographyColumns();
                session.udf().register(getGeographyColumns.getName(), getGeographyColumns,
                                getGeographyColumns.getReturnType());

                // ✅ Job Context
                JobContext jobContext = new JobContextImpl(session);

                jobContext.setParameters("SPARK_KAFKA_BROKER_ANSIBLE", "localhost:9092");
                jobContext.setParameters("SPARK_KAFKA_TOPIC", "juniper.events");
                jobContext.setParameters("SPARK_KAFKA_GROUP", "juniper.events.group");
                jobContext.setParameters("SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER", "10");
                jobContext.setParameters("SPARK_KAFKA_STARTING_OFFSETS", "earliest");
                jobContext.setParameters("SPARK_KAFKA_BATCH_INTERVAL", "10");
                jobContext.setParameters("checkPointLoc", "s3a://fault-management/checkpoints/JUNIPER");
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

                        logger.info("Starting Spark Streaming Application........................................");

                        dataFrame = structuredStreamingKafka.executeAndGetResultDataframe(jobContext);

                        logger.info("Kafka Streaming Started Successfully. Application Will Continue Running...");

                        // ✅ Add Query Listener
                        AddQueryListener addQueryListener = new AddQueryListener();
                        addQueryListener.dataFrame = dataFrame;
                        dataFrame = addQueryListener.executeAndGetResultDataframe(jobContext);

                        // ✅ Streaming Kafka For Each Batch
                        StreamingKafkaForEachBatch streamingKafkaForEachBatch = new StreamingKafkaForEachBatch();
                        streamingKafkaForEachBatch.dataFrame = dataFrame;
                        streamingKafkaForEachBatch.processorId = 2;
                        streamingKafkaForEachBatch.processorName = "Kafka For Each Batch";
                        streamingKafkaForEachBatch.className = "ParseAlarms";
                        streamingKafkaForEachBatch.packageName = "com.enttribe.fm";
                        streamingKafkaForEachBatch.customCode = "package com.enttribe.fm;\n" +
                                        "import com.enttribe.fms.mysql.foreach.ParserForEachBatch;\n" +
                                        "public class ParseAlarms extends ParserForEachBatch {}";
                        streamingKafkaForEachBatch.outputMode = "Append";
                        streamingKafkaForEachBatch.triggerType = "Processing Time";
                        streamingKafkaForEachBatch.interval = jobContext.getParameter("SPARK_KAFKA_BATCH_INTERVAL");
                        streamingKafkaForEachBatch.checkPointLocation = jobContext.getParameter("checkPointLoc");

                        dataFrame = streamingKafkaForEachBatch.executeAndGetResultDataframe(jobContext);

                } catch (Exception e) {
                        logger.error(
                                        "⚠️ Exception While Starting FM Parser Process, Message: " + e.getMessage()
                                                        + " | Error: " + e);
                }
        }

}
