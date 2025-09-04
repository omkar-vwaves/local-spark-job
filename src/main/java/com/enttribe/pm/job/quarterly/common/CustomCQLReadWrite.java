package com.enttribe.pm.job.quarterly.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

public class CustomCQLReadWrite extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CustomCQLReadWrite.class);

    private String tempTable;

    public CustomCQLReadWrite() {
        super();
    }

    public CustomCQLReadWrite(Dataset<Row> dataFrame, Integer id, String processorName, String tempTable) {
        super(id, processorName);
        this.tempTable = tempTable;
        this.dataFrame = dataFrame;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("🚀 CustomCQLReadWrite Execution Started!");

        try {
            // Cassandra config
            String cassandraHost = "demo-dc1-service.k8ssandra-operator.svc.cluster.local";
            String cassandraDatacenter = "dc1";
            String cassandraPort = "9042";
            String cassandraKeyspace = "fms_keyspace";
            // String sourceTable = "event_notification";
            // String targetTable = "event_notification_test";

            String sourceTable = "event_notification_test";
            String targetTable = "event_notification";
            String cassandraUsername = "cassandra";
            String cassandraPassword = "cassandra";

            logger.info("Cassandra Host: {}, Port: {}, Keyspace: {}, Source Table: {}, Target Table: {}",
                    cassandraHost, cassandraPort, cassandraKeyspace, sourceTable, targetTable);

            if (cassandraHost == null || cassandraHost.trim().isEmpty() ||
                    cassandraKeyspace == null || cassandraKeyspace.trim().isEmpty() ||
                    sourceTable == null || sourceTable.trim().isEmpty() ||
                    targetTable == null || targetTable.trim().isEmpty()) {
                throw new IllegalArgumentException("Missing Mandatory Cassandra Configuration Values!");
            }

            try {
                // jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                // .set("spark.cassandra.connection.host", cassandraHost);
                // jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                // .set("spark.cassandra.connection.port", cassandraPort);
                // jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                // .set("spark.cassandra.auth.username", cassandraUsername);
                // jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                // .set("spark.cassandra.auth.password", cassandraPassword);

                jobContext.sqlctx().sparkSession().conf().set("spark.cassandra.connection.host", cassandraHost);
                jobContext.sqlctx().sparkSession().conf().set("spark.cassandra.connection.localDC",
                        cassandraDatacenter);
                jobContext.sqlctx().sparkSession().conf().set("spark.cassandra.connection.port", cassandraPort);
                jobContext.sqlctx().sparkSession().conf().set("spark.cassandra.auth.username", cassandraUsername);
                jobContext.sqlctx().sparkSession().conf().set("spark.cassandra.auth.password", cassandraPassword);

                jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                        .set("spark.cassandra.connection.host", cassandraHost);
                jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                        .set("spark.cassandra.connection.localDC", cassandraDatacenter);
                jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                        .set("spark.cassandra.connection.port", cassandraPort);
                jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                        .set("spark.cassandra.auth.username", cassandraUsername);
                jobContext.sqlctx().sparkSession().sparkContext().hadoopConfiguration()
                        .set("spark.cassandra.auth.password", cassandraPassword);

                logger.info("Final Cassandra Connection Configuration Set Successfully! ✅");

            } catch (Exception e) {
                logger.error("❌ Failed to Set Cassandra Configuration: {}", e.getMessage());
                throw new RuntimeException("Cassandra Configuration Setup Failed!", e);
            }

            Map<String, String> readOptions = new HashMap<>();
            readOptions.put("keyspace", cassandraKeyspace);
            readOptions.put("table", sourceTable);

            Dataset<Row> df;
            try {
                df = jobContext.sqlctx().read()
                        .format("org.apache.spark.sql.cassandra")
                        .options(readOptions)
                        .load();
                logger.info("✅ Successfully Loaded Data From {}.{}", cassandraKeyspace, sourceTable);
            } catch (Exception e) {
                logger.error("❌ Failed to Read Data From {}.{}: {}", cassandraKeyspace, sourceTable, e.getMessage());
                throw new RuntimeException("Cassandra Read Failed", e);
            }

            try {
                long count = df.count();
                logger.info("✅ Total Records Read: {}", count);
            } catch (Exception e) {
                logger.error("⚠️ Could Not Count Records: {}", e.getMessage());
            }

            // Dataset<Row> castedDF = df.withColumn("first_processing_date",
            // df.col("first_processing_date").cast("string"));

            Map<String, String> writeOptions = new HashMap<>();
            writeOptions.put("keyspace", cassandraKeyspace);
            writeOptions.put("table", targetTable);

            try {
                logger.info("📤 Writing Data To {}.{} ...", cassandraKeyspace, targetTable);

                df.write()
                        .format("org.apache.spark.sql.cassandra")
                        .options(writeOptions)
                        .mode("append")
                        .save();

                logger.info("✅ Successfully Written Data To {}.{}", cassandraKeyspace, targetTable);
            } catch (Exception e) {
                logger.error("❌ Failed to Write To Target Table: {}", e.getMessage());
                throw new RuntimeException("Write to Cassandra Failed", e);
            }

            if (tempTable != null && !tempTable.trim().isEmpty()) {
                try {
                    df.createOrReplaceGlobalTempView(tempTable);
                    logger.info("✅ Created Global Temp View: {}", tempTable);
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to Create Global Temp View: {}", tempTable, e.getMessage());
                }
            }

            return df;

        } catch (Exception e) {
            logger.error("🔥 Unexpected Error in CustomCQLReadWrite: {}", e.getMessage(), e);
            throw e;
        }
    }
}
