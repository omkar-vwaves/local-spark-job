package com.enttribe.pm.job.fiveminute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.StreamingQueryManager;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.SparkRunnerUtils;
import com.enttribe.sparkrunner.util.Utils;

/**
 * 5-Minute interval streaming job pushing data to SeaweedFS (S3-compatible) storage.
 * Automatically restarts on critical SeaweedFS/S3 connectivity failures.
 */
public class GeneralPushToS3Updated extends Processor {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(GeneralPushToS3Updated.class);

    public String checkPointLoc;
    public String outputMode = "Append";
    public String triggerType = "Processing Time";
    public String batchInterval;
    public String topic;
    public String rawFilesPath;
    public String minioAccessKey;
    public String minioSecretKey;
    public String minioEndpoint;

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("==== GENERAL PUSH TO S3 STARTED ====");
        dataFrame = transformDataFrame(dataFrame);

        Map<String, String> optionsMap = prepareCheckpointOptions(jobContext);

        try {
            StreamingQuery query = dataFrame.writeStream()
                    .foreachBatch(createBatchFunction())
                    .options(optionsMap)
                    .trigger(getTrigger(this.triggerType, this.batchInterval))
                    .outputMode(getOutputMode(this.outputMode))
                    .start();

            query.awaitTermination();

        }  catch (SdkClientException e) {
            logger.error("FATAL: SeaweedFS (S3) Communication Failure Detected, Message: {}", e.getMessage(), e);
            forceStopStreamingAndExit();
        } catch (Exception e) {
            logger.error("Unexpected Fatal Error in GeneralPushToS3Updated, Message: {}", e.getMessage(), e);
            forceStopStreamingAndExit();
        }

        logger.info("==== GENERAL PUSH TO S3 COMPLETED ====");
        return dataFrame;
    }

    private Dataset<Row> transformDataFrame(Dataset<Row> df) {
        return df.withColumn("parsed_json",
                        functions.from_json(
                                functions.col("value").cast("string"),
                                new StructType().add("timestamp", DataTypes.LongType)))
                .filter("parsed_json IS NOT NULL AND parsed_json.timestamp IS NOT NULL")
                .selectExpr(
                        "CAST(value AS STRING) AS value",
                        "DATE_FORMAT(to_utc_timestamp(FROM_UNIXTIME(FLOOR(parsed_json.timestamp / 300) * 300), 'UTC'), 'yyyyMMddHHmm') AS fiveMinInterval");
    }

    private Map<String, String> prepareCheckpointOptions(JobContext jobContext) {
        Map<String, String> options = new HashMap<>();
        if (Utils.hasValidValue(this.checkPointLoc)) {
            String topicName = this.topic;
            String checkpointLocation = SparkRunnerUtils.getCheckpointLocation(topicName, this.checkPointLoc);
            logger.info("GENERAL TOPIC ==> {} AND CHECKPOINT LOCATION ==> {}", topicName, checkpointLocation);
            options.put("checkpointLocation", checkpointLocation);
        } else {
            logger.warn("No checkpointLocation provided - streaming is not fault tolerant without durable checkpoint.");
        }
        return options;
    }

    private VoidFunction2<Dataset<Row>, Long> createBatchFunction() {
        return (batchDF, batchId) -> {
            long startTime = System.currentTimeMillis();
            logger.info("GENERAL PROCESSING BATCH ID ==> {}", batchId);

            long incoming = batchDF.count();
            if (incoming == 0L) {
                logger.info("Batch {} Has No Data. Skipping.", batchId);
                return;
            }

            logger.info("Batch {} Incoming Records: {}", batchId, incoming);

            List<Row> quarterRows = batchDF.select("fiveMinInterval").distinct().collectAsList();
            logger.info("Batch {} fiveMinInterval To Write: {}", batchId, quarterRows.size());

            for (Row qRow : quarterRows) {
                String fiveMinInterval = qRow.getString(0);
                if (fiveMinInterval == null) {
                    logger.error("Skipping Invalid fiveMinInterval Value in Batch {}: {}", batchId, fiveMinInterval);
                    continue;
                }

                String date = fiveMinInterval.substring(0, 8);
                String time = fiveMinInterval.substring(8);

                Dataset<Row> orcDF = batchDF.filter(functions.col("fiveMinInterval").equalTo(fiveMinInterval))
                        .selectExpr("CAST(value AS STRING) AS rawData")
                        .withColumn("date", functions.lit(date))
                        .withColumn("time", functions.lit(time));

                if (orcDF.isEmpty()) {
                    logger.info("No Rows For fiveMinInterval {} In Batch {}", fiveMinInterval, batchId);
                    continue;
                }

                try {
                    orcDF.write()
                            .format("orc")
                            .mode(SaveMode.Append)
                            .partitionBy("date", "time")
                            .option("compression", "snappy")
                            .option("spark.hadoop.fs.s3a.committer.name", "directory")
                            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
                            .save(rawFilesPath);

                    logger.info("GENERAL SUCCESSFULLY WROTE fiveMinInterval={} (date={},time={}) to {}",
                            fiveMinInterval, date, time, rawFilesPath);

                } catch (SdkClientException e) {
                    logger.error("SEAWEEDFS/S3 FAILURE DURING WRITE for fiveMinInterval {}. Triggering job shutdown.", fiveMinInterval, e);
                    forceStopStreamingAndExit();
                } catch (Exception e) {
                    logger.error("GENERAL FAILED WRITING fiveMinInterval={} (date={},time={}) to {}. Retrying next batch.",
                            fiveMinInterval, date, time, rawFilesPath, e);
                    throw new RuntimeException("Failed to write ORC for fiveMinInterval " + fiveMinInterval, e);
                }
            }

            long durationMs = System.currentTimeMillis() - startTime;
            logger.info("GENERAL BATCH ID {} PROCESSED IN {} MS", batchId, durationMs);
        };
    }

    private Trigger getTrigger(String trigger, String interval) {
        long intervalMs = Utils.hasValidValue(interval) ? Long.parseLong(interval) : 10L;
        return "ONCE".equalsIgnoreCase(trigger)
                ? Trigger.Once()
                : Trigger.ProcessingTime(intervalMs, TimeUnit.MILLISECONDS);
    }

    private OutputMode getOutputMode(String outputMode) {
        if (Utils.hasValidValue(outputMode)) {
            switch (outputMode.toUpperCase()) {
                case "UPDATE":
                    return OutputMode.Update();
                case "COMPLETE":
                    return OutputMode.Complete();
            }
        }
        return OutputMode.Append();
    }

    private void forceStopStreamingAndExit() {
        try {
            SparkSession spark = SparkSession.active();
            StreamingQueryManager sqm = spark.streams();
            for (StreamingQuery q : sqm.active()) {
                logger.info("Stopping Active Query: {}", q.name());
                q.stop();
            }
        } catch (Exception ex) {
            logger.error("Error While Stopping Streaming Queries, Message: {}", ex.getMessage(), ex);
        }
        logger.error("Forcefully Terminating Spark Driver to Allow Orchestrator Restart.");
        System.exit(1);
    }
}
