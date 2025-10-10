package com.enttribe.pm.job.fiveminute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.SparkRunnerUtils;
import com.enttribe.sparkrunner.util.Utils;

public class GeneralPushToS3 extends Processor {
    private static final long serialVersionUID = 1L;

    public String checkPointLoc;
    public String outputMode = "Append";
    public String triggerType = "Processing Time";
    public String batchInterval;
    public String topic;
    public String rawFilesPath;
    public String minioAccessKey;
    public String minioSecretKey;
    public String minioEndpoint;

    private static final Logger logger = LoggerFactory.getLogger(GeneralPushToS3.class);

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("====GENERAL PUSH TO S3 STARTED====");
        dataFrame = transformDataFrame(dataFrame);

        Map<String, String> optionsMap = prepareCheckpointOptions(jobContext);
        StreamingQuery query = dataFrame.writeStream()
                .foreachBatch(createBatchFunction())
                .options(optionsMap)
                .trigger(getTrigger(this.triggerType, this.batchInterval))
                .outputMode(getOutPutMode(this.outputMode))
                .start();
        query.awaitTermination();

        logger.info("====GENERAL PUSH TO S3 COMPLETED====");

        return dataFrame;
    }

    private Dataset<Row> transformDataFrame(Dataset<Row> df) {
        return df.withColumn("parsed_json",
                functions.from_json(functions.col("value").cast("string"),
                        new StructType().add("timestamp", DataTypes.LongType)))
                .filter("parsed_json IS NOT NULL AND parsed_json.timestamp IS NOT NULL")
                .selectExpr(
                        "CAST(value AS STRING) AS value",
                        "DATE_FORMAT(to_utc_timestamp(FROM_UNIXTIME(FLOOR(parsed_json.timestamp / 300) * 300), 'UTC'), 'yyyyMMddHHmm') AS quarter");
    }

    private Map<String, String> prepareCheckpointOptions(JobContext jobContext) {
        Map<String, String> options = new HashMap<>();
        if (Utils.hasValidValue(this.checkPointLoc)) {
            String topicName = this.topic;
            String checkpointLocation = SparkRunnerUtils.getCheckpointLocation(topicName,
                    this.checkPointLoc);
            logger.info("GENERAL TOPICS ==> {} AND CHECKPOINT LOCATION ==> {}", topicName, checkpointLocation);
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
                logger.info("Batch {} has no data. Skipping.", batchId);
                return;
            }
            logger.info("Batch {} incoming records: {}", batchId, incoming);

            Dataset<Row> quarters = batchDF.select("quarter").distinct();
            List<Row> quarterRows = quarters.collectAsList();
            logger.info("Batch {} quarters to write: {}", batchId, quarterRows.size());

            for (Row qRow : quarterRows) {
                String quarter = qRow.getString(0);
                if (quarter == null || quarter.length() < 12) {
                    logger.warn("Skipping invalid quarter value in batch {}: {}", batchId, quarter);
                    continue;
                }
                String date = quarter.substring(0, 8);
                String time = quarter.substring(8);

                String orcBasePath = rawFilesPath;
                logger.info("Batch {} - processing quarter {} => date={}, time={}, basePath={}", batchId, quarter, date, time, orcBasePath);

                Dataset<Row> orcDF = batchDF.filter(functions.col("quarter").equalTo(quarter))
                        .selectExpr("CAST(value AS STRING) AS rawData")
                        .withColumn("date", functions.lit(date))
                        .withColumn("time", functions.lit(time));

                if (orcDF.isEmpty()) {
                    logger.info("No rows for quarter {} in batch {}", quarter, batchId);
                    continue;
                }

                try {
                    orcDF.write()
                            .format("orc")
                            .mode(SaveMode.Append)
                            .partitionBy("date", "time")
                            .option("compression", "snappy")
                            .option("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol") // Suggestion: Remove It
                            .option("spark.sql.orc.output.committer.class", "org.apache.hadoop.fs.s3a.commit.S3ACommitter")
                            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
                            .option("spark.hadoop.fs.s3a.committer.name", "magic")
                            .option("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
                            .save(orcBasePath);

                    logger.info("GENERAL SUCCESSFULLY WROTE quarter={} (date={},time={}) to {}", quarter, date, time, orcBasePath);

                } catch (Exception e) {
                    logger.error("GENERAL FAILED WRITING quarter={} (date={},time={}) to {}. Failing batch to allow retry.", quarter, date, time, orcBasePath, e);
                    throw new RuntimeException("Failed to write ORC for quarter " + quarter, e);
                }
            }

            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            logger.info("GENERAL BATCH ID {} PROCESSED IN {} MS", batchId, durationMs);
        };
    }

    private Trigger getTrigger(String trigger, String interval) {
        return "ONCE".equalsIgnoreCase(trigger) ? Trigger.Once()
                : Trigger.ProcessingTime(Utils.hasValidValue(interval) ? Long.parseLong(interval) : 5L,
                        TimeUnit.SECONDS);
    }

    private OutputMode getOutPutMode(String outputMode) {
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
}
