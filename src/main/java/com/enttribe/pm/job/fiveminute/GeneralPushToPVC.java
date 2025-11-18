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
import com.enttribe.sparkrunner.util.Utils;

public class GeneralPushToPVC extends Processor {
    private static final long serialVersionUID = 1L;

    private static final String CHECKPOINT_LOCATION = "/opt/spark/data/streaming/";

    public String outputMode = "Append";
    public String triggerType = "Processing Time";
    public String batchInterval;
    public String topic;
    public String outputPath;

    private static final Logger logger = LoggerFactory.getLogger(GeneralPushToPVC.class);

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("==== GENERAL PUSH TO PVC STARTED ====");

        dataFrame = transformDataFrame(dataFrame);

        Map<String, String> optionsMap = prepareCheckpointOptions();

        StreamingQuery query = dataFrame.writeStream()
                .foreachBatch(createBatchFunction())
                .options(optionsMap)
                .trigger(getTrigger(this.triggerType, this.batchInterval))
                .outputMode(getOutputMode(this.outputMode))
                .start();

        query.awaitTermination();

        logger.info("==== GENERAL PUSH TO PVC COMPLETED ====");
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

    private Map<String, String> prepareCheckpointOptions() {
        Map<String, String> options = new HashMap<>();
        String topicName = (topic != null && !topic.isEmpty()) ? topic : "default_topic";
        String checkpointPath = CHECKPOINT_LOCATION + topicName;

        logger.info("Using checkpoint location on PVC: {}", checkpointPath);
        options.put("checkpointLocation", checkpointPath);
        return options;
    }

    private VoidFunction2<Dataset<Row>, Long> createBatchFunction() {
        return (batchDF, batchId) -> {
            long startTime = System.currentTimeMillis();
            logger.info("GENERAL PVC PROCESSING BATCH ID ==> {}", batchId);

            long incoming = batchDF.count();
            if (incoming == 0L) {
                logger.info("Batch {} has no data. Skipping.", batchId);
                return;
            }

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

                String pvcPath = outputPath;
                logger.info("Batch {} - processing quarter {} => date={}, time={}, basePath={}",
                        batchId, quarter, date, time, pvcPath);

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
                            .save(pvcPath);

                    logger.info("PVC WRITE SUCCESS - quarter={} (date={},time={}) -> {}", quarter, date, time, pvcPath);

                } catch (Exception e) {
                    logger.error("PVC WRITE FAILED - quarter={} (date={},time={}) -> {}", quarter, date, time, pvcPath, e);
                    throw new RuntimeException("Failed to write ORC for quarter " + quarter, e);
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("PVC BATCH {} COMPLETED in {} ms", batchId, duration);
        };
    }

    private Trigger getTrigger(String trigger, String interval) {
        return "ONCE".equalsIgnoreCase(trigger)
                ? Trigger.Once()
                : Trigger.ProcessingTime(Utils.hasValidValue(interval)
                        ? Long.parseLong(interval)
                        : 10L, TimeUnit.MILLISECONDS);
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
}
