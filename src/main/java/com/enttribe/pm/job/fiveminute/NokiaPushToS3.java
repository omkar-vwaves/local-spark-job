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

public class NokiaPushToS3 extends Processor {
    private static final long serialVersionUID = 1L;

    public String checkPointLoc;
    public String outputMode = "Overwrite";
    public String triggerType = "Processing Time";
    public String batchInterval;
    public String topic;
    public String rawFilesPath;
    public String minioAccessKey;
    public String minioSecretKey;
    public String minioEndpoint;

    private static final Logger logger = LoggerFactory.getLogger(NokiaPushToS3.class);

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("====NOKIA PUSH TO S3 STARTED====");
        dataFrame = transformDataFrame(dataFrame);

        Map<String, String> optionsMap = prepareCheckpointOptions(jobContext);
        StreamingQuery query = dataFrame.writeStream()
                .foreachBatch(createBatchFunction())
                .options(optionsMap)
                .trigger(getTrigger(this.triggerType, this.batchInterval))
                .outputMode(getOutPutMode(this.outputMode))
                .start();
        query.awaitTermination();

        logger.info("====NOKIA PUSH TO S3 COMPLETED====");
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
            logger.info("NOKIA TOPICS ==> {} AND CHECKPOINT LOCATION ==> {}", topicName, checkpointLocation);
            options.put("checkpointLocation", checkpointLocation);
        }
        return options;
    }

    private VoidFunction2<Dataset<Row>, Long> createBatchFunction() {
        return (batchDF, batchId) -> {
            long startTime = System.currentTimeMillis();
            logger.info("NOKIA PROCESSING BATCH ID ==> {}", batchId);

            batchDF.createOrReplaceTempView("temp_table");

            Dataset<Row> aggregated = batchDF.sqlContext().sql(
                    "SELECT quarter, collect_list(value) AS content FROM temp_table GROUP BY quarter");

            List<Row> rows = aggregated.repartition(1).collectAsList();
            for (Row row : rows) {
                String quarter = row.getAs("quarter");
                List<String> contentList = row.getList(row.fieldIndex("content"));
                if (contentList == null || contentList.isEmpty())
                    continue;
                Dataset<Row> orcDF = batchDF.sqlContext()
                        .createDataset(contentList, org.apache.spark.sql.Encoders.STRING()).toDF("rawData");
                String date = quarter.substring(0, 8);
                String time = quarter.substring(8);
                String orcPath = rawFilesPath + String.format("%s/%s", date, time);

                logger.info("NOKIA WRITING ORC TO ==> {}", orcPath);

                try {
                    orcDF.write()
                            .format("orc")
                            .mode(SaveMode.Overwrite)
                            .option("compression", "snappy")
                            .option("spark.sql.sources.commitProtocolClass",
                                    "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol")
                            .option("spark.sql.orc.output.committer.class",
                                    "org.apache.hadoop.fs.s3a.commit.S3ACommitter")
                            .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
                            .option("spark.hadoop.fs.s3a.committer.name", "magic")
                            .option("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
                            .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
                            .save(orcPath);

                    logger.info("NOKIA SUCCESSFULLY WROTE ORC TO ==> {}", orcPath);

                } catch (Exception e) {
                    logger.error("NOKIA FAILED WRITING ORC TO ==> {}", orcPath, e);
                }
            }

            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            logger.info("NOKIA BATCH ID {} PROCESSED IN {} MS", batchId, durationMs);
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
