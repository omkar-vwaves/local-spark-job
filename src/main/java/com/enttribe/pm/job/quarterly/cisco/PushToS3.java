package com.enttribe.pm.job.quarterly.cisco;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

public class PushToS3 extends Processor {
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

    private static final Logger logger = LoggerFactory.getLogger(PushToS3.class);

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("Push To S3 Execution Started!");
        dataFrame = transformDataFrame(dataFrame);

        Map<String, String> optionsMap = prepareCheckpointOptions(jobContext);
        StreamingQuery query = dataFrame.writeStream()
                .foreachBatch(createBatchFunction())
                .options(optionsMap)
                .trigger(getTrigger(this.triggerType, this.batchInterval))
                .outputMode(getOutPutMode(this.outputMode))
                .start();
        query.awaitTermination();

        logger.info("Push To S3 Execution Completed!");
        return dataFrame;
    }

    private Dataset<Row> transformDataFrame(Dataset<Row> df) {
        return df.withColumn("parsed_json",
                functions.from_json(functions.col("value").cast("string"),
                        new StructType().add("timestamp", DataTypes.LongType)))
                .filter("parsed_json IS NOT NULL AND parsed_json.timestamp IS NOT NULL")
                .selectExpr(
                        "CAST(value AS STRING) as value",
                        "DATE_FORMAT(to_utc_timestamp(FROM_UNIXTIME(FLOOR(parsed_json.timestamp / 900) * 900), 'UTC'), 'yyyyMMddHHmm') as quarter");
    }

    private Map<String, String> prepareCheckpointOptions(JobContext jobContext) {
        Map<String, String> options = new HashMap<>();
        if (Utils.hasValidValue(this.checkPointLoc)) {
            String topicName = this.topic;
            String checkpointLocation = SparkRunnerUtils.getCheckpointLocation(topicName,
                    this.checkPointLoc);
            options.put("checkpointLocation", checkpointLocation);
        }
        return options;
    }

    private VoidFunction2<Dataset<Row>, Long> createBatchFunction() {
        return (batchDF, batchId) -> {
            long startTime = System.currentTimeMillis();
            logger.info("Processing Batch ID: {}", batchId);

            batchDF.createOrReplaceTempView("temp_table");

            Dataset<Row> aggregated = batchDF.sqlContext().sql(
                    "SELECT quarter, collect_list(value) AS content FROM temp_table GROUP BY quarter");

            List<Row> rows = aggregated.repartition(1).collectAsList();
            for (Row row : rows) {
                String quarter = row.getAs("quarter");
                List<String> contentList = row.getList(row.fieldIndex("content"));
                if (contentList == null || contentList.isEmpty())
                    continue;

                byte[] jsonBytes = String.join("", contentList).getBytes();

                String date = quarter.substring(0, 8);
                String time = quarter.substring(8);
                String directoryPath = rawFilesPath + String.format("%s/%s/", date, time);
                String filename = String.format("%s/%s/%d_%s.json.zip", date, time, batchId, quarter);
                String s3Path = rawFilesPath + filename;

                logger.info("Writing To: {}", s3Path);
                logger.info("==========[Cleaning Existing Files and Writing New File]==========");

                try (FileSystem fs = FileSystem.get(new URI(s3Path), getHadoopConfig())) {
                    deleteExistingFiles(fs, directoryPath);
                    
                    try (FSDataOutputStream out = fs.create(new Path(s3Path), true);
                        ZipOutputStream zipOut = new ZipOutputStream(out)) {

                        zipOut.putNextEntry(new ZipEntry(quarter + ".json"));
                        zipOut.write(jsonBytes);
                        zipOut.closeEntry();

                        logger.info("Successfully Wrote: {}", s3Path);
                    }

                } catch (IOException e) {
                    logger.error("Failed Writing Zip To: " + s3Path, e);
                } catch (Exception e) {
                    logger.error("Failed processing for path: " + s3Path, e);
                }
            }

            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            logger.info("Batch ID {} Processed In {} ms", batchId, durationMs);
        };
    }

    private void deleteExistingFiles(FileSystem fs, String directoryPath) throws IOException {
        Path dirPath = new Path(directoryPath);
        
        if (fs.exists(dirPath)) {
            logger.info("Checking For Existing Files in Directory: {}", directoryPath);
            FileStatus[] fileStatuses = fs.listStatus(dirPath);
            
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".json.zip")) {
                    logger.info("Deleting Existing File: {}", fileStatus.getPath());
                    boolean deleted = fs.delete(fileStatus.getPath(), false);
                    if (deleted) {
                        logger.info("Successfully Deleted: {}", fileStatus.getPath());
                    } else {
                        logger.warn("Failed to Delete: {}", fileStatus.getPath());
                    }
                }
            }
        } else {
            logger.info("Directory Does Not Exist, Will Be Created: {}", directoryPath);
        }
    }

    private Configuration getHadoopConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", this.minioAccessKey);
        conf.set("fs.s3a.secret.key", this.minioSecretKey);
        conf.set("fs.s3a.endpoint", this.minioEndpoint);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        return conf;
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