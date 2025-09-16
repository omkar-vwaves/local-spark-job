package com.enttribe.pm.job.fiveminute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

public class ORCWriteForTrino extends Processor {

    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ORCWriteForTrino.class);
    public String compressionType;
    public String partitionBy;

    @Dynamic
    public String path;
    public String saveMode;
    public String numPartition;

    static long startTime;
    static {
        startTime = System.currentTimeMillis();
    }

    public ORCWriteForTrino() {
        super();
    }

    public ORCWriteForTrino(Dataset<Row> dataframe, int processorId, String processorName, String partitionBy,
            String compressionType,
            String path, String saveMode, String numPartition) {
        super(processorId, processorName);
        this.dataFrame = dataframe;
        this.compressionType = compressionType;
        this.partitionBy = partitionBy.toLowerCase();
        this.path = path.toLowerCase();
        this.saveMode = saveMode;
        this.numPartition = numPartition;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) {

        logger.info("ORCWriteForTrino Execution Started!");

        try {

            String domain = jobContext.getParameter("DOMAIN");
            String vendor = jobContext.getParameter("VENDOR");
            String technology = jobContext.getParameter("TECHNOLOGY");
            String emsType = jobContext.getParameter("EMS_TYPE");
            Integer sequenceno = Integer.valueOf(jobContext.getParameter("sequenceno"));

            logger.info("Received Parameters - Domain: {}, Vendor: {}, Technology: {}, EmsType: {}, SequenceNo: {}",
                    domain, vendor, technology, emsType, sequenceno);

            StringBuilder mapQuery = new StringBuilder();
            for (int i = 1; i <= 50; i++) {
                mapQuery.append("rawcounters['").append(i).append("'] AS `C").append(i).append("`, ");
            }

            String mapSelect = StringUtils.substringBeforeLast(mapQuery.toString(), ",");

            String query = "SELECT finalKey, DateKey, HourKey, QuarterKey, fiveminutekey, pmemsid, nename, neid, interface_name AS interfacename, "
                    + mapSelect + " , frequency, '" + domain + "' AS domain, '" + vendor + "' AS vendor, '" + emsType
                    + "' AS emstype, '" + technology
                    + "' AS technology, date, time, ptime, categoryname FROM AllCounterData ";

            logger.info("Generated ORCWriteForTrino Query: {}", query);

            Dataset<Row> finalDataFrame = this.dataFrame.sqlContext().sql(query);
            String counterPath = StringUtils.substringBeforeLast(this.path,
                    "parquefile");
            this.path = counterPath;

            logger.info("Adjusted Output Path: {}", this.path);

            if (partitionBy != null && !partitionBy.trim().isEmpty() &&
                    this.saveMode != null && !this.saveMode.trim().isEmpty()) {

                deleteExistingPartitionFiles(jobContext, finalDataFrame, partitionBy);

                String[] colName = partitionBy.split(" ");
                Column[] col = new Column[colName.length];
                for (int i = 0; i < colName.length; i++) {
                    col[i] = new Column(colName[i]);

                }

                finalDataFrame = finalDataFrame.repartition(Integer.valueOf(200), col);
                logger.info("Repartitioned finalDataFrame with {} Partitions on Columns: {}", 200,
                        Arrays.toString(colName));

                jobContext.sqlctx().setConf("spark.sql.sources.partitionOverwriteMode", "dynamic");
                finalDataFrame.write()
                        .option("spark.hadoop.fs.s3a.committer.name", "directory")
                        .option("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
                        .option("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")
                        .option("spark.sql.sources.partitionOverwriteMode", "dynamic")
                        .mode(SaveMode.Overwrite)
                        .partitionBy(colName)
                        .orc(this.path);

                logger.info("Data Successfully Written to Path: {}", this.path);
            }

        } catch (Exception e) {
            logger.error("Exception Occurred During ORCWriteForTrino Execution: {}", e.getMessage(), e);
        }

        long endTime = System.currentTimeMillis();
        logger.info("ORCWriteForTrino Execution Completed : {} Milliseconds", endTime - startTime);

        return this.dataFrame;
    }

    @Override
    public List<String> validProcessorConfigurations() {
        List<String> list = new ArrayList<>();
        if (this.path == null || this.path.trim().isEmpty()) {
            list.add("Exception in ORCWriteForTrino Processor Name : " + this.processorName +
                    " : Path is : " + this.path + " that must not be " + this.path);
        }
        return list;
    }

    private void deleteExistingPartitionFiles(JobContext jobContext, Dataset<Row> dataframe, String partitionBy) {
        try {
            String[] colName = partitionBy.split(" ");

            Dataset<Row> distinctPartitions = dataframe.selectExpr(colName).distinct();
            List<Row> partitionRows = distinctPartitions.collectAsList();

            Path basePath = new Path(this.path);
            FileSystem fs = basePath.getFileSystem(jobContext.sqlctx().sparkContext().hadoopConfiguration());

            for (Row partitionRow : partitionRows) {
                StringBuilder partitionPath = new StringBuilder(this.path);

                for (int i = 0; i < colName.length; i++) {
                    String colValue = partitionRow.get(i) != null ? partitionRow.get(i).toString() : "null";
                    partitionPath.append("/").append(colName[i]).append("=").append(colValue);
                }

                Path partitionDir = new Path(partitionPath.toString());

                if (fs.exists(partitionDir)) {
                    logger.info("Deleting Existing Partition Directory: {}", partitionDir);
                    fs.delete(partitionDir, true);
                }
            }

            logger.info("Successfully Deleted Existing Partition Files for Overwrite");

        } catch (Exception e) {
            logger.info("Failed to Delete Existing Partition Files: {}", e.getMessage());
        }
    }
}
