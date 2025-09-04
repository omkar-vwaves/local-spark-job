package com.enttribe.pm.job.quarterly.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

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
        logger.info("ORCWriteForTrino: Default Constructor Called");
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

        logger.info(
                "ORCWriteForTrino Initialized with Parameters - ProcessorId: {}, ProcessorName: {}, PartitionBy: {}, CompressionType: {}, Path: {}, SaveMode: {}, NumPartition: {}",
                processorId, processorName, this.partitionBy, this.compressionType, this.path, this.saveMode,
                this.numPartition);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) {

        logger.info("ORCWriteForTrino Execution Started :: ");

        try {

            String domain = jobContext.getParameter("DOMAIN");
            String vendor = jobContext.getParameter("VENDOR");
            String technology = jobContext.getParameter("TECHNOLOGY");
            String emsType = jobContext.getParameter("emsType");
            Integer sequenceno = Integer.valueOf(jobContext.getParameter("sequenceno"));

            logger.info("Received Parameters - Domain: {}, Vendor: {}, Technology: {}, EmsType: {}, SequenceNo: {}",
                    domain, vendor, technology, emsType, sequenceno);

            StringBuilder mapQuery = new StringBuilder();
            for (int i = 1; i <= sequenceno; i++) {
                mapQuery.append("rawcounters['").append(i).append("'] AS `C").append(i).append("`, ");
            }

            String mapSelect = StringUtils.substringBeforeLast(mapQuery.toString(), ",");

            // String query = "SELECT finalKey, DateKey, HourKey, QuarterKey, fiveminutekey,
            // pmemsid, nename, neid, interface_name AS interfacename, "
            // + mapSelect + " , frequency, '"
            // + domain + "' AS domain, '" + vendor + "' AS vendor, '" + emsType + "' AS
            // emstype, '" + technology
            // + "' AS technology, date, time, ptime, categoryname FROM AllCounterData ";

            // As Per Trino Table Schema
            String query = "SELECT finalKey, DateKey, HourKey, QuarterKey, pmemsid, nename, neid, "
                    + mapSelect + " , fiveminutekey, interface_name AS interfacename,  frequency, '"
                    + domain + "' AS domain, '" + vendor + "' AS vendor, '" + emsType + "' AS emstype, '" + technology
                    + "' AS technology, date, time, ptime, categoryname FROM AllCounterData ";

            logger.info("Generated ORCWriteForTrino Query: {}", query);

            Dataset<Row> finalDataFrame = this.dataFrame.sqlContext().sql(query);
            finalDataFrame.persist(StorageLevel.MEMORY_AND_DISK());
            finalDataFrame.createOrReplaceTempView("ORCTemp");
            String counterPath = StringUtils.substringBeforeLast(this.path,
                    "parquefile");
            this.path = counterPath;

            logger.info("Adjusted Output Path: {}", this.path);

            if (partitionBy != null && !partitionBy.trim().isEmpty() &&
                    this.saveMode != null && !this.saveMode.trim().isEmpty()) {

                String[] colName = partitionBy.split(" ");
                Column[] col = new Column[colName.length];
                for (int i = 0; i < colName.length; i++) {
                    col[i] = new Column(colName[i]);

                }

                finalDataFrame = finalDataFrame.repartition(Integer.valueOf(200), col);
                logger.error("Repartitioned finalDataFrame with {} Partitions on Columns: {}", 200,
                        Arrays.toString(colName));

                logger.info("ORCWriteForTrino: DataFrame View (Showing 5 Rows):: ");
                
                finalDataFrame.show();

                finalDataFrame.write()
                        .mode(this.saveMode)
                        .partitionBy(colName)
                        .orc(this.path);

                logger.error("ORCWriteForTrino: Data Successfully Written to Path: {}", this.path);
            } else {
                logger.info(
                        "PartitionBy or SaveMode is not properly configured. Skipping ORCWriteForTrino Operation.");
            }

            if (this.dataFrame != null) {
                long count = this.dataFrame.count();
                logger.info("ORCWriteForTrino - Result Row Count Size : {}", count);
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
            list.add("Exception in ParquetWrite Processor Name : " + this.processorName +
                    " : Path is : " + this.path + " that must not be " + this.path);
        }
        return list;
    }
}