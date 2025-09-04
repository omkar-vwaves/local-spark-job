package com.enttribe.pm.job.quarterly.common;

import java.util.*;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.constants.SparkRunnerConstants;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Utils;

public class CustomParquetWriteForDI extends Processor {

    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomParquetWriteForDI.class);

    public String compressionType;
    public String partitionBy;

    @Dynamic
    public String path;
    public String saveMode;
    public String numPartition;

    public CustomParquetWriteForDI(Dataset<Row> dataFrame, int processorId, String processorName,
            String compressionType, String partitionBy, String path,
            String saveMode, String numPartition) {
        super(processorId, processorName);
        this.compressionType = compressionType;
        this.partitionBy = partitionBy;
        this.path = path;
        this.saveMode = saveMode;
        this.numPartition = numPartition;
        this.dataFrame = dataFrame;
        logger.info("CustomParquetWriteCustom: Parameterized Constructor Called with path: {}, saveMode: {}", path,
                saveMode);
    }

    static long startTime;
    static {
        startTime = System.currentTimeMillis();
    }

    public CustomParquetWriteForDI() {
        super();
        logger.info("Initialized CustomParquetWriteForDI with Default Constructor");
    }

    public CustomParquetWriteForDI(int processorId, String processorName, String partitionBy, String compressionType,
            String path, String saveMode, String numPartition) {
        super(processorId, processorName);
        this.compressionType = compressionType;
        this.partitionBy = partitionBy;
        this.path = path;
        this.saveMode = saveMode;
        this.numPartition = numPartition;

        logger.info(
                "Initialized CustomParquetWriteForDI with params - processorId: {}, processorName: {}, partitionBy: {}, compressionType: {}, path: {}, saveMode: {}, numPartition: {}",
                processorId, processorName, partitionBy, compressionType, path, saveMode, numPartition);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("CustomParquetWriteForDI Execution Started :: ");

        try {

            configureParquetCompression(jobContext);

            if (partitionBy != null && !partitionBy.trim().isEmpty() && saveMode != null
                    && !saveMode.trim().isEmpty()) {

                logger.info("Repartitioning And Writing Dataframe with partitionBy: {} and saveMode: {}", partitionBy,
                        saveMode);

                String[] columnNames = partitionBy.split(" ");
                Column[] columns = new Column[columnNames.length];

                for (int i = SparkRunnerConstants.ZERO_INT; i < columnNames.length; i++) {
                    columns[i] = new Column(columnNames[i]);
                }

                this.dataFrame = this.dataFrame.repartition(Integer.valueOf(this.numPartition), columns);

                this.dataFrame.write().mode(this.saveMode).partitionBy(columnNames).orc(this.path);

            } else if (Utils.hasValidValue(this.saveMode)) {

                logger.info("Writing Dataframe without Partitioning, Using saveMode: {}", saveMode);

                this.dataFrame.write().mode(this.saveMode).orc(this.path);

            } else {

                logger.info("Writing Dataframe Using Default saveMode: ErrorIfExists");

                this.dataFrame.write().mode(SaveMode.ErrorIfExists).orc(this.path);
            }

            if (this.dataFrame != null) {
                long count = this.dataFrame.count();
                logger.info("CustomParquetWriteForDI - Result Row Count Size : {}", count);
            }

        } catch (Exception e) {
            logger.error("Exception During Execution of CustomParquetWriteForDI processor: {} - {}", this.processorName,
                    e.getMessage(), e);
        }

        return this.dataFrame;
    }

    private void configureParquetCompression(JobContext jobContext) {

        Properties sparkProperties = new Properties();
        String codec = (compressionType != null && !compressionType.isBlank()) ? compressionType : "snappy";
        sparkProperties.put("spark.sql.parquet.compression.codec", codec);
        jobContext.sqlctx().setConf(sparkProperties);

        logger.info("Parquet Compression Codec Set to: {}", codec);
    }

    @Override
    public List<String> validProcessorConfigurations() {
        List<String> list = new ArrayList<>();
        if (this.path == null || this.path.trim().isEmpty()) {
            list.add("Exception in CustomParquetWriteForDI Processor Name : " + this.processorName + " : Path is : "
                    + this.path
                    + " that must not be " + this.path);
        }
        return list;
    }
}