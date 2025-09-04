package com.enttribe.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Utils;

public class KafkaShow extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaShow.class);

    public String noOfRows;
    public String truncate;

    public KafkaShow() {
        logger.info("Initialized KafkaShow with noOfRows: {} and truncate: {}", noOfRows, truncate);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("KafkaShow Processor Started: {}", this.processorName);

        StreamingQuery query = this.dataFrame
                .writeStream()
                .format("console")
                .start();

        logger.info("Streaming Query Started. Awaiting Termination...");

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            logger.error("Streaming Query Failed: {}", e.getMessage(), e);
        }

        logger.info("KafkaShow Execution Completed.");
        return this.dataFrame;
    }

    @Override
    public List<String> validProcessorConfigurations() {
        List<String> list = new ArrayList<>();
        String commonException = "Exception in KafkaShow Processor Named: " + this.processorName;
        if (!Utils.hasValidValue(this.noOfRows) && StringUtils.isNumeric(this.noOfRows)) {
            String errorMessage = commonException + " - No. of Rows is Invalid: [" + this.noOfRows + "]";
            logger.error(errorMessage);
            list.add(errorMessage);
        }

        if (!Utils.hasValidValue(this.truncate) && StringUtils.isNumeric(this.truncate)) {
            String errorMessage = commonException + " - truncate is invalid: [" + this.truncate + "]";
            logger.error(errorMessage);
            list.add(errorMessage);
        }
        logger.info("Validation Completed for Processor: {}. Errors: {}", this.processorName, list);
        return list;
    }
}