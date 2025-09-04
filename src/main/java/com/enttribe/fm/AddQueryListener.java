package com.enttribe.fm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.enttribe.fms.mysql.utils.CustomStreamingConfig;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

public class AddQueryListener extends Processor {

    private static final long serialVersionUID = -8694641653078244226L;
    private static final Logger logger = LogManager.getLogger(AddQueryListener.class);

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        try {
            logger.info("AddQueryListener Execution Started!");
            CustomStreamingConfig.executeListener(this.dataFrame.sparkSession());
            logger.info("AddQueryListener Executed Successfully!");
        } catch (Exception e) {
            logger.error("Exception in AddQueryListener, Message: {} | Error: {}", e.getMessage(), e);
        }
        return this.dataFrame;
    }
}