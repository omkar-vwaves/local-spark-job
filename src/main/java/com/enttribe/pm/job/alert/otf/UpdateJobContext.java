package com.enttribe.pm.job.alert.otf;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

public class UpdateJobContext extends Processor {

    public UpdateJobContext() {
        super();
    }

    public UpdateJobContext(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("Received JobContext: {}", jobContext.getParameters());
        Map<String, String> parameters = jobContext.getParameters();
        String startIndex = parameters.get("START_INDEX");
        logger.info("Current Start Index: {}", startIndex);
        String key = "FILTER_LEVEL" + startIndex;
        String filterLevel = parameters.get(key);
        logger.info("Current Filter Level: {}", filterLevel);
        jobContext.setParameters("CURRENT_FILTER_LEVEL", filterLevel);
        logger.info("Updated JobContext: {}", jobContext.getParameters());
        return this.dataFrame;
    }
}