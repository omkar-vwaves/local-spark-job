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

        String key1 = "RAW_FILE_COUNTER_NODE_AGGR_QUERY" + startIndex;
        String value1 = parameters.get(key1);
        logger.info("key1={}, value1={}", key1, value1);
        jobContext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", value1);

        String key2 = "FILTER_QUERY_FINAL" + startIndex;
        String value2 = parameters.get(key2);
        logger.info("key2={}, value2={}", key2, value2);
        jobContext.setParameters("FILTER_QUERY_FINAL", value2);

        String key3 = "COUNTER_NODE_AGGR_QUERY" + startIndex;
        String value3 = parameters.get(key3);
        logger.info("key3={}, value3={}", key3, value3);
        jobContext.setParameters("COUNTER_NODE_AGGR_QUERY", value3);

        String key4 = "COUNTER_TIME_AGGR_QUERY" + startIndex;
        String value4 = parameters.get(key4);
        logger.info("key4={}, value4={}", key4, value4);
        jobContext.setParameters("COUNTER_TIME_AGGR_QUERY", value4);
 
        String key5 = "COUNTER_MAP_QUERY" + startIndex;
        String value5 = parameters.get(key5);
        logger.info("key5={}, value5={}", key5, value5);
        jobContext.setParameters("COUNTER_MAP_QUERY", value5);

        logger.info("Updated JobContext: {}", jobContext.getParameters());
        return this.dataFrame;
    }
}