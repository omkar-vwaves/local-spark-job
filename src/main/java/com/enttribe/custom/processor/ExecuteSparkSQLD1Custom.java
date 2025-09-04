package com.enttribe.custom.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.processors.transformation.ExecuteSparkSQLD1;

public class ExecuteSparkSQLD1Custom extends ExecuteSparkSQLD1 {

    private static final Logger logger = LogManager.getLogger(ExecuteSparkSQLD1Custom.class);

    public ExecuteSparkSQLD1Custom() {
        super();
    }
    

    public ExecuteSparkSQLD1Custom(Dataset<Row> dataFrame, int processorId, String processorName, String sql, String tempTable, String contextParameter) {
        super(processorId, processorName, sql, tempTable, contextParameter);
        this.dataFrame = dataFrame;
    }
}
