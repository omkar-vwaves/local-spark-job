package com.enttribe.custom.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.processors.dataset.jdbc.JDBCUpdate;

public class JDBCUpdateCustom extends JDBCUpdate {

    private static final Logger logger = LogManager.getLogger(JDBCUpdateCustom.class);

    public JDBCUpdateCustom() {
        super();
    }

    public JDBCUpdateCustom(Dataset<Row> dataFrame,
                             int processorId,
                             String processorName,
                             String driverName,
                             String dbURL,
                             String userName,
                             String password,
                             String query,
                             String updateColumns,
                             String batchLimit,
                             String contextParameter,
                             String dateFormat) {
        super(processorId, processorName, driverName, dbURL, userName, password, query, updateColumns, batchLimit, contextParameter, dateFormat);
        this.dataFrame = dataFrame;
    }

    public JDBCUpdateCustom(Dataset<Row> dataFrame,
                             int processorId,
                             String processorName,
                             String driverName,
                             String dbURL,
                             String userName,
                             String password,
                             String query,
                             String updateColumns,
                             String checkpointLocation,
                             String outputMode,
                             String contextParameter,
                             String dateFormat) {
        super(processorId, processorName, driverName, dbURL, userName, password, query, updateColumns, checkpointLocation, outputMode, contextParameter, dateFormat);
        this.dataFrame = dataFrame;
    }
}
