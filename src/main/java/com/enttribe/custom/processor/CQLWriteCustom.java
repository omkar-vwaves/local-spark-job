package com.enttribe.custom.processor;

import com.enttribe.sparkrunner.processors.CQLWriteCustom1;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CQLWriteCustom extends CQLWriteCustom1 {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CQLWriteCustom.class);

    public CQLWriteCustom() {
        super();
        logger.info("CQLWriteCustom: Default Constructor Called");
    }

    public CQLWriteCustom(Dataset<Row> dataFrame, int processorId, String processorName, String tableName,
                                 String keyspace, String saveMode, String host, String port, String username,
                                 String password, String datacenter, String jsoncolumnmapping) {
        super(processorId, processorName, tableName, keyspace, saveMode, host, port, username, password, datacenter, jsoncolumnmapping);
        this.dataFrame = dataFrame;
        logger.info("CQLWriteCustom: Parameterized Constructor Called with tableName: {}, keyspace: {}", tableName, keyspace);
    }

    public CQLWriteCustom(Dataset<Row> dataFrame, int processorId, String processorName, String tableName,
                                 String keyspace, String saveMode, String host, String port, String username,
                                 String password, String outputMode, String checkpointLocation, String datacenter,
                                 String jsoncolumnmapping) {
        super(processorId, processorName, tableName, keyspace, saveMode, host, port, username, password, outputMode, checkpointLocation, datacenter, jsoncolumnmapping);
        this.dataFrame = dataFrame;
        logger.info("CQLWriteCustom: Full Parameterized Constructor Called with tableName: {}, outputMode: {}", tableName, outputMode);
    }
}
