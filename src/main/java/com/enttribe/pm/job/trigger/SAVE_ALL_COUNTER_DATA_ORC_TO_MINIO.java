package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCReadCustom;
import com.enttribe.pm.job.quarterly.common.ORCWriteForTrino;
import com.enttribe.sparkrunner.context.JobContext;

public class SAVE_ALL_COUNTER_DATA_ORC_TO_MINIO {

    private static final Logger logger = LoggerFactory.getLogger(SAVE_ALL_COUNTER_DATA_ORC_TO_MINIO.class);

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        /*
         * READ NETWORK ELEMENT DATA FOR ORC WRITE
         */

        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

        String DOMAIN = jobContext.getParameter("DOMAIN");
        String VENDOR = jobContext.getParameter("VENDOR");
        String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");

        String NETWORK_ELEMENT_QUERY = "SELECT NE_ID AS neid, NE_NAME AS nename, PM_EMS_ID AS pmemsid FROM NETWORK_ELEMENT WHERE DOMAIN = '"
                + DOMAIN + "' AND VENDOR = '" + VENDOR + "' AND TECHNOLOGY = '" + TECHNOLOGY
                + "' AND DELETED = 0 AND PM_EMS_ID IS NOT NULL";

        dataFrame = new JDBCReadCustom(dataFrame, 1, "READ NETWORK ELEMENT DATA FOR ORC WRITE", SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD, NETWORK_ELEMENT_QUERY, "NEDetail",
                null, null, null, null, null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 READ NETWORK ELEMENT DATA FOR ORC WRITE Executed Successfully! ✅");

        /**
         * JOIN ALL COUNTER DATA AND NETWORK ELEMENT BY PM_EMS_ID
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 2, "NE JOINED BY PMEMSID",
                "SELECT agr.rowKey AS finalKey, agr.Date, agr.Time, agr.DateKey, agr.HourKey, agr.QuarterKey, agr.PT AS PTime, agr.pmemsid, ne.nename, ne.neid, agr.categoryName, agr.rawcounters, agr.interface_name, agr.fiveminutekey, agr.frequency FROM (SELECT * FROM parseAllCounter WHERE SIZE(rawcounters) != 0) agr JOIN (SELECT DISTINCT pmemsid, nename, neid FROM NEDetail) ne ON UPPER(agr.pmemsid) = UPPER(ne.pmemsid)",
                "AllCounterData", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 NE JOINED BY PMEMSID Executed Successfully! ✅");

        /*
         * ALL COUNTER ORC WRITE FOR TRINO
         */

        String KPI_ORC_PATH = jobContext.getParameter("KPI_ORC_PATH");
        dataFrame = new ORCWriteForTrino(dataFrame, 3, "ALL COUNTER ORC WRITE FOR TRINO",
                "domain vendor emstype technology date time ptime categoryname", "snappy", KPI_ORC_PATH, "Overwrite",
                "200").executeAndGetResultDataframe(jobContext);

        // dataFrame.show(5);
        logger.info("🚀 ALL COUNTER ORC WRITE FOR TRINO Executed Successfully! ✅");

        return dataFrame;
    }
}
