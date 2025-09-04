package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCUpdateCustom;
import com.enttribe.sparkrunner.context.JobContext;

public class INSERT_RAW_FILE_COUNT_TO_PROCESSING_AUDIT {

    private static final Logger logger = LoggerFactory.getLogger(INSERT_RAW_FILE_COUNT_TO_PROCESSING_AUDIT.class);

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        /*
         * SELECT RAW FILE COUNT FOR PROCESSING AUDIT
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 1, "SELECT RAW FILE COUNT FOR PROCESSING AUDIT",
                "SELECT COUNT(fileName) AS RAW_FILE_COUNT FROM cacheRawBytesDataFrame",
                "rawFileCount", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 SELECT RAW FILE COUNT FOR PROCESSING AUDIT Executed Successfully! ✅");

        /*
         * UPDATE RAW FILE COUNT TO PROCESSING AUDIT / INSERT IF NOT EXISTS
         */

        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

        String DOMAIN = jobContext.getParameter("DOMAIN");
        String VENDOR = jobContext.getParameter("VENDOR");
        String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");
        String EMS_TYPE = jobContext.getParameter("EMS_TYPE");
        String MODULE = jobContext.getParameter("MODULE");
        String AUDIT_TIME = jobContext.getParameter("AUDIT_TIME");
        String RAW_FILE_COUNT = dataFrame.select("RAW_FILE_COUNT").collectAsList().get(0).toString();

        String RAW_FILE_COUNT_INSERT_QUERY = "INSERT INTO PROCESSING_AUDIT ( DOMAIN, VENDOR, TECHNOLOGY, EMS_TYPE, MODULE, AUDIT_TIME, RAW_FILE_COUNT ) VALUES ( '"
                + DOMAIN + "', '" + VENDOR + "', '" + TECHNOLOGY + "', '" + EMS_TYPE + "', '" + MODULE + "', '"
                + AUDIT_TIME + "', '" + RAW_FILE_COUNT
                + "' ) ON DUPLICATE KEY UPDATE RAW_FILE_COUNT = VALUES(RAW_FILE_COUNT)";

        logger.info("🚀 RAW_FILE_COUNT_INSERT_QUERY: " + RAW_FILE_COUNT_INSERT_QUERY);

        dataFrame = new JDBCUpdateCustom(
                dataFrame,
                2,
                "UPDATE RAW FILE COUNT TO PROCESSING AUDIT / INSERT IF NOT EXISTS",
                SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL,
                SPARK_PM_JDBC_USERNAME,
                SPARK_PM_JDBC_PASSWORD,
                RAW_FILE_COUNT_INSERT_QUERY,
                "RAW_FILE_COUNT",
                "Append",
                null,
                null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 UPDATE RAW FILE COUNT TO PROCESSING AUDIT / INSERT IF NOT EXISTS Executed Successfully! ✅");

        /*
         * RETURN DATAFRAME
         */

        return dataFrame;
    }
}