package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.ORCRead;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.sparkrunner.processors.util.UpdateParameterInJobContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class READ_15MIN_ORC_FILES_FROM_S3_SUBFLOW {

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        final Logger logger = LoggerFactory.getLogger(READ_15MIN_ORC_FILES_FROM_S3_SUBFLOW.class);

        String KPI_ORC_PATH = jobContext.getParameter("KPI_ORC_PATH");
        String CQL_TABLE_NAME = jobContext.getParameter("CQL_TABLE_NAME");
        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");
        String DOMAIN = jobContext.getParameter("DOMAIN");
        String VENDOR = jobContext.getParameter("VENDOR");
        String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");
        String NE_TYPES = jobContext.getParameter("NE_TYPES");
        String NE_TYPES_ESCAPED = NE_TYPES == null ? "" : NE_TYPES.replace("'", "''");
        String NODES = jobContext.getParameter("NODES");
        String EMS_TYPE = jobContext.getParameter("EMS_TYPE");
        String SPARK_MINIO_ENDPOINT_URL = jobContext.getParameter("SPARK_MINIO_ENDPOINT_URL");
        String SPARK_MINIO_ACCESS_KEY = jobContext.getParameter("SPARK_MINIO_ACCESS_KEY");
        String SPARK_MINIO_SECRET_KEY = jobContext.getParameter("SPARK_MINIO_SECRET_KEY");
        String SPARK_MINIO_BUCKET_NAME_PM = jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM");

        String SPARK_PM_JDBC_QUERY = "SELECT '${KPI_ORC_PATH}' AS KPI_ORC_PATH, '${CQL_TABLE_NAME}' AS CQL_TABLE_NAME, '${SPARK_PM_JDBC_DRIVER}' AS SPARK_PM_JDBC_DRIVER, '${SPARK_PM_JDBC_URL}' AS SPARK_PM_JDBC_URL, '${SPARK_PM_JDBC_USERNAME}' AS SPARK_PM_JDBC_USERNAME, '${SPARK_PM_JDBC_PASSWORD}' AS SPARK_PM_JDBC_PASSWORD, '${DOMAIN}' AS DOMAIN, '${VENDOR}' AS VENDOR, '${TECHNOLOGY}' AS TECHNOLOGY, '${NE_TYPES}' AS NE_TYPES, '${NODES}' AS NODES, '${EMS_TYPE}' AS EMS_TYPE, '${SPARK_MINIO_ENDPOINT_URL}' AS SPARK_MINIO_ENDPOINT_URL, '${SPARK_MINIO_ACCESS_KEY}' AS SPARK_MINIO_ACCESS_KEY, '${SPARK_MINIO_SECRET_KEY}' AS SPARK_MINIO_SECRET_KEY, '${SPARK_MINIO_BUCKET_NAME_PM}' AS SPARK_MINIO_BUCKET_NAME_PM";

        SPARK_PM_JDBC_QUERY += ", NETWORK_TYPE, GROUP_CONCAT(CONCAT(NETYPE, '##', CONCAT( 'RowKeyAppender@', COALESCE(ROWKEY_TECHNOLOGY, ''), IF( (ROWKEY_TECHNOLOGY IS NOT NULL), '_', '' ), COALESCE(NETWORK_TYPE, '') ) ) SEPARATOR 'LINE' ) AS ROW_KEY_APPENDER FROM PM_NODE_VENDOR WHERE DOMAIN = '${DOMAIN}' AND VENDOR = '${VENDOR}' AND TECHNOLOGY = '${TECHNOLOGY}' AND NETYPE IS NOT NULL GROUP BY ROWKEY_TECHNOLOGY, NETWORK_TYPE";

        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${KPI_ORC_PATH}", KPI_ORC_PATH);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${CQL_TABLE_NAME}", CQL_TABLE_NAME);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_PM_JDBC_DRIVER}", SPARK_PM_JDBC_DRIVER);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_PM_JDBC_URL}", SPARK_PM_JDBC_URL);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_PM_JDBC_USERNAME}", SPARK_PM_JDBC_USERNAME);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_PM_JDBC_PASSWORD}", SPARK_PM_JDBC_PASSWORD);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${DOMAIN}", DOMAIN);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${VENDOR}", VENDOR);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${TECHNOLOGY}", TECHNOLOGY);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${NE_TYPES}", NE_TYPES_ESCAPED);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${NODES}", NODES);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${EMS_TYPE}", EMS_TYPE);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_MINIO_ENDPOINT_URL}", SPARK_MINIO_ENDPOINT_URL);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_MINIO_ACCESS_KEY}", SPARK_MINIO_ACCESS_KEY);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_MINIO_SECRET_KEY}", SPARK_MINIO_SECRET_KEY);
        SPARK_PM_JDBC_QUERY = SPARK_PM_JDBC_QUERY.replace("${SPARK_MINIO_BUCKET_NAME_PM}", SPARK_MINIO_BUCKET_NAME_PM);

        logger.info("🚀 SPARK_PM_JDBC_QUERY JOB CONTEXT: {}", SPARK_PM_JDBC_QUERY);

        dataFrame = new UpdateParameterInJobContext(
                1,
                "UPDATE PARAMETER IN JOB CONTEXT",
                SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL,
                SPARK_PM_JDBC_USERNAME,
                SPARK_PM_JDBC_PASSWORD,
                SPARK_PM_JDBC_QUERY,
                null,
                "JobContext").executeAndGetResultDataframe(jobContext);

        // dataFrame.show(false);
        logger.info("🚀 UPDATE JOB CONTEXT Executed Successfully! ✅");

        // GET COUNTER AND KPI DETAILS AND REGISTER UDF SUBFLOW
        dataFrame = GET_COUNT_AND_KPI_DETAILS_AND_REGISTER_HOURLY_UDF.callSubFlow(dataFrame, jobContext);

        // READ ORC FILES

        String directoryPath = jobContext.getParameter("FOUND_ORC_FILES");
        String inputSchema = jobContext.getParameter("ORC_COUNTERS_DATATYPE");
        String basePath = KPI_ORC_PATH;
        String tempTable = "OrcRead";

        ORCRead orcRead = new ORCRead(1, "READ ORC FILES", directoryPath, inputSchema, basePath, tempTable);
        dataFrame = orcRead.executeAndGetResultDataframe(jobContext);
        // dataFrame.show(5);
        logger.info("🚀 READ ORC FILES Executed Successfully! ✅");

        // REPARTITION DATAFRAME

        dataFrame = new RepartitionD1Custom(
                dataFrame,
                3,
                "REPARTITION DATAFRAME",
                "200",
                "finalKey",
                "orcTemp").executeAndGetResultDataframe(jobContext);

        // SEQUENCE TO COUNTER QUERY

        String SEQUENCE_TO_COUNTER_QUERY = jobContext.getParameter("SEQUENCE_TO_COUNTER_QUERY");

        dataFrame = new ExecuteSparkSQLD1Custom(
                dataFrame,
                4,
                "SEQUENCE TO COUNTER QUERY",
                SEQUENCE_TO_COUNTER_QUERY,
                "parseRawNodeData",
                null).executeAndGetResultDataframe(jobContext);

        // dataFrame.show(5);
        logger.info("🚀 SEQUENCE TO COUNTER QUERY Executed Successfully! ✅");

        return dataFrame;
    }
}
