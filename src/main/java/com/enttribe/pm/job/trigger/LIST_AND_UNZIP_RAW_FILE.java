package com.enttribe.pm.job.trigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.util.UpdateParameterInJobContext;
import com.enttribe.custom.processor.ListMINIOFilesCustomV1;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.custom.processor.CacheDatasetCustom;
import com.enttribe.pm.job.quarterly.common.UnzipToBytesForYB;

public class LIST_AND_UNZIP_RAW_FILE {

        public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

                final Logger logger = LoggerFactory.getLogger(LIST_AND_UNZIP_RAW_FILE.class);

                /*
                 * UPDATE PARAMETER IN JOB CONTEXT
                 */

                String SPARK_MINIO_ENDPOINT_URL = jobContext.getParameter("SPARK_MINIO_ENDPOINT_URL");
                String SPARK_MINIO_ACCESS_KEY = jobContext.getParameter("SPARK_MINIO_ACCESS_KEY");
                String SPARK_MINIO_SECRET_KEY = jobContext.getParameter("SPARK_MINIO_SECRET_KEY");
                String SPARK_MINIO_BUCKET_NAME_PM = jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM");

                String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");
                String TEMP_TABLE = "SQL_CREDENTIAL";

                String RAW_FILE_PATH = jobContext.getParameter("RAW_FILE_PATH");
                String RAW_FILE_PATH_SUFFIX = jobContext.getParameter("RAW_FILE_PATH_SUFFIX");

                String PREVIOUS_RAW_FILE_PATH = jobContext.getParameter("PREVIOUS_RAW_FILE_PATH");
                // String PREVIOUS_RAW_FILE_PATH_SUFFIX =
                // jobContext.getParameter("PREVIOUS_RAW_FILE_PATH_SUFFIX");

                RAW_FILE_PATH = RAW_FILE_PATH + RAW_FILE_PATH_SUFFIX;
                // PREVIOUS_RAW_FILE_PATH = PREVIOUS_RAW_FILE_PATH +
                // PREVIOUS_RAW_FILE_PATH_SUFFIX;
                PREVIOUS_RAW_FILE_PATH = RAW_FILE_PATH;
                String DOMAIN = jobContext.getParameter("DOMAIN");
                String VENDOR = jobContext.getParameter("VENDOR");
                String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");
                String NE_TYPE = jobContext.getParameter("NE_TYPE");
                NE_TYPE = "'" + NE_TYPE.replace("'", "''") + "'";
                String NODES = jobContext.getParameter("NODES");
                String EMS_TYPE = jobContext.getParameter("EMS_TYPE");

                String FROM_PM_NODE_VENDOR = "ROWKEY_TECHNOLOGY, NETWORK_TYPE, " +
                                "GROUP_CONCAT(CONCAT(NETYPE, '##', CONCAT('ROW_KEY_APPENDER@', COALESCE(ROWKEY_TECHNOLOGY, ''), "
                                +
                                "IF((ROWKEY_TECHNOLOGY IS NOT NULL), '_', ''), COALESCE(NETWORK_TYPE, ''))) SEPARATOR 'LINE') AS ROW_KEY_APPENDER "
                                +
                                "FROM PM_NODE_VENDOR WHERE DOMAIN = '" + DOMAIN + "' AND VENDOR = '" + VENDOR +
                                "' AND TECHNOLOGY = '" + TECHNOLOGY + "' AND NETYPE IS NOT NULL " +
                                "GROUP BY ROWKEY_TECHNOLOGY, NETWORK_TYPE";

                String SPARK_PM_JDBC_QUERY = "SELECT '" + RAW_FILE_PATH + "' AS RAW_FILE_PATH, '"
                                + PREVIOUS_RAW_FILE_PATH + "' AS PREVIOUS_RAW_FILE_PATH, '" + DOMAIN + "' AS DOMAIN, '"
                                + VENDOR + "' AS VENDOR, '" + TECHNOLOGY + "' AS TECHNOLOGY, " + NE_TYPE
                                + " AS NE_TYPE, '" + NODES
                                + "' AS NODES, '" + EMS_TYPE + "' AS EMS_TYPE, '" + SPARK_MINIO_ENDPOINT_URL
                                + "' AS SPARK_MINIO_ENDPOINT_URL, '"
                                + SPARK_MINIO_ACCESS_KEY + "' AS SPARK_MINIO_ACCESS_KEY, '" + SPARK_MINIO_SECRET_KEY
                                + "' AS SPARK_MINIO_SECRET_KEY, '"
                                + SPARK_MINIO_BUCKET_NAME_PM + "' AS SPARK_MINIO_BUCKET_NAME_PM, '"
                                + SPARK_PM_JDBC_DRIVER
                                + "' AS SPARK_PM_JDBC_DRIVER, '" + SPARK_PM_JDBC_URL + "' AS SPARK_PM_JDBC_URL, '"
                                + SPARK_PM_JDBC_USERNAME + "' AS SPARK_PM_JDBC_USERNAME, '" + SPARK_PM_JDBC_PASSWORD
                                + "' AS SPARK_PM_JDBC_PASSWORD, " + FROM_PM_NODE_VENDOR;

                logger.info("🚀 SPARK_PM_JDBC_QUERY: {}", SPARK_PM_JDBC_QUERY);

                dataFrame = new UpdateParameterInJobContext(
                                1,
                                "UPDATE PARAMETER IN JOB CONTEXT",
                                SPARK_PM_JDBC_DRIVER,
                                SPARK_PM_JDBC_URL,
                                SPARK_PM_JDBC_USERNAME,
                                SPARK_PM_JDBC_PASSWORD,
                                SPARK_PM_JDBC_QUERY,
                                null,
                                TEMP_TABLE).executeAndGetResultDataframe(jobContext);

                logger.info("Updated Job Context Executed Successfully! ✅");

                /*
                 * GET COUNTER AND KPI DETAILS AND REGISTER UDF
                 */

                dataFrame = GET_COUNT_AND_KPI_DETAILS_AND_REGISTER_UDF.callSubFlow(dataFrame, jobContext);

                logger.info("🚀 GET COUNTER AND KPI DETAILS AND REGISTER UDF Executed Successfully! ✅");

                /*
                 * READ RAW FILES FROM MINIO
                 */

                dataFrame = new ListMINIOFilesCustomV1(
                                dataFrame,
                                2,
                                "LIST MINIO FILES",
                                SPARK_MINIO_BUCKET_NAME_PM + ":" + RAW_FILE_PATH,
                                "fileName",
                                SPARK_MINIO_BUCKET_NAME_PM,
                                "fileList",
                                "true").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 LIST MINIO FILES Processor Executed Successfully! ✅");

                /*
                 * REPARTITION FILE LIST
                 */

                dataFrame = new RepartitionD1Custom(
                                dataFrame,
                                3,
                                "REPARTITION FILE LIST",
                                "200",
                                "fileName",
                                "fileTable").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 REPARTITION FILE LIST Executed Successfully! ✅");

                /*
                 * UNZIP FILES UDF
                 */

                UnzipToBytesForYB unzipToBytesForYB = new UnzipToBytesForYB();
                unzipToBytesForYB.jobContext = jobContext;
                jobContext.sqlctx().udf().register(unzipToBytesForYB.getName(), unzipToBytesForYB,
                                unzipToBytesForYB.getReturnType());
                dataFrame = jobContext.sqlctx().sql(
                                "SELECT parsedData.raw_data_arr.* FROM (SELECT EXPLODE(UnzipToBytes(fileName)) AS raw_data_arr FROM fileTable) AS parsedData");

                logger.info("🚀 UNZIP FILES UDF Executed Successfully! ✅");

                /*
                 * REPARTITION UNZIPPED FILE DATA
                 */

                dataFrame = new RepartitionD1Custom(
                                dataFrame,
                                4,
                                "REPARTITION UNZIPPED FILE DATA",
                                "200",
                                "partitionNumber",
                                "ExtractCsvfileListPartition").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 REPARTITION UNZIPPED FILE DATA Executed Successfully! ✅");

                /*
                 * CACHE UNZIPPED FILE DATA
                 */

                dataFrame = new CacheDatasetCustom(
                                dataFrame,
                                5,
                                "CACHE UNZIPPED FILE DATA",
                                "memoryAndDisk",
                                "cacheRawBytesDataFrame").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 CACHE UNZIPPED FILE DATA Executed Successfully! ✅");

                /*
                 * INSERT RAW FILE COUNT TO PROCESSING AUDIT
                 */

                dataFrame = INSERT_RAW_FILE_COUNT_TO_PROCESSING_AUDIT.callSubFlow(dataFrame, jobContext);

                logger.info("🚀 INSERT RAW FILE COUNT TO PROCESSING AUDIT Executed Successfully! ✅");

                /*
                 * RETURN DATAFRAME
                 */

                return dataFrame;
        }
}
