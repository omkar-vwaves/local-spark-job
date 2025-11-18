package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.custom.processor.CacheDatasetCustom;
import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCReadCustom;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.sparkrunner.context.JobContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRANSPORT_ALL_VENDOR_HOURLY_KPI_COMPUTATION {

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        final Logger logger = LoggerFactory.getLogger(TRANSPORT_ALL_VENDOR_HOURLY_KPI_COMPUTATION.class);

        /*
         * READ 15 MIN ORC FILES FROM S3 SUBFLOW
         */

        dataFrame = READ_15MIN_ORC_FILES_FROM_S3_SUBFLOW.callSubFlow(dataFrame, jobContext);

        /*
         * REPARTITION DATAFRAME
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 1, "REPARTITION DATAFRAME",
                "SET spark.sql.sources.partitionOverwriteMode=dynamic", "RepartitionDataFrame", null)
                .executeAndGetResultDataframe(jobContext);

        logger.info("🚀 OVERWRITE MATCHING PARTITIONS Executed Successfully! ✅");

        /*
         * GROUP AND EXTRACT VALUES
         */

        String RAW_FILE_COUNTER_NODE_AGGR_QUERY = jobContext.getParameter("RAW_FILE_COUNTER_NODE_AGGR_QUERY");

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 2, "GROUP AND EXTRACT VALUES",
                "SELECT rowKey, SPLIT(rowKey, '##')[0] AS pmemsId, "
                        + RAW_FILE_COUNTER_NODE_AGGR_QUERY
                        + ", FIRST_VALUE(PT) AS PT FROM parseRawNodeData GROUP BY rowKey",
                "nodeAggrRawData", null).executeAndGetResultDataframe(jobContext);
        logger.info("🚀 GROUP AND EXTRACT VALUES Executed Successfully! ✅");

        /*
         * READ CELL LEVEL NETWORK ELEMENT
         */

        String CELL_LEVEL_NETWORK_ELEMENT_QUERY = "SELECT CHILD_NETWORK_ELEMENT_ID_PK AS networkelementid_pk, CHILD_PM_EMS_ID AS pmemsid, CHILD_NE_TYPE AS netype, CONCAT( 'D@', COALESCE(DOMAIN, 'null'), '##', 'V@', COALESCE(VENDOR, 'null'), '##', 'NEID@', COALESCE(CHILD_NE_ID, 'null'), '##', 'NAM@', COALESCE(CHILD_NE_NAME, 'null'), '##', 'NET@', COALESCE(CHILD_NE_TYPE, 'null'), '##', 'L4@', COALESCE(CHILD_L4, 'L4'), '##', 'L3@', COALESCE(CHILD_L3, 'L3'), '##', 'L2@', COALESCE(CHILD_L2, 'L2'), '##', 'L1@', COALESCE(CHILD_L1, 'L1'), '##', 'DL4@', COALESCE(CHILD_DL4, 'L4'), '##', 'DL3@', COALESCE(CHILD_DL3, 'L3'), '##', 'DL2@', COALESCE(CHILD_DL2, 'L2'), '##', 'DL1@', COALESCE(CHILD_DL1, 'L1'), '##', 'ENB@', COALESCE(PARENT_ENTITY_NAME, 'null'), '##', 'ENB_NEID@', COALESCE(PARENT_ENTITY_ID, 'null'), '##', 'displaynodename@', COALESCE(CHILD_NE_NAME, 'null'), '##', 'NODE_TYPE@', COALESCE(PARENT_NODE_TYPE, 'null'), '##', 'ENTITY_ID@', COALESCE(PARENT_ENTITY_ID, 'null'), '##', 'ENTITY_NAME@', COALESCE(PARENT_ENTITY_NAME, 'null'), '##', 'ENTITY_TYPE@', COALESCE(PARENT_ENTITY_TYPE, 'null') ) AS geoDetails FROM ( SELECT PARENT_NODE_TYPE, CHILD_NETWORK_ELEMENT_ID_PK, 'TRANSPORT' AS DOMAIN, 'JUNIPER' AS VENDOR, CHILD_NE_TYPE, CHILD_PM_EMS_ID, CHILD_NE_ID, CHILD_NE_NAME, gl4.GEO_NAME AS CHILD_L4, gl4.GEO_NAME AS CHILD_DL4, gl3.GEO_NAME AS CHILD_L3, gl3.GEO_NAME AS CHILD_DL3, gl2.GEO_NAME AS CHILD_L2, gl2.GEO_NAME AS CHILD_DL2, gl1.GEO_NAME AS CHILD_L1, gl1.GEO_NAME AS CHILD_DL1, PARENT_ENTITY_ID, PARENT_ENTITY_NAME, PARENT_ENTITY_TYPE FROM ( SELECT UPPER(parent.NE_TYPE) AS PARENT_NODE_TYPE, child.NETWORK_ELEMENT_ID_PK AS CHILD_NETWORK_ELEMENT_ID_PK, child.NE_TYPE AS CHILD_NE_TYPE, child.PM_EMS_ID AS CHILD_PM_EMS_ID, child.NE_NAME AS CHILD_NE_NAME, child.NE_ID AS CHILD_NE_ID, child.GEOGRAPHY_L4_ID_FK, child.GEOGRAPHY_L3_ID_FK, child.GEOGRAPHY_L2_ID_FK, child.GEOGRAPHY_L1_ID_FK, child.NE_LOCATION_ID_FK, parent.NE_LOCATION_ID_FK AS parent_loc_id, parent.NE_ID AS PARENT_ENTITY_ID, parent.NE_NAME AS PARENT_ENTITY_NAME, parent.NE_TYPE AS PARENT_ENTITY_TYPE FROM NETWORK_ELEMENT child LEFT JOIN NETWORK_ELEMENT parent ON child.PARENT_NE_ID_FK = parent.NETWORK_ELEMENT_ID_PK LEFT JOIN NE_GEOGRAPHY_MAPPING neg ON child.NETWORK_ELEMENT_ID_PK = neg.NETWORK_ELEMENT_ID_FK LEFT JOIN ADDITIONAL_GEO og ON neg.OTHER_GEOGRAPHY_ID_FK = og.ID WHERE child.DOMAIN = 'TRANSPORT' AND child.VENDOR = 'JUNIPER' AND child.PM_EMS_ID IS NOT NULL AND child.DELETED = 0 AND child.NE_NAME IS NOT NULL AND parent.NE_NAME IS NOT NULL AND child.NE_TYPE IS NOT NULL AND parent.NE_TYPE IS NOT NULL AND child.NE_TYPE IN ('INTERFACE','ROUTER') ) base LEFT JOIN NE_LOCATION gc ON gc.NE_LOCATION_ID_PK = base.parent_loc_id LEFT JOIN PRIMARY_GEO_L4 gl4 ON base.GEOGRAPHY_L4_ID_FK = gl4.ID LEFT JOIN PRIMARY_GEO_L3 gl3 ON base.GEOGRAPHY_L3_ID_FK = gl3.ID LEFT JOIN PRIMARY_GEO_L2 gl2 ON base.GEOGRAPHY_L2_ID_FK = gl2.ID LEFT JOIN PRIMARY_GEO_L1 gl1 ON base.GEOGRAPHY_L1_ID_FK = gl1.ID ) tmp1 WHERE CHILD_NE_ID IS NOT NULL AND CHILD_PM_EMS_ID IS NOT NULL";
        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

        dataFrame = new JDBCReadCustom(dataFrame, 3, "READ CELL LEVEL NETWORK ELEMENT", SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL,
                SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD, CELL_LEVEL_NETWORK_ELEMENT_QUERY, "NEDetail", null,
                null, null, null, null)
                .executeAndGetResultDataframe(jobContext);

        // dataFrame.show(5);
        logger.info("🚀 READ CELL LEVEL NETWORK ELEMENT Executed Successfully! ✅");

        /*
         * REPARTITION BASED ON PM_EMS_ID
         */

        dataFrame = new RepartitionD1Custom(dataFrame, 4, "REPARTITION BASED ON PM_EMS_ID", "200", "pmemsid",
                "GeographyDetail").executeAndGetResultDataframe(jobContext);

        logger.info("🚀 REPARTITION BASED ON PM_EMS_ID Executed Successfully! ✅");

        /*
         * JOINED BY PM_EMS_ID
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 5, "JOINED BY PM_EMS_ID",
                "SELECT ne.netype, ne.geoDetails, agr.* FROM nodeAggrRawData agr INNER JOIN GeographyDetail ne ON UPPER(agr.pmemsid) = UPPER(ne.pmemsid)",
                "GeoDetailJoinedDF", null).executeAndGetResultDataframe(jobContext);

        // dataFrame.show(5);
        logger.info("🚀 JOINED BY PM_EMS_ID Executed Successfully! ✅");

        /*
         * CREATE META DATA
         */

        String COUNTER_SELECT_QUERY = jobContext.getParameter("COUNTER_SELECT_QUERY");

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 6, "CREATE META DATA",
                "SELECT CreateMetaData(netype, rowKey, geoDetails, 'HOURLY', PT) AS rawMetaData,"
                        + COUNTER_SELECT_QUERY
                        + " FROM GeoDetailJoinedDF",
                "metaColData", null).executeAndGetResultDataframe(jobContext);

        // dataFrame.show(5);
        logger.info("🚀 CREATE META DATA Executed Successfully! ✅");

        /*
         * EXPLODE META COLUMNS DATA
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 7, "EXPLODE META COLUMNS DATA",
                "SELECT 'Cell' AS parquetLevel, parseData.rawdata.domain, parseData.rawdata.rowKey AS rowkey, parseData.rawdata.vendor, parseData.rawdata.technology, parseData.rawdata.networkType, parseData.rawdata.Level, parseData.rawdata.nodeName, parseData.rawdata.ctime, parseData.rawdata.metaData, rawdata.metaData['displaynodename'] AS nename, DATE_FORMAT(TO_DATE(CAST(UNIX_TIMESTAMP(rawdata.metaData['Date'], 'yyMMdd') AS TIMESTAMP)), 'yyMM') AS Month, concat(DATE_FORMAT(TO_DATE(CAST(UNIX_TIMESTAMP(rawdata.metaData['Date'], 'yyMMdd') AS TIMESTAMP)), 'yy'), LPAD(WEEKOFYEAR(TO_DATE(CAST(UNIX_TIMESTAMP(rawdata.metaData['Date'], 'yyMMdd') AS TIMESTAMP))) ,2,0)) AS Week, rawdata.metaData['Date'] AS Date, rawdata.metaData['Time'] AS Time, rawdata.metaData['PT'] AS PT, "
                        + COUNTER_SELECT_QUERY
                        + " FROM (SELECT EXPLODE(rawMetaData) AS rawdata, "
                        + COUNTER_SELECT_QUERY
                        + " FROM metaColData) AS parseData",
                "cellAggrCounterData", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 EXPLODE META COLUMNS DATA Executed Successfully! ✅");

        /*
         * CACHE DATASET FOR AGGREGATION
         */

        dataFrame = new CacheDatasetCustom(dataFrame, 8, "CACHE DATASET FOR AGGREGATION", "memoryAndDisk",
                "CacheNodeAggrRawData").executeAndGetResultDataframe(jobContext);

        dataFrame.show(5);
        logger.info("🚀 CACHE DATASET FOR AGGREGATION Executed Successfully! ✅");

        /*
         * CELL LEVEL KPI COMPUTATION
         */

        dataFrame = HOURLY_CELL_LEVEL_KPI_COMPUTATION_SUBFLOW.callSubFlow(dataFrame, jobContext);

        dataFrame.show(5);
        logger.info("🚀 HOURLY CELL LEVEL KPI COMPUTATION Executed Successfully! ✅");

        return dataFrame;
    }
}
