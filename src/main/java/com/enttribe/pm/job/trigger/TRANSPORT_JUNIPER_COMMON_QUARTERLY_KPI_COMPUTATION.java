package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.JDBCReadCustom;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.custom.processor.CacheDatasetCustom;

public class TRANSPORT_JUNIPER_COMMON_QUARTERLY_KPI_COMPUTATION {

    private static Logger logger = LoggerFactory.getLogger(TRANSPORT_JUNIPER_COMMON_QUARTERLY_KPI_COMPUTATION.class);

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        /*
         * LIST AND UNZIP RAW FILE
         */

        dataFrame = LIST_AND_UNZIP_RAW_FILE.callSubFlow(dataFrame, jobContext);

        logger.info("🚀 LIST AND UNZIP RAW FILE Executed Successfully! ✅");

        /*
         * CUSTOM PROCESSING UDF
         */

        dataFrame = CUSTOM_JUNIPER_COMMON_PARSING_AND_ORC_WRITE.callSubFlow(dataFrame, jobContext);

        logger.info("🚀 CUSTOM PROCESSING UDF Executed Successfully! ✅");

        /*
         * OVERWRITE MATCHING PARTITIONS
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 1, "OVERWRITE MATCHING PARTITIONS",
                "SET spark.sql.sources.partitionOverwriteMode=dynamic", "OverwriteMatchingPartitions", null)
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

        String DOMAIN = jobContext.getParameter("DOMAIN");
        String VENDOR = jobContext.getParameter("VENDOR");
        String NE_TYPE = jobContext.getParameter("NE_TYPE");

        String CELL_LEVEL_NETWORK_ELEMENT_QUERY = "SELECT networkelementid_pk, pmemsid, netype, CONCAT( 'NS@', COALESCE(nestatus, 'null'), '##', 'GC@', COALESCE(gcname, 'null'), '##', 'ENB@', COALESCE(RIU, 'null'), '##', 'ENB_NEID@', COALESCE(RIUNEID, 'null'), '##', 'BND@', COALESCE(NEFREQ, 'null'), '##', 'NEID@', COALESCE(NEID, 'null'), '##', 'L4@', COALESCE(L4, 'null'), '##', 'L3@', COALESCE(L3, 'null'), '##', 'L2@', COALESCE(L2, 'null'), '##', 'L1@', COALESCE(L1, 'null'), '##', 'EGID@', COALESCE(ECGI, 'null'), '##', 'EID@', COALESCE(ENBID, 'null'), '##', 'NET@', COALESCE(netype, 'null'), '##', 'DL4@', COALESCE(DL4, 'null'), '##', 'DL3@', COALESCE(DL3, 'null'), '##', 'DL2@', COALESCE(DL2, 'null'), '##', 'DL1@', COALESCE(DL1, 'null'), '##', 'D@', COALESCE(domain, 'null'), '##', 'V@', COALESCE(vendor, 'null'), '##', 'OG@', COALESCE(ZONE, 'null'), '##', 'DOG@', COALESCE(DZONE, 'null'), '##', 'NAM@', COALESCE(child, 'null'), '##', 'displaynodename@', COALESCE(child, 'null'), '##', 'NODE_TYPE@', COALESCE(NODE_TYPE, 'null'), '##', 'ENTITY_ID@', COALESCE(ENTITY_ID, 'null'), '##', 'ENTITY_NAME@', COALESCE(ENTITY_NAME, 'null'), '##', 'ENTITY_TYPE@', COALESCE(ENTITY_TYPE, 'null'), '##', 'PARENT_ENTITY_ID@', COALESCE(PARENT_ENTITY_ID, 'null'), '##', 'PARENT_ENTITY_NAME@', COALESCE(PARENT_ENTITY_NAME, 'null'), '##', 'PARENT_ENTITY_TYPE@', COALESCE(PARENT_ENTITY_TYPE, 'null')) AS geoDetails FROM ( SELECT NODE_TYPE, networkelementid_pk, ZONE, DZONE, '$vendor' AS vendor, '$domain' AS domain, netype, nestatus, pmemsid, ECGI, ENBID, CELLID, child, RIU, RIUNEID, NEID, NEFREQ, UPPER(gl4.GEO_NAME) AS L4, UPPER(gl4.PRETTY_NAME) AS DL4, UPPER(gl3.GEO_NAME) AS L3, UPPER(gl3.PRETTY_NAME) AS DL3, UPPER(gl2.GEO_NAME) AS L2, UPPER(gl2.PRETTY_NAME) AS DL2, UPPER(gl1.GEO_NAME) AS L1, UPPER(gl1.PRETTY_NAME) AS DL1, gc.NEL_ID AS gcneid, gc.NEL_ID AS gcname, ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, PARENT_ENTITY_ID, PARENT_ENTITY_NAME, PARENT_ENTITY_TYPE FROM ( SELECT UPPER(parent.NE_TYPE) AS NODE_TYPE, child.NETWORK_ELEMENT_ID_PK AS networkelementid_pk, og.GEO_NAME AS ZONE, og.PRETTY_NAME AS DZONE, child.NE_TYPE AS netype, child.NE_STATUS AS nestatus, child.PM_EMS_ID AS pmemsid, child.ECGI, child.ENB_ID AS ENBID, child.CELL_NUM AS CELLID, child.NE_NAME AS child, parent.NE_NAME AS RIU, parent.NE_ID AS RIUNEID, child.NE_ID AS NEID, child.NE_FREQUENCY AS NEFREQ, child.GEOGRAPHY_L4_ID_FK, child.GEOGRAPHY_L3_ID_FK, child.GEOGRAPHY_L2_ID_FK, child.GEOGRAPHY_L1_ID_FK, child.NE_LOCATION_ID_FK, parent.NE_LOCATION_ID_FK AS parent_loc_id, parent.NE_ID AS ENTITY_ID, parent.NE_NAME AS ENTITY_NAME, parent.NE_TYPE AS ENTITY_TYPE, parent.NE_ID AS PARENT_ENTITY_ID, parent.NE_NAME AS PARENT_ENTITY_NAME, parent.NE_TYPE AS PARENT_ENTITY_TYPE FROM NETWORK_ELEMENT child LEFT JOIN NETWORK_ELEMENT parent ON child.PARENT_NE_ID_FK = parent.NETWORK_ELEMENT_ID_PK LEFT JOIN NE_GEOGRAPHY_MAPPING neg ON child.NETWORK_ELEMENT_ID_PK = neg.NETWORK_ELEMENT_ID_FK AND UPPER(neg.GEOGRAPHY_TYPE) = 'ZONE' LEFT JOIN ADDITIONAL_GEO og ON neg.OTHER_GEOGRAPHY_ID_FK = og.ID WHERE child.DOMAIN = '$domain' AND child.VENDOR = '$vendor' AND child.PM_EMS_ID IS NOT NULL AND child.DELETED = 0 AND child.NE_NAME IS NOT NULL AND parent.NE_NAME IS NOT NULL AND child.NE_TYPE IS NOT NULL AND parent.NE_TYPE IS NOT NULL) base LEFT JOIN NE_LOCATION gc ON gc.NE_LOCATION_ID_PK = base.parent_loc_id LEFT JOIN PRIMARY_GEO_L4 gl4 ON base.GEOGRAPHY_L4_ID_FK = gl4.ID LEFT JOIN PRIMARY_GEO_L3 gl3 ON base.GEOGRAPHY_L3_ID_FK = gl3.ID LEFT JOIN PRIMARY_GEO_L2 gl2 ON base.GEOGRAPHY_L2_ID_FK = gl2.ID LEFT JOIN PRIMARY_GEO_L1 gl1 ON base.GEOGRAPHY_L1_ID_FK = gl1.ID ) tmp1 WHERE NEID IS NOT NULL AND pmemsid IS NOT NULL AND netype IN (${neType})";

        CELL_LEVEL_NETWORK_ELEMENT_QUERY = CELL_LEVEL_NETWORK_ELEMENT_QUERY.replace("$domain", DOMAIN);
        CELL_LEVEL_NETWORK_ELEMENT_QUERY = CELL_LEVEL_NETWORK_ELEMENT_QUERY.replace("$vendor", VENDOR);
        CELL_LEVEL_NETWORK_ELEMENT_QUERY = CELL_LEVEL_NETWORK_ELEMENT_QUERY.replace("${neType}", NE_TYPE);

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

        dataFrame.show(5);
        logger.info("🚀 JOINED BY PM_EMS_ID Executed Successfully! ✅");

        /*
         * CREATE META DATA
         */

        String COUNTER_SELECT_QUERY = jobContext.getParameter("COUNTER_SELECT_QUERY");

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 6, "CREATE META DATA",
                "SELECT CreateMetaData(netype, rowKey, geoDetails, 'QUARTERLY', PT) AS rawMetaData,"
                        + COUNTER_SELECT_QUERY
                        + " FROM GeoDetailJoinedDF",
                "metaColData", null).executeAndGetResultDataframe(jobContext);

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

        logger.info("🚀 CACHE DATASET FOR AGGREGATION Executed Successfully! ✅");

        /*
         * CELL LEVEL KPI COMPUTATION
         */

        dataFrame = CELL_LEVEL_KPI_COMPUTATION_SUBFLOW.callSubFlow(dataFrame, jobContext);

        logger.info("🚀 CELL LEVEL AGGREGATION Executed Successfully! ✅");

        /*
         * CELL LEVEL TO NODE LEVEL
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 9, "CELL LEVEL TO NODE LEVEL",
                "SELECT NodeAggrRowKeyConverter('Node', metaData['NAM'], CAST(ctime AS BIGINT), metaData, '"
                        + jobContext.getParameter("SubAggrType") + "') AS rawNodeData,"
                        + COUNTER_SELECT_QUERY
                        + " FROM CacheNodeAggrRawData WHERE nodeName IS NOT NULL",
                "RawCounterDataNodeLevel", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 CELL LEVEL TO NODE LEVEL Executed Successfully! ✅");

        /*
         * EXPLODE NODE LEVEL DATA
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 10, "EXPLODE NODE LEVEL DATA",
                "SELECT 'Node' AS parquetLevel, parsedata.rawdata.domain, parsedata.rawdata.vendor, parsedata.rawdata.technology, parsedata.rawdata.networkType, parsedata.rawdata.Level, parsedata.rawdata.nodeName, parsedata.rawdata.ctime, parsedata.rawdata.metaData, rawdata.metaData['NAM'] AS nename, "
                        + COUNTER_SELECT_QUERY
                        + " FROM (SELECT EXPLODE(rawNodeData) AS rawdata, "
                        + COUNTER_SELECT_QUERY
                        + " FROM RawCounterDataNodeLevel) parsedata",
                "NodeRowKeyConvertedData", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 EXPLODE NODE LEVEL DATA Executed Successfully! ✅");

        /*
         * NODE LEVEL KPI COMPUTATION
         */

        dataFrame = NODE_LEVEL_KPI_COMPUTATION_SUBFLOW.callSubFlow(dataFrame, jobContext);

        logger.info("🚀 NODE LEVEL KPI COMPUTATION Executed Successfully! ✅");


        /*
         * CELL LEVEL TO GEO LEVEL AGGREGATION
        */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 11, "CELL LEVEL TO GEO LEVEL AGGREGATION",
        "SELECT NodeAggrRowKeyConverter('Geography', metaData['NAM'], CAST(ctime AS BIGINT), metaData,'"
                        + jobContext.getParameter("SubAggrType") + "') AS rawNodeData,"
                        + COUNTER_SELECT_QUERY
                        + " FROM CacheNodeAggrRawData WHERE nodeName IS NOT NULL",
        "RawCounterDataGeoLevel", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 CELL LEVEL TO GEO LEVEL AGGREGATION Executed Successfully! ✅");

        /*
         * EXPLODE GEO LEVEL DATA
        */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 12, "EXPLODE GEO LEVEL DATA",
        "SELECT 'Geography' AS parquetLevel, parsedata.rawdata.domain, parsedata.rawdata.vendor, parsedata.rawdata.technology, parsedata.rawdata.networkType, parsedata.rawdata.Level, parsedata.rawdata.nodeName, parsedata.rawdata.ctime, parsedata.rawdata.metaData, rawdata.metaData['NAM'] AS nename, "
                        + COUNTER_SELECT_QUERY
                        + " FROM (SELECT EXPLODE(rawNodeData) AS rawdata, "
                        + COUNTER_SELECT_QUERY
                        + " FROM RawCounterDataGeoLevel) parsedata",
                "NodeRowKeyConvertedData", null).executeAndGetResultDataframe(jobContext);

        logger.info("🚀 EXPLODE GEO LEVEL DATA Executed Successfully! ✅");

        /*
         * GEO LEVEL KPI COMPUTATION
        */

        dataFrame = NODE_LEVEL_KPI_COMPUTATION_SUBFLOW.callSubFlow(dataFrame, jobContext);

        logger.info("🚀 GEO LEVEL KPI COMPUTATION Executed Successfully! ✅");

        /*
         * WRITE ORC FOR DATA INTEGRITY
        */

        dataFrame = WRITE_ORC_FOR_DATA_INTEGRITY.callSubFlow(dataFrame, jobContext);

        logger.info("🚀 WRITE ORC FOR DATA INTEGRITY Executed Successfully! ✅");

        return dataFrame;
    }
}