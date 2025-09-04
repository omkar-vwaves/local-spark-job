package com.enttribe.pm.job.trigger;

import com.enttribe.sparkrunner.context.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.CacheDatasetCustom;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.custom.processor.CQLWriteCustom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NODE_LEVEL_KPI_COMPUTATION_SUBFLOW {

        private static final Logger logger = LoggerFactory.getLogger(NODE_LEVEL_KPI_COMPUTATION_SUBFLOW.class);

        public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

                /*
                 * PARSE NODE LEVEL DATA
                 */

                String COUNTER_NODE_AGGR_QUERY = jobContext.getParameter("COUNTER_NODE_AGGR_QUERY");

                String PARSE_NODE_LEVEL_DATA_QUERY = "SELECT *,DATE_FORMAT(TO_DATE(CAST(UNIX_TIMESTAMP(metaData['Date'], 'yyMMdd') AS TIMESTAMP)),'yyMM') AS Month, concat(DATE_FORMAT(TO_DATE(CAST(UNIX_TIMESTAMP(metaData['Date'], 'yyMMdd') AS TIMESTAMP)),'yy'), lpad(weekofyear(TO_DATE(CAST(UNIX_TIMESTAMP(metaData['Date'], 'yyMMdd') AS TIMESTAMP))) ,2,0)) AS Week,DATE_FORMAT(CAST(UNIX_TIMESTAMP(concat(metaData['Date'],metaData['Time']), 'yyMMddHHmm') AS TIMESTAMP)-INTERVAL 5 Hours ,'yyMMdd') AS CETDate,metaData['Date'] AS Date,metaData['Time'] AS Time,metaData['PT'] AS PT FROM (SELECT first_value(nodeName) AS nodeName,first_value(Level) AS Level,first_value(ctime) AS ctime,first_value(domain) AS domain,first_value(vendor) AS vendor,first_value(technology) AS technology,first_value(networkType) AS networkType,"
                                + COUNTER_NODE_AGGR_QUERY
                                + ",first_value(metaData) AS metaData,first_value(parquetLevel) AS parquetLevel FROM NodeRowKeyConvertedData WHERE nodeName IS NOT NULL GROUP BY nodeName, Level, ctime) data";

                // PARSE_NODE_LEVEL_DATA_QUERY = "SELECT
                // *,DATE_FORMAT(TO_DATE(CAST(UNIX_TIMESTAMP(metaData['Date'], 'yyMMdd') AS
                // TIMESTAMP)),'yyMM') AS Month,
                // concat(DATE_FORMAT(TO_DATE(CAST(UNIX_TIMESTAMP(metaData['Date'], 'yyMMdd') AS
                // TIMESTAMP)),'yy'),
                // lpad(weekofyear(TO_DATE(CAST(UNIX_TIMESTAMP(metaData['Date'], 'yyMMdd') AS
                // TIMESTAMP))) ,2,0)) AS
                // Week,DATE_FORMAT(CAST(UNIX_TIMESTAMP(concat(metaData['Date'],metaData['Time']),
                // 'yyMMddHHmm') AS TIMESTAMP)-INTERVAL 5 Hours ,'yyMMdd') AS
                // CETDate,metaData['Date'] AS Date,metaData['Time'] AS Time,metaData['PT'] AS
                // PT FROM (SELECT first(nodeName, true) AS nodeName,first(Level, true) AS
                // Level,first(ctime, true) AS ctime,first(domain, true) AS domain,first(vendor,
                // true) AS vendor,first(technology, true) AS technology,first(networkType,
                // true) AS networkType,"
                // + COUNTER_NODE_AGGR_QUERY
                // + ",first(metaData, true) AS metaData,first(parquetLevel, true) AS
                // parquetLevel FROM NodeRowKeyConvertedData WHERE nodeName IS NOT NULL GROUP BY
                // nodeName, Level, ctime) data";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                                1,
                                "PARSE NODE LEVEL DATA",
                                PARSE_NODE_LEVEL_DATA_QUERY,
                                "nodeAggrRawDataOne",
                                null).executeAndGetResultDataframe(jobContext);

                dataFrame.show(false);
                logger.info("🚀 PARSE NODE LEVEL DATA Executed Successfully! ✅");

                /*
                 * CACHE PARSED NODE LEVEL DATASET
                 */

                dataFrame = new CacheDatasetCustom(
                                dataFrame,
                                2,
                                "CACHE PARSED NODE LEVEL DATASET",
                                "memoryAndDisk",
                                "cacheNodeAggrRawDataOne")
                                .executeAndGetResultDataframe(jobContext);

                logger.info("🚀 CACHE PARSED NODE LEVEL DATASET Executed Successfully! ✅");

                /*
                 * NODE LEVEL RAW DATA TIME AGGREGATION
                 */

                String COUNTER_SELECT_QUERY = jobContext.getParameter("COUNTER_SELECT_QUERY");
                String JOB_TYPE = jobContext.getParameter("JOB_TYPE");

                String NODE_LEVEL_RAW_DATA_TIME_AGGREGATION_QUERY = "SELECT nodeName, Level, domain, vendor, technology, networkType, timeAggrFinalKey.ctime, "
                                + COUNTER_SELECT_QUERY
                                + ", timeAggrFinalKey.metaData FROM (SELECT TimeAggrRowKeyConverter(CAST(ctime AS BIGINT), '"
                                + JOB_TYPE + "', metaData) AS timeAggrFinalKey,"
                                + COUNTER_SELECT_QUERY
                                + ",nodeName, Level, domain, vendor, technology, networkType FROM cacheNodeAggrRawDataOne WHERE concat(metaData['Date'],metaData['Time'])!= metaData['PT']) timeAggrRawdata";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                                2,
                                "NODE LEVEL RAW DATA TIME AGGREGATION",
                                NODE_LEVEL_RAW_DATA_TIME_AGGREGATION_QUERY,
                                "RawDataForTimeAggregatorOne",
                                null).executeAndGetResultDataframe(jobContext);

                dataFrame.show(false);
                logger.info("🚀 NODE LEVEL RAW DATA TIME AGGREGATION Executed Successfully! ✅");

                /*
                 * NODE LEVEL TIME AGGREGATION MAP
                 */

                String COUNTER_MAP_QUERY = jobContext.getParameter("COUNTER_MAP_QUERY");

                String NODE_LEVEL_TIME_AGGREGATION_MAP_QUERY = "SELECT nodeName, Level, ctime, domain, vendor, technology, networkType,"
                                + COUNTER_MAP_QUERY
                                + ",metaData FROM RawDataForTimeAggregatorOne";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                                3,
                                "NODE LEVEL TIME AGGREGATION MAP",
                                NODE_LEVEL_TIME_AGGREGATION_MAP_QUERY,
                                "timeNodeAggrMapDataFrameOne",
                                null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 NODE LEVEL TIME AGGREGATION MAP Executed Successfully! ✅");

                /*
                 * NODE LEVEL KPI COMPUTATION
                 */

                String NODE_LEVEL_KPI_COMPUTATION_QUERY = "SELECT formula_calculated_data.record_data.* FROM (SELECT KPIEvaluatorBntv(domain, vendor, technology,networkType,Level,nodeName,ctime,rawcounters,metaData) AS record_data FROM timeNodeAggrMapDataFrameOne) formula_calculated_data";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                                4,
                                "NODE LEVEL KPI COMPUTATION",
                                NODE_LEVEL_KPI_COMPUTATION_QUERY,
                                "KPICalculatedDataframeOne",
                                null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 NODE LEVEL KPI COMPUTATION Executed Successfully! ✅");

                /*
                 * REPARTITION BEFORE NODE LEVEL CQL WRITE
                 */

                dataFrame = new RepartitionD1Custom(dataFrame,
                                5,
                                "REPARTITION BEFORE NODE LEVEL CQL WRITE",
                                "200",
                                "nodeName",
                                "repartitionTemp").executeAndGetResultDataframe(jobContext);

                dataFrame.show(false);
                logger.info("🚀 REPARTITION BEFORE NODE LEVEL CQL WRITE Executed Successfully! ✅");

                /*
                 * NODE LEVEL CQL WRITE
                 */

                String CQL_TABLE_NAME = jobContext.getParameter("CQL_TABLE_NAME");
                String SPARK_CASSANDRA_KEYSPACE_PM = jobContext.getParameter("SPARK_CASSANDRA_KEYSPACE_PM");
                String SPARK_CASSANDRA_HOST = jobContext.getParameter("SPARK_CASSANDRA_HOST");
                String SPARK_CASSANDRA_PORT = jobContext.getParameter("SPARK_CASSANDRA_PORT");
                String SPARK_CASSANDRA_USERNAME = jobContext.getParameter("SPARK_CASSANDRA_USERNAME");
                String SPARK_CASSANDRA_PASSWORD = jobContext.getParameter("SPARK_CASSANDRA_PASSWORD");
                String SPARK_CASSANDRA_DATACENTER = jobContext.getParameter("SPARK_CASSANDRA_DATACENTER");

                dataFrame = new CQLWriteCustom(dataFrame, 6, "NODE LEVEL CQL WRITE",
                                CQL_TABLE_NAME,
                                SPARK_CASSANDRA_KEYSPACE_PM,
                                "Append",
                                SPARK_CASSANDRA_HOST,
                                SPARK_CASSANDRA_PORT,
                                SPARK_CASSANDRA_USERNAME,
                                SPARK_CASSANDRA_PASSWORD,
                                null,
                                null,
                                SPARK_CASSANDRA_DATACENTER,
                                null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 NODE LEVEL CQL WRITE Executed Successfully! ✅");

                return dataFrame;

        }
}
