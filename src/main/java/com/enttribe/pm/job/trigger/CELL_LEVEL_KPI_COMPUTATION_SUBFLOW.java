package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.custom.processor.CQLWriteCustom;

public class CELL_LEVEL_KPI_COMPUTATION_SUBFLOW {

    private static Logger logger = LoggerFactory.getLogger(CELL_LEVEL_KPI_COMPUTATION_SUBFLOW.class);

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        /*
         * PARSE CELL LEVEL DATA
         */

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 1, "PARSE CELL LEVEL DATA",
                "SELECT * FROM CacheNodeAggrRawData", "cellAggregatedCounterData", null)
                .executeAndGetResultDataframe(jobContext);

        logger.info("🚀 PARSE CELL LEVEL DATA Executed Successfully! ✅");

        /*
         * CELL LEVEL RAW DATA TIME AGGREGATION
         */

        String COUNTER_SELECT_QUERY = jobContext.getParameter("COUNTER_SELECT_QUERY");
        String JOB_TYPE = jobContext.getParameter("JOB_TYPE");

        String CELL_LEVEL_RAW_DATA_TIME_AGGREGATION_QUERY = "SELECT nodeName, Level, domain, vendor, technology, networkType, timeAggrFinalKey.ctime, "
                + COUNTER_SELECT_QUERY
                + ", timeAggrFinalKey.metaData FROM (SELECT TimeAggrRowKeyConverter(CAST(ctime AS BIGINT), '"
                + JOB_TYPE + "', metaData) AS timeAggrFinalKey, " + COUNTER_SELECT_QUERY
                + ", nodeName, Level, domain, vendor, technology, networkType FROM cellAggregatedCounterData WHERE CONCAT(metaData['Date'], metaData['Time']) != metaData['PT']) AS timeAggrRawdata";

        dataFrame = new ExecuteSparkSQLD1Custom(
                dataFrame,
                2, "CELL LEVEL RAW DATA TIME AGGREGATION",
                CELL_LEVEL_RAW_DATA_TIME_AGGREGATION_QUERY,
                "RawDataForTimeAggr",
                null).executeAndGetResultDataframe(jobContext);

        // dataFrame.show(false);
        logger.info("🚀 CELL LEVEL RAW DATA TIME AGGREGATION Executed Successfully! ✅");

        /*
         * CELL LEVEL TIME AGGREGATION MAP
         */

        String COUNTER_MAP_QUERY = jobContext.getParameter("COUNTER_MAP_QUERY");

        String CELL_LEVEL_TIME_AGGREGATION_MAP_QUERY = "SELECT nodeName, Level, ctime, domain, vendor, technology, networkType, "
                + COUNTER_MAP_QUERY + ", metaData FROM RawDataForTimeAggr";

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                3, "CELL LEVEL TIME AGGREGATION MAP",
                CELL_LEVEL_TIME_AGGREGATION_MAP_QUERY,
                "timeCellAggrMapDataFrame",
                null).executeAndGetResultDataframe(jobContext);

        // dataFrame.show(false);
        logger.info("🚀 CELL LEVEL TIME AGGREGATION MAP Executed Successfully! ✅");

        /*
         * CELL LEVEL KPI COMPUTATION
         */

        String CELL_LEVEL_KPI_COMPUTATION_QUERY = "SELECT formula_calculated_data.record_data.* FROM (SELECT KPIEvaluatorBntv(domain, vendor, technology, networkType,Level,nodeName, ctime,rawcounters,metaData) AS record_data FROM timeCellAggrMapDataFrame) formula_calculated_data";

        dataFrame = new ExecuteSparkSQLD1Custom(
                dataFrame,
                4,
                "CELL LEVEL KPI COMPUTATION",
                CELL_LEVEL_KPI_COMPUTATION_QUERY,
                "KPICalculatedDataframe",
                null).executeAndGetResultDataframe(jobContext);

        // dataFrame.show(false);
        logger.info("🚀 CELL LEVEL KPI COMPUTATION Executed Successfully! ✅");

        /*
         * REPARTITION DATA BEFORE CQL WRITE
         */

        dataFrame = new RepartitionD1Custom(
                dataFrame,
                5,
                "REPARTITION DATA BEFORE CQL WRITE",
                "200",
                "nodeName",
                "repartitionTemp").executeAndGetResultDataframe(jobContext);

        // dataFrame.show(false);
        // dataFrame.printSchema();
        logger.info("🚀 REPARTITION DATA BEFORE CQL WRITE Executed Successfully! ✅");

        /*
         * CELL LEVEL CQL WRITE
         */

        String CQL_TABLE_NAME = jobContext.getParameter("CQL_TABLE_NAME");
        String SPARK_CASSANDRA_KEYSPACE_PM = jobContext.getParameter("SPARK_CASSANDRA_KEYSPACE_PM");
        String SPARK_CASSANDRA_HOST = jobContext.getParameter("SPARK_CASSANDRA_HOST");
        String SPARK_CASSANDRA_PORT = jobContext.getParameter("SPARK_CASSANDRA_PORT");
        String SPARK_CASSANDRA_USERNAME = jobContext.getParameter("SPARK_CASSANDRA_USERNAME");
        String SPARK_CASSANDRA_PASSWORD = jobContext.getParameter("SPARK_CASSANDRA_PASSWORD");
        String SPARK_CASSANDRA_DATACENTER = jobContext.getParameter("SPARK_CASSANDRA_DATACENTER");

        logger.info("🚀 CASSANDRA CREDENTIALS: SPARK_CASSANDRA_KEYSPACE_PM: " + SPARK_CASSANDRA_KEYSPACE_PM + " ✅");
        logger.info("🚀 CASSANDRA CREDENTIALS: SPARK_CASSANDRA_HOST: " + SPARK_CASSANDRA_HOST + " ✅");
        logger.info("🚀 CASSANDRA CREDENTIALS: SPARK_CASSANDRA_PORT: " + SPARK_CASSANDRA_PORT + " ✅");
        logger.info("🚀 CASSANDRA CREDENTIALS: SPARK_CASSANDRA_USERNAME: " + SPARK_CASSANDRA_USERNAME + " ✅");
        logger.info("🚀 CASSANDRA CREDENTIALS: SPARK_CASSANDRA_PASSWORD: " + SPARK_CASSANDRA_PASSWORD + " ✅");
        logger.info("🚀 CASSANDRA CREDENTIALS: SPARK_CASSANDRA_DATACENTER: " + SPARK_CASSANDRA_DATACENTER + " ✅");
        logger.info("🚀 CASSANDRA CREDENTIALS: CQL_TABLE_NAME: " + CQL_TABLE_NAME + " ✅");

        dataFrame = new CQLWriteCustom(
                dataFrame,
                6,
                "CELL LEVEL CQL WRITE",
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

        dataFrame.show(5);
        logger.info("🚀 CELL LEVEL CQL WRITE Executed Successfully! ✅");

        return dataFrame;
    }
}
