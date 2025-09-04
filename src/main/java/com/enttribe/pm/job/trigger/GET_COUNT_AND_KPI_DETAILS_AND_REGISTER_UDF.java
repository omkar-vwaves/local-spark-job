package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.pm.job.quarterly.common.GetCounterAndKPIDetails;
import com.enttribe.pm.job.quarterly.common.RegisterUDF_KPI;
import com.enttribe.sparkrunner.context.JobContext;

public class GET_COUNT_AND_KPI_DETAILS_AND_REGISTER_UDF {

    private static final Logger logger = LoggerFactory.getLogger(GET_COUNT_AND_KPI_DETAILS_AND_REGISTER_UDF.class);

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        /*
         * GET COUNT AND KPI DETAILS
         */

        dataFrame = new GetCounterAndKPIDetails(1, "GET COUNT AND KPI DETAILS")
                .executeAndGetResultDataframe(jobContext);

        logger.info("🚀 GET COUNT AND KPI DETAILS Executed Successfully! ✅");

        /*
         * REGISTER UDF
         */

        dataFrame = new RegisterUDF_KPI(dataFrame, 2, "REGISTER UDF", "TEMP_TABLE")
                .executeAndGetResultDataframe(jobContext);

        logger.info("🚀 REGISTER UDF Executed Successfully! ✅");

        return dataFrame;
    }
}
