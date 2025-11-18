package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.pm.job.hourly.GetCounterAndKPIDetailAtDriver_YB;
import com.enttribe.pm.job.hourly.RegisterUDF_KPI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GET_COUNT_AND_KPI_DETAILS_AND_REGISTER_HOURLY_UDF {

    private static final Logger logger = LoggerFactory.getLogger(GET_COUNT_AND_KPI_DETAILS_AND_REGISTER_HOURLY_UDF.class);
    
    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        // GET COUNTER AND KPI DETAILS

        dataFrame = new GetCounterAndKPIDetailAtDriver_YB(1, "GET COUNTER AND KPI DETAILS")
                .executeAndGetResultDataframe(jobContext);

        logger.info("🚀 GET COUNTER AND KPI DETAILS Executed Successfully! ✅");

        // REGISTER UDF
        dataFrame = new RegisterUDF_KPI(dataFrame, 2, "REGISTER UDF", "TEMP_TABLE")
                .executeAndGetResultDataframe(jobContext);

        logger.info("🚀 REGISTER UDF Executed Successfully! ✅");

        return dataFrame;
    }
}
