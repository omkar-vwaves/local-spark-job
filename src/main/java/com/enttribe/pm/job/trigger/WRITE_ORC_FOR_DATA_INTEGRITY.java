package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.RepartitionD1Custom;
import com.enttribe.pm.job.quarterly.common.DIOrcUDF;
import com.enttribe.pm.job.quarterly.common.CustomParquetWriteForDI;
import com.enttribe.sparkrunner.context.JobContext;

public class WRITE_ORC_FOR_DATA_INTEGRITY {

    public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

        /*
         * REGISTER AND CALL UDF FOR DATA INTEGRITY
         */

        DIOrcUDF diOrcUDF = new DIOrcUDF();
        jobContext.sqlctx().udf().register(diOrcUDF.getName(), diOrcUDF, diOrcUDF.getReturnType());

        String PROCESSING_DATE = jobContext.getParameter("PROCESSING_DATE"); // YYMMDD
        String PROCESSING_HOUR = jobContext.getParameter("PROCESSING_HOUR"); // HHMM
        String JOB_TYPE = jobContext.getParameter("JOB_TYPE");
        String EMS_TYPE = jobContext.getParameter("EMS_TYPE");
        String TECHNOLOGY = jobContext.getParameter("TECHNOLOGY");

        String DATA_INTEGRITY_UDF_QUERY = "SELECT di.* FROM  (SELECT EXPLODE(DIOrcUDF(rowkey, metaData, '"
                + PROCESSING_DATE + "', '" + PROCESSING_HOUR + "', '" + JOB_TYPE + "','" + EMS_TYPE + "','"
                + TECHNOLOGY
                + "')) AS di FROM CacheNodeAggrRawData WHERE metaData['displaynodename'] IS NOT NULL ) AS nggr";

        dataFrame = new ExecuteSparkSQLD1Custom(
                dataFrame,
                1,
                "REGISTER AND CALL UDF FOR DATA INTEGRITY",
                DATA_INTEGRITY_UDF_QUERY,
                "DIOrcUDF",
                null).executeAndGetResultDataframe(jobContext);

        /*
         * REPARTITION BEFORE DI WRITE
         */

        dataFrame = new RepartitionD1Custom(
                dataFrame,
                2,
                "REPARTITION BEFORE DI WRITE",
                "2",
                null,
                "repartionedDIData")
                .executeAndGetResultDataframe(jobContext);

        /*
         * READ DATA INTEGRITY TRINO COLUMNS
         */

        String REPARTITION_BEFORE_DI_WRITE_QUERY = "SELECT ROW_KEY AS rowkey, TIMESTAMP AS timestamp, IS_EXIST AS e, PROCESSING_HOUR AS hr, ENTITY_NAME AS ne, ENTITY_TYPE AS nt, PROCESSING_DATE AS pd, IS_RECEIVED AS r, STATUS AS str, DOMAIN AS dn, VENDOR AS vr, EMS_TYPE AS ems, TECHNOLOGY AS t, ACTUAL_DATE AS ad, ACTUAL_TIME AS at, PROCESSING_TIME AS pt FROM repartionedDIData";

        dataFrame = new ExecuteSparkSQLD1Custom(dataFrame,
                3,
                "READ DATA INTEGRITY TRINO COLUMNS",
                REPARTITION_BEFORE_DI_WRITE_QUERY,
                "DataIntegrityReadData",
                null).executeAndGetResultDataframe(jobContext);

        /*
         * WRITE DATA INTEGRITY TRINO ORC
         */

        String DI_ORC_PATH = jobContext.getParameter("DI_ORC_PATH");
        dataFrame = new CustomParquetWriteForDI(dataFrame,
                4,
                "DI ORC WRITE TO SWF",
                "snappy",
                "dn vr ems t ad at pt",
                DI_ORC_PATH,
                "Overwrite",
                "100")
                .executeAndGetResultDataframe(jobContext);

        return dataFrame;
    }
}
