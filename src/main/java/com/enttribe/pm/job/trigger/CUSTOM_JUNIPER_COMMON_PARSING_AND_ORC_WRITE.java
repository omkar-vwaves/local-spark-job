package com.enttribe.pm.job.trigger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.custom.processor.ExecuteSparkSQLD1Custom;
import com.enttribe.custom.processor.CacheDatasetCustom;
import com.enttribe.pm.job.quarterly.common.DeltaComputationUDF;
import com.enttribe.pm.job.quarterly.common.GetStaticMapForPMProcess;
import com.enttribe.pm.job.quarterly.common.UnzipToBytesForYB;
import com.enttribe.pm.job.quarterly.juniper.JuniperParserForDelta;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.pm.job.quarterly.juniper.JuniperParseDefinedCounter;
import com.enttribe.pm.job.quarterly.juniper.JuniperParseAllCounter;
import com.enttribe.custom.processor.ListMINIOFilesCustomV1;
import com.enttribe.custom.processor.RepartitionD1Custom;

public class CUSTOM_JUNIPER_COMMON_PARSING_AND_ORC_WRITE {

        private static final Logger logger = LoggerFactory.getLogger(CUSTOM_JUNIPER_COMMON_PARSING_AND_ORC_WRITE.class);

        public static Dataset<Row> callSubFlow(Dataset<Row> dataFrame, JobContext jobContext) throws Exception {

                /*
                 * GET STATIC MAP FOR PM PROCESSING
                 */

                dataFrame = new GetStaticMapForPMProcess(dataFrame, 1, "GET STATIC MAP FOR PM PROCESSING", "TEMP_TABLE")
                                .executeAndGetResultDataframe(jobContext);

                logger.info("🚀 GET STATIC MAP FOR PM PROCESSING Executed Successfully! ✅");

                /*
                 * JUNIPER CURRENT FILE PARSING UDF
                 */

                JuniperParserForDelta juniperParser = new JuniperParserForDelta();
                juniperParser.jobcontext = jobContext;
                jobContext.sqlctx().udf().register(juniperParser.getName(), juniperParser,
                                juniperParser.getReturnType());

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 2, "JUNIPER PARSING UDF",
                                "SELECT parsedData.raw_data_arr.* FROM (SELECT EXPLODE(JuniperParser(zipFilePath, fileName, xmlNameContent,false)) AS raw_data_arr FROM cacheRawBytesDataFrame ) AS parsedData",
                                "inputDF", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(5);
                logger.info("🚀 JUNIPER CURRENT FILE PARSING UDF Executed Successfully! ✅");

                /*
                 * EXPLODE AND NORMALIZE CURRENT FILE
                 */

                String EXPLODE_AND_NORMALIZE_SQL = "SELECT PT, regexp_replace(mapKey, '##\\d+$', '') AS baseKey, key AS counterName, value AS counterValue, CAST(value AS DOUBLE) AS counterValueDouble, CAST(value AS STRING) AS counterValueString FROM inputDF LATERAL VIEW explode(counterValueMap) t AS key, value";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 2, "EXPLODE AND NORMALIZE CURRENT FILE",
                                EXPLODE_AND_NORMALIZE_SQL,
                                "explodedDF", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 EXPLODE AND NORMALIZE CURRENT FILE Executed Successfully! ✅");

                /*
                 * AGGREGATE CURRENT FILE
                 */

                String AGGREGATE_SQL = "SELECT PT, baseKey, counterName, CASE WHEN MAX(counterValueDouble) IS NOT NULL THEN AVG(counterValueDouble) ELSE LAST(counterValueString, TRUE) END AS aggValue FROM explodedDF GROUP BY PT, baseKey, counterName";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 3, "AGGREGATE CURRENT FILE",
                                AGGREGATE_SQL,
                                "aggregatedDF", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 AGGREGATE CURRENT FILE Executed Successfully! ✅");

                /*
                 * COUNTER MAP CURRENT FILE
                 */

                String COUNTER_MAP_SQL = "SELECT PT, baseKey, map_from_entries( collect_list(named_struct('key', counterName, 'value', aggValue)) ) AS counterMap FROM aggregatedDF GROUP BY PT, baseKey";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 4, "COUNTER MAP CURRENT FILE",
                                COUNTER_MAP_SQL,
                                "counterMapDF", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 COUNTER MAP CURRENT FILE Executed Successfully! ✅");

                /*
                 * RESULT CURRENT FILE
                 */

                String RESULT_SQL = "SELECT PT, concat(regexp_extract(baseKey, '^(.*)##[0-9]+$', 1), '##') AS mapKey, counterMap AS counterValueMap FROM counterMapDF";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 5, "RESULT CURRENT FILE",
                                RESULT_SQL,
                                "ResultDF", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(5);
                logger.info("🚀 RESULT CURRENT FILE Executed Successfully! ✅");

                /*
                 * JUNIPER READ PREVIOUS FILE
                 */

                String PREVIOUS_RAW_FILE_PATH = jobContext.getParameter("PREVIOUS_RAW_FILE_PATH");
                String SPARK_MINIO_BUCKET_NAME_PM = jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM");

                dataFrame = new ListMINIOFilesCustomV1(
                                dataFrame,
                                6,
                                "LIST MINIO FILES PREVIOUS FILE",
                                SPARK_MINIO_BUCKET_NAME_PM + ":" + PREVIOUS_RAW_FILE_PATH,
                                "fileName",
                                SPARK_MINIO_BUCKET_NAME_PM,
                                "fileList",
                                "true").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 LIST MINIO FILES PREVIOUS FILE Processor Executed Successfully! ✅");

                /*
                 * REPARTITION FILE LIST
                 */

                dataFrame = new RepartitionD1Custom(
                                dataFrame,
                                7,
                                "REPARTITION FILE LIST PREVIOUS FILE",
                                "200",
                                "fileName",
                                "fileTable2").executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 REPARTITION FILE LIST PREVIOUS FILE Executed Successfully! ✅");

                /*
                 * UNZIP FILES UDF PREVIOUS FILE
                 */

                UnzipToBytesForYB unzipToBytesForYB = new UnzipToBytesForYB();
                unzipToBytesForYB.jobContext = jobContext;
                jobContext.sqlctx().udf().register(unzipToBytesForYB.getName(), unzipToBytesForYB,
                                unzipToBytesForYB.getReturnType());
                dataFrame = jobContext.sqlctx().sql(
                                "SELECT parsedData.raw_data_arr.* FROM (SELECT EXPLODE(UnzipToBytes(fileName)) AS raw_data_arr FROM fileTable2) AS parsedData");

                logger.info("🚀 UNZIP PREVIOUS FILE Executed Successfully! ✅");

                /*
                 * REPARTITION UNZIPPED PREVIOUS FILE DATA
                 */

                dataFrame = new RepartitionD1Custom(
                                dataFrame,
                                8,
                                "REPARTITION UNZIPPED PREVIOUS FILE DATA",
                                "200",
                                "partitionNumber",
                                "ExtractCsvfileListPartition2").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 REPARTITION UNZIPPED PREVIOUS FILE DATA Executed Successfully! ✅");

                /*
                 * CACHE PREVIOUS UNZIPPED FILE DATA
                 */

                dataFrame = new CacheDatasetCustom(
                                dataFrame,
                                9,
                                "CACHE PREVIOUS UNZIPPED FILE DATA",
                                "memoryAndDisk",
                                "cacheRawBytesDataFrame2").executeAndGetResultDataframe(jobContext);

                logger.info("🚀 CACHE PREVIOUS UNZIPPED FILE DATA Executed Successfully! ✅");

                /*
                 * JUNIPER PREVIOUS FILE PARSING UDF
                 */

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 12, "JUNIPER PREVIOUS FILE PARSING UDF",
                                "SELECT parsedData.raw_data_arr.* FROM (SELECT EXPLODE(JuniperParser(zipFilePath, fileName, xmlNameContent,false)) AS raw_data_arr FROM cacheRawBytesDataFrame2 ) AS parsedData",
                                "inputDF2", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(5);
                logger.info("🚀 JUNIPER PREVIOUS FILE PARSING UDF Executed Successfully! ✅");

                /*
                 * EXPLODE AND NORMALIZE PREVIOUS FILE
                 */

                String EXPLODE_AND_NORMALIZE_SQL2 = "SELECT PT, regexp_replace(mapKey, '##\\d+$', '') AS baseKey, key AS counterName, value AS counterValue, CAST(value AS DOUBLE) AS counterValueDouble, CAST(value AS STRING) AS counterValueString FROM inputDF2 LATERAL VIEW explode(counterValueMap) t AS key, value";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 13, "EXPLODE AND NORMALIZE PREVIOUS FILE",
                                EXPLODE_AND_NORMALIZE_SQL2,
                                "explodedDF2", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 EXPLODE AND NORMALIZE PREVIOUS FILE Executed Successfully! ✅");

                /*
                 * AGGREGATE PREVIOUS FILE
                 */

                String AGGREGATE_SQL2 = "SELECT PT, baseKey, counterName, CASE WHEN MAX(counterValueDouble) IS NOT NULL THEN AVG(counterValueDouble) ELSE LAST(counterValueString, TRUE) END AS aggValue FROM explodedDF2 GROUP BY PT, baseKey, counterName";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 14, "AGGREGATE PREVIOUS FILE",
                                AGGREGATE_SQL2,
                                "aggregatedDF2", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 AGGREGATE PREVIOUS FILE Executed Successfully! ✅");

                /*
                 * COUNTER MAP PREVIOUS FILE
                 */

                String COUNTER_MAP_SQL2 = "SELECT PT, baseKey, map_from_entries( collect_list(named_struct('key', counterName, 'value', aggValue)) ) AS counterMap FROM aggregatedDF2 GROUP BY PT, baseKey";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 15, "COUNTER MAP PREVIOUS FILE",
                                COUNTER_MAP_SQL2,
                                "counterMapDF2", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(false);
                logger.info("🚀 COUNTER MAP PREVIOUS FILE Executed Successfully! ✅");

                /*
                 * RESULT PREVIOUS FILE
                 */

                String RESULT_SQL2 = "SELECT PT, concat(regexp_extract(baseKey, '^(.*)##[0-9]+$', 1), '##') AS mapKey, counterMap AS counterValueMap FROM counterMapDF2";

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 16, "RESULT PREVIOUS FILE",
                                RESULT_SQL2,
                                "ResultDF2", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(5);
                logger.info("🚀 RESULT PREVIOUS FILE Executed Successfully! ✅");

                /*
                 * DELTA COMPUTATION UDF
                 */

                DeltaComputationUDF deltaComputationUDF = new DeltaComputationUDF();
                deltaComputationUDF.jobContext = jobContext;
                jobContext.sqlctx().udf().register(deltaComputationUDF.getName(), deltaComputationUDF,
                                deltaComputationUDF.getReturnType());

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 17, "DELTA COMPUTATION UDF",
                                "SELECT parsedData.raw_data_arr.* FROM( SELECT explode(DeltaComputationUDF(cr.PT, cr.mapKey, cr.counterValueMap, pr.counterValueMap)) AS raw_data_arr FROM ResultDF cr LEFT JOIN ResultDF2 pr ON concat_ws('##', split(cr.mapKey, '##')[0], split(cr.mapKey, '##')[1]) = concat_ws('##', split(pr.mapKey, '##')[0], split(pr.mapKey, '##')[1])) parsedData",
                                "deltaComputationData", null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 DELTA COMPUTATION UDF Executed Successfully! ✅");

                /*
                 * CACHE COMPUTED DELTA DATA
                 */

                dataFrame = new CacheDatasetCustom(dataFrame, 10, "CACHE COMPUTED DELTA DATA",
                                "memoryAndDisk",
                                "cacheFileParsedRawData").executeAndGetResultDataframe(jobContext);

                dataFrame.show(5);
                logger.info("🚀 CACHE COMPUTED DELTA DATA Executed Successfully! ✅");

                /*
                 * PARSE JUNIPER ALL COUNTER DATA FOR ORC WRITE
                 */

                JuniperParseAllCounter juniperParseAllCounter = new JuniperParseAllCounter();
                juniperParseAllCounter.jobcontext = jobContext;
                jobContext.sqlctx().udf().register(juniperParseAllCounter.getName(), juniperParseAllCounter,
                                juniperParseAllCounter.getReturnType());

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 4, "PARSE JUNIPER ALL COUNTER DATA FOR ORC WRITE",
                                "SELECT JuniperParseAllCounter(processingTime, parsedCounterMap) AS raw_data_arr_new FROM cacheFileParsedRawData",
                                "parseAllCounterData", null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 PARSE JUNIPER ALL COUNTER DATA FOR ORC WRITE Executed Successfully! ✅");

                /*
                 * EXPLODE ALL COUNTER DATA
                 */

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 5, "EXPLODE ALL COUNTER DATA",
                                "SELECT parsedData.raw_data_arr.* FROM (SELECT EXPLODE(raw_data_arr_new) AS raw_data_arr FROM parseAllCounterData) AS parsedData",
                                "parseAllCounter", null).executeAndGetResultDataframe(jobContext);

                dataFrame.show(5);
                logger.info("🚀 EXPLODE ALL COUNTER DATA Executed Successfully! ✅");

                /*
                 * SAVE ALL COUNTER DATA ORC TO MINIO
                 */

                // dataFrame = SAVE_ALL_COUNTER_DATA_ORC_TO_MINIO.callSubFlow(dataFrame, jobContext);

                logger.info("🚀 SAVE ALL COUNTER DATA ORC TO MINIO Executed Successfully! ✅");

                /**
                 * DEFINED COUNTER DATA
                 */

                JuniperParseDefinedCounter juniperParseDefinedCounter = new JuniperParseDefinedCounter();
                juniperParseDefinedCounter.jobcontext = jobContext;
                jobContext.sqlctx().udf().register(juniperParseDefinedCounter.getName(), juniperParseDefinedCounter,
                                juniperParseDefinedCounter.getReturnType());

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 6,
                                "PARSE JUNIPER DEFINED COUNTER DATA FOR ORC WRITE",
                                "SELECT JuniperParseDefinedCounter(processingTime, parsedCounterMap) AS raw_data_arr_new FROM cacheFileParsedRawData",
                                "parseDefinedCounterData", null).executeAndGetResultDataframe(jobContext);

                logger.info("🚀 PARSE JUNIPER DEFINED COUNTER DATA FOR ORC WRITE Executed Successfully! ✅");

                /*
                 * EXPLODE DEFINED COUNTER DATA
                 */

                dataFrame = new ExecuteSparkSQLD1Custom(dataFrame, 7, "EXPLODE DEFINED COUNTER DATA",
                                "SELECT parsedData.raw_data_arr.* FROM (SELECT EXPLODE(raw_data_arr_new) AS raw_data_arr FROM parseDefinedCounterData) AS parsedData",
                                "parseRawNodeData", null).executeAndGetResultDataframe(jobContext);

                // dataFrame.show(5);
                logger.info("🚀 EXPLODE DEFINED COUNTER DATA Executed Successfully! ✅");

                return dataFrame;
        }
}