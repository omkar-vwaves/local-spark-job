package com.enttribe.pm.job.report.common;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;

import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class ProcessMetricDF extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessMetricDF.class);
    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";

    public ProcessMetricDF() {
        super();
        logger.info("ProcessMetricDF No Argument Constructor Called!");
    }

    public ProcessMetricDF(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("ProcessMetricDF Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public ProcessMetricDF(Integer id, String processorName) {
        super(id, processorName);
        logger.info("ProcessMetricDF Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("[ProcessMetricDF] Execution Started!");

        long startTime = System.currentTimeMillis();

        Dataset<Row> cqlResultDataFrame = this.dataFrame;

        Map<String, String> jobContextMap = jobContext.getParameters();

        SPARK_PM_JDBC_DRIVER = jobContextMap.get("SPARK_PM_JDBC_DRIVER");
        SPARK_PM_JDBC_URL = jobContextMap.get("SPARK_PM_JDBC_URL");
        SPARK_PM_JDBC_USERNAME = jobContextMap.get("SPARK_PM_JDBC_USERNAME");
        SPARK_PM_JDBC_PASSWORD = jobContextMap.get("SPARK_PM_JDBC_PASSWORD");

        logger.info("JDBC Credentials: Driver={}, URL={}, Username={}, Password={}", SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

        String reportWidgetDetails = jobContextMap.get("REPORT_WIDGET_DETAILS");
        String nodeAndAggregationDetails = jobContextMap.get("NODE_AND_AGGREGATION_DETAILS");
        String extraParameters = jobContextMap.get("EXTRA_PARAMETERS");
        String metaColumns = jobContextMap.get("META_COLUMNS");
        String kpiMapDetails = jobContextMap.get("KPI_MAP");
        String kpiGroupMapJson = jobContextMap.get("KPI_GROUP_MAP");

        if (reportWidgetDetails == null) {
            throw new Exception(
                    "REPORT_WIDGET_DETAILS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (nodeAndAggregationDetails == null) {
            throw new Exception(
                    "NODE_AND_AGGREGATION_DETAILS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (extraParameters == null) {
            throw new Exception(
                    "EXTRA_PARAMETERS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (metaColumns == null) {
            throw new Exception(
                    "META_COLUMNS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (kpiMapDetails == null) {
            throw new Exception(
                    "KPI_MAP is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (kpiGroupMapJson == null) {
            throw new Exception(
                    "KPI_GROUP_MAP is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        Map<String, String> reportWidgetDetailsMap = new ObjectMapper().readValue(reportWidgetDetails,
                new TypeReference<Map<String, String>>() {
                });

        String reportName = reportWidgetDetailsMap.get("reportName");
        String domain = reportWidgetDetailsMap.get("domain");
        String vendor = reportWidgetDetailsMap.get("vendor");
        String technology = reportWidgetDetailsMap.get("technology");
        String frequency = reportWidgetDetailsMap.get("frequency");
        jobContext.setParameters("reportName", reportName);
        jobContext.setParameters("domain", domain);
        jobContext.setParameters("vendor", vendor);
        jobContext.setParameters("technology", technology);
        jobContext.setParameters("frequency", frequency);

        logger.info("reportName={}, domain={}, vendor={}, technology={}, frequency={} Set to Job Context Successfully!",
                reportName, domain, vendor, technology, frequency);

        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(nodeAndAggregationDetails,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extraParameters,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> metaColumnsMap = new ObjectMapper().readValue(metaColumns,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> kpiMap = new ObjectMapper().readValue(kpiMapDetails,
                new TypeReference<Map<String, String>>() {
                });

        extraParametersMap.putAll(nodeAndAggregationDetailsMap);
        extraParametersMap.putAll(reportWidgetDetailsMap);

        logger.info("EXTRA_PARAMETERS: {}", extraParametersMap);

        Dataset<Row> expectedDataFrame = processWithGeneratedHeaders(cqlResultDataFrame, extraParametersMap, kpiMap,
                metaColumnsMap, jobContext);

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.info("[ProcessMetricDF] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes, seconds);

        if (expectedDataFrame != null && !expectedDataFrame.isEmpty()) {
            // expectedDataFrame.show(5, false);
            logger.info("++[ProcessMetricDF] Expected DataFrame Displayed Successfully!");
            int before = expectedDataFrame.rdd().getNumPartitions();
            logger.info("Number of Partitions Before Repartition: {}", before);
            expectedDataFrame = expectedDataFrame.repartition(100);
            int after = expectedDataFrame.rdd().getNumPartitions();
            logger.info("Number of Partitions After Repartition: {}", after);
        }

        return expectedDataFrame;
    }

    private Dataset<Row> processWithGeneratedHeaders(Dataset<Row> cqlResultDataFrame,
            Map<String, String> extraParametersMap, Map<String, String> kpiMap,
            Map<String, String> metaColumnsMap, JobContext jobContext) {

        logger.info("Processing With Generated Headers Started!");
        logger.info("Extra Parameters: {}", extraParametersMap);
        logger.info("KPI Map: {}", kpiMap);
        logger.info("Meta Columns: {}", metaColumnsMap);

        Dataset<Row> expectedDataFrame = cqlResultDataFrame;

        String reportFormatType = extraParametersMap.get("reportFormatType");
        jobContext.setParameters("REPORT_FORMAT_TYPE", reportFormatType);
        logger.info("REPORT_FORMAT_TYPE '{}' Set to Job Context Successfully!", reportFormatType);

        try {
            cqlResultDataFrame.createOrReplaceTempView("InputCQLData");

            String aggregationLevel = jobContext.getParameter("aggregationLevel");

            Dataset<Row> inputDataset = null;

            if ("L0".equals(aggregationLevel)) {

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, '-' AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, '-' AS ENTITY_ID, nodename AS ENTITY_NAME, kpijson FROM InputCQLData");

            } else if ("L1".equals(aggregationLevel)) {

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, nodename AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, '-' AS ENTITY_ID, nodename AS ENTITY_NAME, kpijson FROM InputCQLData");

            } else if ("L2".equals(aggregationLevel)) {

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(nodename) AS DL2, '-' AS DL3, '-' AS DL4, '-' AS ENTITY_ID, nodename AS ENTITY_NAME, kpijson FROM InputCQLData");

            } else if ("L3".equals(aggregationLevel)) {

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(metajson['DL2']) AS DL2, UPPER(nodename) AS DL3, '-' AS DL4, '-' AS ENTITY_ID, nodename AS ENTITY_NAME, kpijson FROM InputCQLData");

            } else if ("L4".equals(aggregationLevel)) {

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3, UPPER(nodename) AS DL4, '-' AS ENTITY_ID, nodename AS ENTITY_NAME, kpijson FROM InputCQLData");

            } else {

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT " +
                                "'INDIA' AS COUNTRY, " +
                                "UPPER(nodename) AS nodename, " +
                                "datalevel AS level, " +
                                "DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, " +
                                "UPPER(metajson['DL1']) AS DL1, " +
                                "UPPER(metajson['DL2']) AS DL2, " +
                                "UPPER(metajson['DL3']) AS DL3, " +
                                "UPPER(metajson['DL4']) AS DL4, " +

                                // ENTITY_ID Fallback Logic
                                "CASE " +
                                "WHEN metajson['ENTITY_ID'] IS NULL OR TRIM(metajson['ENTITY_ID']) = '' OR metajson['ENTITY_ID'] = '-' "
                                +
                                "THEN metajson['NEID'] " +
                                "ELSE metajson['ENTITY_ID'] " +
                                "END AS ENTITY_ID, " +

                                // ENTITY_NAME Fallback Logic
                                "CASE " +
                                "WHEN metajson['ENTITY_NAME'] IS NULL OR TRIM(metajson['ENTITY_NAME']) = '' OR metajson['ENTITY_NAME'] = '-' "
                                +
                                "THEN UPPER(metajson['NAM']) " +
                                "ELSE UPPER(metajson['ENTITY_NAME']) " +
                                "END AS ENTITY_NAME, " +

                                "kpijson " +
                                "FROM InputCQLData ORDER BY timestamp ASC");

                String selectedHeader = extraParametersMap.get("selectedHeader");
                if (selectedHeader.equalsIgnoreCase("default") && metaColumnsMap.containsKey("NODENAME")) {
                    metaColumnsMap.remove("NODENAME");
                    metaColumnsMap.put("ENTITY_ID", "Node");
                    metaColumnsMap.put("ENTITY_NAME", "Node Name");
                }

            }

            for (String colName : inputDataset.columns()) {
                inputDataset = inputDataset.withColumnRenamed(colName, colName.toUpperCase());
            }

            if (Arrays.asList(inputDataset.columns()).contains("DT")) {
                inputDataset = inputDataset.withColumn(
                        "DT",
                        regexp_replace(col("DT"), "-", "/"));
            }

            if (Arrays.asList(inputDataset.columns()).contains("HR")) {
                inputDataset = inputDataset.withColumn(
                        "HR",
                        regexp_replace(
                                lpad(col("HR").cast("string"), 4, "0"),
                                "(\\d{2})(\\d{2})",
                                "$1:$2"));
            }

            // inputDataset.show(5, false);
            logger.info("Input Dataset Displayed Successfully!");

            String domain = extraParametersMap.get("domain");
            String vendor = extraParametersMap.get("vendor");
            String technology = extraParametersMap.get("technology");
            String level = extraParametersMap.get("aggregationLevel");

            if (level == null) {
                level = jobContext.getParameters().get("aggregationLevel");
            }

            logger.info("Processing Parameters: Domain={}, Vendor={}, Technology={}, Level={}", domain, vendor,
                    technology, level);

            if (level.equals("L0") || level.equals("L1") || level.equals("L2") || level.equals("L3")
                    || level.equals("L4")) {

                logger.info("Aggregation Level is L0, L1, L2, L3 or L4, Continue With Report!");

                for (Map.Entry<String, String> entry : metaColumnsMap.entrySet()) {
                    String alias = entry.getKey();

                    if (!Arrays.asList(inputDataset.columns()).contains(alias)) {
                        inputDataset = inputDataset.withColumn(alias, functions.lit("-"));
                    }
                }

                inputDataset.createOrReplaceTempView("FixedCQLData");

                StringBuilder selectClause = new StringBuilder();

                for (Map.Entry<String, String> entry : metaColumnsMap.entrySet()) {
                    String columnName = entry.getKey();
                    String alias = entry.getValue();
                    selectClause.append(String.format("%s AS `%s`, ", columnName, alias));
                }

                logger.info("Select Clause After Meta Columns: {}", selectClause);

                for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
                    String kpiKey = entry.getKey();
                    String alias = entry.getValue();
                    selectClause.append(String.format("KPIJSON['%s'] AS `%s`, ", kpiKey, alias));
                }

                logger.info("Select Clause After KPI Columns: {}", selectClause);

                if (selectClause.length() > 2) {
                    selectClause.setLength(selectClause.length() - 2);
                }

                String sqlQuery = "SELECT " + selectClause + " FROM FixedCQLData";
                logger.info("Executing Dynamic Spark SQL Query: {}", sqlQuery);

                expectedDataFrame = jobContext.sqlctx().sql(sqlQuery);

                for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
                    String columnName = entry.getValue();

                    if (Arrays.asList(expectedDataFrame.columns()).contains(columnName)) {

                        expectedDataFrame = expectedDataFrame.withColumn(
                                columnName,
                                when(col(columnName).isNull()
                                        .or(col(columnName).equalTo("-")),
                                        lit(null))
                                        .otherwise(
                                                round(col(columnName).cast("double"), 4)));
                    }
                }

                expectedDataFrame.show(5, false);
                logger.info("Expected DataFrame Displayed Successfully!");

                return expectedDataFrame;

            } else {

                logger.info("Before Meta Columns Map: {}", metaColumnsMap);

                if (metaColumnsMap.containsKey("NODENAME")) {
                    metaColumnsMap.remove("NODENAME");
                    metaColumnsMap.put("ENTITY_ID", "Node");
                    metaColumnsMap.put("ENTITY_NAME", "Node Name");
                }

                logger.info("After Meta Columns Map: {}", metaColumnsMap);

                Dataset<Row> networkElement = getNetworkElement(domain, vendor, technology, jobContext);

                for (String colName : networkElement.columns()) {
                    networkElement = networkElement.withColumnRenamed(colName, colName.toUpperCase());
                }

                if (networkElement != null) {

                    // Dataset<Row> joinedDF = inputDataset
                    // .join(networkElement,
                    // inputDataset.col("NODENAME").equalTo(networkElement.col("NE_ID")),
                    // "INNER");

                    Dataset<Row> joinedDF = inputDataset
                            .join(networkElement,
                                    inputDataset.col("NODENAME").equalTo(networkElement.col("NE_ID")),
                                    "LEFT");

                    for (Map.Entry<String, String> entry : metaColumnsMap.entrySet()) {
                        String alias = entry.getValue();

                        if (!Arrays.asList(joinedDF.columns()).contains(alias)) {
                            joinedDF = joinedDF.withColumn(alias, functions.lit("-"));
                        }
                    }

                    joinedDF.createOrReplaceTempView("JoinedCQLData");

                    StringBuilder selectClause = new StringBuilder();

                    for (Map.Entry<String, String> entry : metaColumnsMap.entrySet()) {
                        String columnName = entry.getKey();
                        String alias = entry.getValue();
                        selectClause.append(String.format("%s AS `%s`, ", columnName, alias));

                    }

                    for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
                        String kpiKey = entry.getKey();
                        String alias = entry.getValue();
                        selectClause.append(String.format("KPIJSON['%s'] AS `%s`, ", kpiKey, alias));

                    }

                    if (selectClause.length() > 2) {
                        selectClause.setLength(selectClause.length() - 2);
                    }

                    String sqlQuery = "SELECT " + selectClause + " FROM JoinedCQLData";
                    logger.info("Executing Dynamic Spark SQL Query: {}", sqlQuery);

                    expectedDataFrame = jobContext.sqlctx().sql(sqlQuery);

                    for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
                        String columnName = entry.getValue();

                        if (Arrays.asList(expectedDataFrame.columns()).contains(columnName)) {

                            expectedDataFrame = expectedDataFrame.withColumn(
                                    columnName,
                                    when(col(columnName).isNull()
                                            .or(col(columnName).equalTo("-")),
                                            lit(null))
                                            .otherwise(
                                                    round(col(columnName).cast("double"), 4)));
                        }
                    }

                    return expectedDataFrame;

                } else {
                    logger.error("Network Element is Null, Continue With Empty Report!");
                }
            }

        } catch (Exception e) {
            logger.error("Error in Processing With Generated Headers, Message: {}, Error: {}", e.getMessage(), e);
        }

        return expectedDataFrame;
    }

    private static Dataset<Row> getNetworkElement(String domain, String vendor, String technology,
            JobContext jobContext) {

        String sqlQuery = "SELECT * FROM NETWORK_ELEMENT WHERE DOMAIN = '" + domain + "' AND VENDOR = '" + vendor
                + "' AND TECHNOLOGY = '" + technology + "'";

        logger.info("Executing Network Element Query: {}", sqlQuery);

        return executeQuery(sqlQuery, jobContext);

    }

    private static Dataset<Row> executeQuery(String sqlQuery, JobContext jobContext) {

        Dataset<Row> resultDataset = null;

        try {
            resultDataset = jobContext.sqlctx().read()
                    .format("jdbc")
                    .option("driver", SPARK_PM_JDBC_DRIVER)
                    .option("url", SPARK_PM_JDBC_URL)
                    .option("user", SPARK_PM_JDBC_USERNAME)
                    .option("password", SPARK_PM_JDBC_PASSWORD)
                    .option("query", sqlQuery)
                    .load();

            return resultDataset;

        } catch (Exception e) {
            logger.error("Exception in Executing Query, Message: " + e.getMessage() + " | Error: " + e);
            return resultDataset;
        }

    }

}
