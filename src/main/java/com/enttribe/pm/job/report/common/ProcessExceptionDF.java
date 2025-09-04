package com.enttribe.pm.job.report.common;

import java.util.Map;
import java.util.stream.Collectors;

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
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ProcessExceptionDF extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessExceptionDF.class);
    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";

    public ProcessExceptionDF() {
        super();
        logger.info("ProcessExceptionDF No Argument Constructor Called!");
    }

    public ProcessExceptionDF(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("ProcessExceptionDF Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public ProcessExceptionDF(Integer id, String processorName) {
        super(id, processorName);
        logger.info("ProcessExceptionDF Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("[ProcessExceptionDF] Execution Started!");

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

        String ragConfiguration = jobContextMap.get("RAG_CONFIGURATION");

        jobContext.setParameters("RAG_CONFIGURATION", ragConfiguration);
        logger.info("RAG_CONFIGURATION Set to Job Context Successfully! ✅");

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

        if (ragConfiguration == null) {
            throw new Exception(
                    "RAG_CONFIGURATION is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
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

        logger.info("Report Name '{}' Set to Job Context Successfully! ✅", reportName);
        logger.info("Domain '{}' Set to Job Context Successfully! ✅", domain);
        logger.info("Vendor '{}' Set to Job Context Successfully! ✅", vendor);
        logger.info("Technology '{}' Set to Job Context Successfully! ✅", technology);
        logger.info("Frequency '{}' Set to Job Context Successfully! ✅", frequency);

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

        logger.info("📊 EXTRA_PARAMETERS: {}", extraParametersMap);

        Dataset<Row> expectedDataFrame = processWithGeneratedHeaders(cqlResultDataFrame, extraParametersMap, kpiMap,
                metaColumnsMap, jobContext);

        expectedDataFrame = filterExceptionData(expectedDataFrame, extraParametersMap, kpiMap, metaColumnsMap,
                jobContext);

        expectedDataFrame.show();
        logger.info("Expected Data Displayed Successfully! ✅");

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.info("[ProcessExceptionDF] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes, seconds);

        return expectedDataFrame;
    }

    private Dataset<Row> filterExceptionData(Dataset<Row> expectedDataFrame, Map<String, String> extraParametersMap,
            Map<String, String> kpiMap,
            Map<String, String> metaColumnsMap, JobContext jobContext) {

        Dataset<Row> updatedDataFrame = expectedDataFrame;

        try {

            // String expression = extraParametersMap.get("kpiExpression");
            String configuration = extraParametersMap.get("configuration");
            String expression = generateKPIExpression(configuration);
            logger.info("🔍 Custom KPI Expression: {}", expression);

            StructType schema = expectedDataFrame.schema();
            logger.info("🔍 Schema: {}", schema);

            List<Row> rows = expectedDataFrame.collectAsList();
            List<Row> updatedRows = new ArrayList<>();

            if (expression != null && !expression.isEmpty() && !expression.equals("NULL")) {
                for (Row row : rows) {
                    boolean isRowValid = isRowValid(row, kpiMap, expression);
                    if (isRowValid) {
                        updatedRows.add(row);
                    }
                }
            } else {
                updatedRows = rows;
            }

            updatedDataFrame = jobContext.sqlctx().createDataFrame(updatedRows, schema);
        } catch (Exception e) {
            logger.error("Error in Filtering Exception Data, Message: {}, Error: {}", e.getMessage(), e);
        }

        return updatedDataFrame;
    }

    // public static String generateKPIExpression(String configuration) {

    // String fixedJson = configuration.replace("'", "\"");
    // try {
    // JSONObject configJson = new JSONObject(fixedJson);
    // String kpiExpression = "";
    // boolean isFirstCondition = true;

    // if (configJson.has("kpi")) {
    // JSONArray kpiArray = configJson.getJSONArray("kpi");
    // for (int i = 0; i < kpiArray.length(); i++) {
    // JSONObject kpi = kpiArray.getJSONObject(i);
    // if (kpi.optBoolean("Threshold", false)) {
    // String kpiCode = kpi.getString("kpicode");
    // String thresholdConditions = kpi.getString("threshold_conditions");
    // String logicalCondition = kpi.getString("logical_conditions");

    // System.out.println("KPI Code: " + kpiCode);
    // System.out.println("Threshold Conditions: " + thresholdConditions);
    // System.out.println("Logical Condition: " + logicalCondition);

    // String condition;
    // if (thresholdConditions.startsWith("Between#")) {
    // // Extract numbers from Between#10to20 format
    // String[] parts = thresholdConditions.replace("Between#", "").split("to");
    // if (parts.length == 2) {
    // String lowerBound = parts[0];
    // String upperBound = parts[1];
    // condition = "(KPI#" + kpiCode + " >= " + lowerBound + " && KPI#" + kpiCode +
    // " <= "
    // + upperBound + ")";
    // } else {
    // condition = "(KPI#" + kpiCode + " " + thresholdConditions + ")";
    // }
    // } else {
    // condition = "(KPI#" + kpiCode + " " + thresholdConditions + ")";
    // }

    // if (isFirstCondition) {
    // kpiExpression = condition;
    // isFirstCondition = false;
    // } else {
    // if (logicalCondition.equals("OR")) {
    // kpiExpression += " || " + condition;
    // } else {
    // kpiExpression += " && " + condition;
    // }
    // }
    // }
    // }
    // }
    // return kpiExpression;
    // } catch (Exception e) {
    // System.err.println("Error generating KPI expression: " + e.getMessage());
    // return "";
    // }
    // }

    public static String generateKPIExpression(String configuration) {
        // Replace single quotes with double quotes for valid JSON parsing
        String fixedJson = configuration.replace("'", "\"");

        try {
            JSONObject configJson = new JSONObject(fixedJson);
            if (!configJson.has("kpi"))
                return "";

            JSONArray kpiArray = configJson.getJSONArray("kpi");
            List<String> expressions = new ArrayList<>();

            for (int i = 0; i < kpiArray.length(); i++) {
                JSONObject kpi = kpiArray.getJSONObject(i);
                if (!kpi.optBoolean("Threshold", false))
                    continue;

                String kpiCode = kpi.getString("kpicode");
                String threshold = kpi.getString("threshold_conditions").trim();

                String condition;
                if (threshold.startsWith("Between#")) {
                    // Parse range for Between condition
                    String[] range = threshold.substring("Between#".length()).split("to");
                    if (range.length == 2) {
                        condition = "(KPI#" + kpiCode + " >= " + range[0].trim() + " && KPI#" + kpiCode + " <= "
                                + range[1].trim() + ")";
                    } else {
                        // fallback if split fails
                        condition = "KPI#" + kpiCode + " " + threshold;
                    }
                } else {
                    // Other comparison operators
                    condition = "KPI#" + kpiCode + " " + threshold;
                }

                expressions.add(condition);
            }

            if (expressions.isEmpty()) {
                return "";
            }

            // Build combined expression with logical operators from each KPI except last
            StringBuilder finalExpr = new StringBuilder();
            for (int i = 0; i < expressions.size(); i++) {
                finalExpr.append(expressions.get(i));

                // Append logical operator if not the last KPI expression
                if (i < expressions.size() - 1) {
                    // Use the logical operator from the current KPI
                    String logic = kpiArray.getJSONObject(i).optString("logical_conditions", "AND").trim()
                            .toUpperCase();
                    if ("AND".equals(logic)) {
                        finalExpr.append(" && ");
                    } else if ("OR".equals(logic)) {
                        finalExpr.append(" || ");
                    } else {
                        // Default fallback
                        finalExpr.append(" && ");
                    }
                }
            }

            return "(" + finalExpr.toString() + ")";

        } catch (Exception e) {
            System.err.println("❌ Error generating KPI expression: " + e.getMessage());
            return "";
        }
    }

    private static boolean isRowValid(Row row, Map<String, String> kpiMap, String expression) {

        try {
            // String expression = extraParameters.get("kpiExpression");
            // String optimizedExpression = optimizeExpression(expression, row, kpiMap);

            Map<String, String> kpiValueMap = getKPIValueMap(row, kpiMap);
            String result = parseExpression(expression, kpiValueMap);
            boolean isTrue = isValidExpression(result);

            logger.info("🔍 KPI Value Map: {}", kpiValueMap);
            logger.info("🔍 Result: {}", result);
            logger.info("🔍 Is Valid Expression: {}", isTrue);

            return isTrue;
            // logger.info("🔍 Optimized Expression: {}", optimizedExpression);

            // if (isExpressionTrue(optimizedExpression)) {
            // return true;
            // } else {
            // return false;
            // }
        } catch (Exception e) {
            logger.error("Error in Checking if Row is Valid, Message: {}, Error: {}", e.getMessage(), e);
            return false;
        }
    }

    public static boolean isValidExpression(String expression) {

        try {
            com.enttribe.sparkrunner.util.Expression evaluatedExpression = new com.enttribe.sparkrunner.util.Expression(
                    expression);
            String result = evaluatedExpression.eval();
            return result.equalsIgnoreCase("1");
        } catch (Exception e) {
            return false;
        }
    }

    public static String parseExpression(String expression, Map<String, String> kpiMap) {
        String result = expression;
        for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
            String kpiCode = entry.getKey();
            String kpiValue = entry.getValue();
            result = result.replace("KPI#" + kpiCode, kpiValue);
        }
        return result;
    }

    // private static boolean isExpressionTrue(String expression) {

    // try {
    // com.enttribe.sparkrunner.util.Expression evaluatedExpression = new
    // com.enttribe.sparkrunner.util.Expression(
    // expression);
    // String result = evaluatedExpression.eval();
    // if (result.equalsIgnoreCase("1")) {
    // return true;
    // } else {
    // return false;
    // }
    // } catch (Exception e) {
    // logger.error("Error in Evaluating Expression={}, Message={}, Error={}",
    // expression, e.getMessage(), e);
    // return false;
    // }
    // }

    private static Map<String, String> getKPIValueMap(Row row, Map<String, String> kpiMap) {
        Map<String, String> kpiValueMap = new HashMap<>();

        if (row == null || kpiMap == null || kpiMap.isEmpty()) {
            logger.error("Row or KPI map is NULL/EMPTY - Returning Empty Map");
            return kpiValueMap;
        }

        try {
            for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
                if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }

                String kpiCode = entry.getKey();
                String kpiName = entry.getValue();

                try {
                    Object kpiValueObj = row.getAs(kpiName);
                    String kpiValue = null;

                    if (kpiValueObj != null) {
                        if (kpiValueObj instanceof Number) {
                            try {
                                kpiValue = String.valueOf(((Number) kpiValueObj).doubleValue());
                            } catch (NumberFormatException nfe) {
                                logger.error("Failed to Convert Number Value for KPI {}: {}", kpiName,
                                        nfe.getMessage());
                                continue;
                            }
                        } else {
                            kpiValue = kpiValueObj.toString();
                        }
                    }

                    if (kpiValue != null && !kpiValue.equalsIgnoreCase("NULL")) {
                        kpiValueMap.put(kpiCode, kpiValue);
                    }
                } catch (IllegalArgumentException e) {
                    logger.error("Column {} Not Found in Row - Skipping", kpiName);
                    continue;
                }
            }
        } catch (Exception e) {
            logger.error("Error in Getting KPI Value Map, Message: {}, Error: {}", e.getMessage(), e);
        }

        return kpiValueMap;
    }

    // private static String optimizeExpression(String expression, Row row,
    // Map<String, String> kpiMap) {

    // Map<String, String> kpiValueMap = getKPIValueMap(row, kpiMap);
    // logger.info("🔍 KPI Value Map: {}", kpiValueMap);
    // StringBuilder optimizedExpression = new StringBuilder(expression);

    // if (expression.contains("KPI#")) {
    // String[] exp = expression.split("KPI#");
    // int i = 0;
    // for (String kpi : exp) {
    // if (i++ != 0) {
    // int endIndex = kpi.indexOf(')');
    // String kpiId = (endIndex != -1) ? kpi.substring(0, endIndex) : kpi;

    // if (kpiValueMap.containsKey(kpiId)) {
    // String kpiValue = kpiValueMap.get(kpiId);
    // String replacement = (kpiValue.equalsIgnoreCase("-")) ? "NULL" : kpiValue;

    // int startIndex = optimizedExpression.indexOf("((" + "KPI#" + kpiId + "))");
    // if (startIndex != -1) {
    // optimizedExpression.replace(startIndex,
    // startIndex + ("((" + "KPI#" + kpiId + "))").length(), replacement);
    // }
    // }
    // }
    // }
    // }
    // return optimizedExpression.toString();
    // }

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
        logger.info("REPORT_FORMAT_TYPE '{}' Set to Job Context Successfully! ✅", reportFormatType);

        try {
            cqlResultDataFrame.createOrReplaceTempView("InputCQLData");

            // Dataset<Row> inputDataset = jobContext.sqlctx().sql(
            // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
            // metajson['DT'] AS
            // DT, metajson['HR'] AS HR, metajson['DL1'] AS DL1,metajson['DL2'] AS
            // DL2,metajson['DL3'] AS DL3, metajson['DL4'] AS DL4, metajson['NEID'] AS NEID,
            // metajson['NAM'] AS NAM, kpijson FROM InputCQLData");

            String aggregationLevel = jobContext.getParameter("aggregationLevel");

            Dataset<Row> inputDataset = null;

            if ("L0".equals(aggregationLevel)) {

                // inputDataset = jobContext.sqlctx().sql(
                // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
                // DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS
                // HR, '-' AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, '-' AS NEID,
                // UPPER(metajson['NAM']) AS NAM, kpijson FROM InputCQLData");
                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, DATE_FORMAT(timestamp, 'dd-MM-yyyy') AS DT, DATE_FORMAT(timestamp, 'HHmm') AS HR, '-' AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, '-' AS NEID, UPPER(metajson['ENTITY_NAME']) AS NAM, kpijson FROM InputCQLData");

            } else if ("L1".equals(aggregationLevel)) {

                // inputDataset = jobContext.sqlctx().sql(
                // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
                // metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1,
                // '-' AS DL2, '-' AS DL3, '-' AS DL4, '-' AS NEID, UPPER(metajson['NAM']) AS
                // NAM, kpijson FROM InputCQLData");
                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, '-' AS NEID, UPPER(metajson['ENTITY_NAME']) AS NAM, kpijson FROM InputCQLData");

            } else if ("L2".equals(aggregationLevel)) {

                // inputDataset = jobContext.sqlctx().sql(
                // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
                // metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1,
                // UPPER(metajson['DL2']) AS DL2, '-' AS DL3, '-' AS DL4, '-' AS NEID,
                // UPPER(metajson['NAM']) AS NAM, kpijson FROM InputCQLData");
                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(metajson['DL2']) AS DL2, '-' AS DL3, '-' AS DL4, '-' AS NEID, UPPER(metajson['ENTITY_NAME']) AS NAM, kpijson FROM InputCQLData");

            } else if ("L3".equals(aggregationLevel)) {

                // inputDataset = jobContext.sqlctx().sql(
                // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
                // metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1,
                // UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3, '-' AS DL4, '-'
                // AS NEID, UPPER(metajson['NAM']) AS NAM, kpijson FROM InputCQLData");
                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3, '-' AS DL4, '-' AS NEID, UPPER(metajson['ENTITY_NAME']) AS NAM, kpijson FROM InputCQLData");

            } else if ("L4".equals(aggregationLevel)) {

                // inputDataset = jobContext.sqlctx().sql(
                // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
                // metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1,
                // UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3,
                // UPPER(metajson['DL4']) AS DL4, '-' AS NEID, UPPER(metajson['NAM']) AS NAM,
                // kpijson FROM InputCQLData");

                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3, UPPER(metajson['DL4']) AS DL4, '-' AS NEID, UPPER(metajson['ENTITY_NAME']) AS NAM, kpijson FROM InputCQLData");
            } else {

                // inputDataset = jobContext.sqlctx().sql(
                // "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level,
                // metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1,
                // UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3,
                // UPPER(metajson['DL4']) AS DL4, metajson['NEID'] AS NEID,
                // UPPER(metajson['NAM']) AS NAM, kpijson FROM InputCQLData");
                inputDataset = jobContext.sqlctx().sql(
                        "SELECT 'INDIA' AS COUNTRY, UPPER(nodename) AS nodename, datalevel AS level, metajson['DT'] AS DT, metajson['HR'] AS HR, UPPER(metajson['DL1']) AS DL1, UPPER(metajson['DL2']) AS DL2, UPPER(metajson['DL3']) AS DL3, UPPER(metajson['DL4']) AS DL4, metajson['ENTITY_ID'] AS ENTITY_ID, UPPER(metajson['ENTITY_NAME']) AS ENTITY_NAME, kpijson FROM InputCQLData");
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

            inputDataset.show();
            logger.info("Input Dataset Displayed Successfully! ✅");

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

                logger.info("Aggregation Level is L0, L1, L2, L3 or L4, Continue With Report! 📄");

                for (Map.Entry<String, String> entry : metaColumnsMap.entrySet()) {
                    String alias = entry.getKey();

                    if (!Arrays.asList(inputDataset.columns()).contains(alias)) {
                        inputDataset = inputDataset.withColumn(alias, functions.lit("-"));
                    }
                }

                inputDataset.show();
                logger.info("Generated Dataset Displayed Successfully! ✅");

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

                expectedDataFrame.show();
                logger.info("Expected DataFrame Displayed Successfully! ✅");

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

                expectedDataFrame.show();
                logger.info("DataFrame After KPI Rounding Displayed Successfully! ✅");

                return expectedDataFrame;

            } else {

                Dataset<Row> networkElement = getNetworkElement(domain, vendor, technology, jobContext);

                for (String colName : networkElement.columns()) {
                    networkElement = networkElement.withColumnRenamed(colName, colName.toUpperCase());
                }
                // networkElement = networkElement.withColumnRenamed("NEID", "NE_ID");

                if (networkElement != null) {

                    // networkElement.show();
                    // logger.info("Network Element Displayed Successfully! ✅");

                    Dataset<Row> joinedDF = inputDataset
                            .join(networkElement,
                                    inputDataset.col("ENTITY_ID").equalTo(networkElement.col("NE_ID")),
                                    "LEFT");

                    joinedDF.show();
                    logger.info("Joined Data Displayed Successfully! ✅");

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
                    logger.error("⚠️ Network Element is Null, Continue With Empty Report! 📄");
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
