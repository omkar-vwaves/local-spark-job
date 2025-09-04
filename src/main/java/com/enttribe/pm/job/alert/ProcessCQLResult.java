package com.enttribe.pm.job.alert;

import com.enttribe.sparkrunner.processors.Processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enttribe.sparkrunner.util.Expression;

public class ProcessCQLResult extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ProcessCQLResult.class);
    private static Map<String, String> jobContextMap = new HashMap<>();

    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";

    private static String SPARK_FM_JDBC_DRIVER = "SPARK_FM_JDBC_DRIVER";
    private static String SPARK_FM_JDBC_URL = "SPARK_FM_JDBC_URL";
    private static String SPARK_FM_JDBC_USERNAME = "SPARK_FM_JDBC_USERNAME";
    private static String SPARK_FM_JDBC_PASSWORD = "SPARK_FM_JDBC_PASSWORD";

    private static String SPARK_CASSANDRA_KEYSPACE_PM = "SPARK_CASSANDRA_KEYSPACE_PM";
    private static String SPARK_CASSANDRA_HOST = "SPARK_CASSANDRA_HOST";
    private static String SPARK_CASSANDRA_PORT = "SPARK_CASSANDRA_PORT";
    private static String SPARK_CASSANDRA_DATACENTER = "SPARK_CASSANDRA_DATACENTER";
    private static String SPARK_CASSANDRA_USERNAME = "SPARK_CASSANDRA_USERNAME";
    private static String SPARK_CASSANDRA_PASSWORD = "SPARK_CASSANDRA_PASSWORD";

    public ProcessCQLResult() {
        super();
        logger.info("ProcessCQLResult No Argument Constructor Called!");
    }

    public ProcessCQLResult(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("ProcessCQLResult Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public ProcessCQLResult(Integer id, String processorName) {
        super(id, processorName);
        logger.info("ProcessCQLResult Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        long startTime = System.currentTimeMillis();

        logger.info("[ProcessCQLResult] Execution Started!");

        jobContextMap = jobContext.getParameters();

        SPARK_PM_JDBC_DRIVER = jobContextMap.get(SPARK_PM_JDBC_DRIVER);
        SPARK_PM_JDBC_URL = jobContextMap.get(SPARK_PM_JDBC_URL);
        SPARK_PM_JDBC_USERNAME = jobContextMap.get(SPARK_PM_JDBC_USERNAME);
        SPARK_PM_JDBC_PASSWORD = jobContextMap.get(SPARK_PM_JDBC_PASSWORD);

        SPARK_FM_JDBC_DRIVER = jobContextMap.get(SPARK_FM_JDBC_DRIVER);
        SPARK_FM_JDBC_URL = jobContextMap.get(SPARK_FM_JDBC_URL);
        SPARK_FM_JDBC_USERNAME = jobContextMap.get(SPARK_FM_JDBC_USERNAME);
        SPARK_FM_JDBC_PASSWORD = jobContextMap.get(SPARK_FM_JDBC_PASSWORD);

        SPARK_CASSANDRA_KEYSPACE_PM = jobContextMap.get(SPARK_CASSANDRA_KEYSPACE_PM);
        SPARK_CASSANDRA_HOST = jobContextMap.get(SPARK_CASSANDRA_HOST);
        SPARK_CASSANDRA_PORT = jobContextMap.get(SPARK_CASSANDRA_PORT);
        SPARK_CASSANDRA_DATACENTER = jobContextMap.get(SPARK_CASSANDRA_DATACENTER);
        SPARK_CASSANDRA_USERNAME = jobContextMap.get(SPARK_CASSANDRA_USERNAME);
        SPARK_CASSANDRA_PASSWORD = jobContextMap.get(SPARK_CASSANDRA_PASSWORD);

        jobContext = setSparkConf(jobContext);

        logger.info("PM JDBC Credentials: Driver={}, URL={}, User={}, Password={}", SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

        logger.info("FM JDBC Credentials: Driver={}, URL={}, User={}, Password={}", SPARK_FM_JDBC_DRIVER,
                SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME, SPARK_FM_JDBC_PASSWORD);

        logger.info("Cassandra Credentials: Keyspace={}, Host={}, Port={}, Datacenter={}, Username={}, Password={}",
                SPARK_CASSANDRA_KEYSPACE_PM,
                SPARK_CASSANDRA_HOST, SPARK_CASSANDRA_PORT, SPARK_CASSANDRA_DATACENTER, SPARK_CASSANDRA_USERNAME,
                SPARK_CASSANDRA_PASSWORD);

        Dataset<Row> inputDF = this.dataFrame;
        if (inputDF.isEmpty()) {
            logger.info("Input DataFrame Is Empty. Returning Empty DataFrame.");
            return inputDF;
        }
        inputDF.createOrReplaceTempView("CQLResult");

        String aggregationLevel = jobContextMap.get("aggregationLevel");
        String timestamp = jobContextMap.get("TIMESTAMP");

        logger.info("⏰ Processing Timestamp: {} | 📊 Aggregation Level: {}", timestamp, aggregationLevel);

        if (this.dataFrame == null) {
            logger.info("DataFrame Is NULL. Returning Empty DataFrame.");
            return this.dataFrame;
        }

        String inputConfig = null;
        String extractedParameters = null;
        String nodeAndAggregationDetails = null;

        Object inoutConfigurationsObj = jobContextMap.get("INPUT_CONFIGURATIONS");
        inputConfig = inoutConfigurationsObj != null ? inoutConfigurationsObj.toString() : null;
        if (inputConfig == null) {
            logger.info("INPUT_CONFIGURATIONS is NULL. Returning Empty DataFrame.");
            return this.dataFrame;
        }

        jobContext.setParameters("INPUT_CONFIGURATIONS", inputConfig);
        logger.info("📊 INPUT_CONFIGURATIONS Set to Job Context Successfully! ✅");

        Object nodeAndAggregationDetailsObj = jobContextMap.get("NODE_AND_AGGREGATION_DETAILS");
        nodeAndAggregationDetails = nodeAndAggregationDetailsObj != null ? nodeAndAggregationDetailsObj.toString()
                : null;
        if (nodeAndAggregationDetails == null) {
            logger.info("NODE_AND_AGGREGATION_DETAILS is NULL. Returning Empty DataFrame.");
            return this.dataFrame;
        }

        jobContext.setParameters("NODE_AND_AGGREGATION_DETAILS", nodeAndAggregationDetails);
        logger.info("📊 NODE_AND_AGGREGATION_DETAILS Set to Job Context Successfully! ✅");

        Object extractedParametersObj = jobContextMap.get("EXTRACTED_PARAMETERS");
        extractedParameters = extractedParametersObj != null ? extractedParametersObj.toString() : null;
        if (extractedParameters == null) {
            logger.info("EXTRACTED_PARAMETERS is NULL. Returning Empty DataFrame.");
            return this.dataFrame;
        }

        jobContext.setParameters("EXTRACTED_PARAMETERS", extractedParameters);
        logger.info("📊 EXTRACTED_PARAMETERS Set to Job Context Successfully! ✅");

        Map<String, String> inputConfigMap = new ObjectMapper().readValue(inputConfig,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(nodeAndAggregationDetails,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extractedParameters,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> finalMap = new HashMap<>();

        finalMap.putAll(inputConfigMap);
        finalMap.putAll(nodeAndAggregationDetailsMap);
        finalMap.putAll(extraParametersMap);

        logger.info("📊 Input Configurations : {}", inputConfigMap);
        logger.info("📊 Node and Aggregation Details: {}", nodeAndAggregationDetailsMap);
        logger.info("📊 Extracted Parameters: {}", extraParametersMap);

        String cqlQuery = buildCQLQuery(finalMap, aggregationLevel, jobContext);
        logger.info("📊 CQL Query On InputDF : {}", cqlQuery);

        Dataset<Row> cqlResultDF = jobContext.sqlctx().sql(cqlQuery);
        cqlResultDF.show(false);

        List<Row> rowList = cqlResultDF.collectAsList();

        if (rowList.isEmpty()) {
            logger.info(
                    "No Data Found in Cassandra for the Given Filter Conditions. Skipping Alert Generation Process!");
            return this.dataFrame;
        }

        int rowNumber = 1;
        for (Row row : rowList) {

            logger.info("🔄 Processing CQL {} With Nodename: {}", rowNumber, row.getAs("nodename"));
            processEachCQLRow(row, extraParametersMap, jobContext, inputConfigMap);
            logger.info("Processing CQL {} With Nodename: {} Completed! ✅", rowNumber++, row.getAs("nodename"));
        }

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.info("⏱️ [ProcessCQLResult] Execution Completed! Time Taken: {} Minutes | {} Seconds ⏱️", minutes,
                seconds);
        return this.dataFrame;
    }

    private JobContext setSparkConf(JobContext jobContext) {
        jobContext.sqlctx().setConf("spark.sql.caseSensitive", "true");
        jobContext.sqlctx().setConf("spark.cassandra.connection.localDC", SPARK_CASSANDRA_DATACENTER);
        jobContext.sqlctx().setConf("spark.cassandra.connection.host", SPARK_CASSANDRA_HOST);
        jobContext.sqlctx().setConf("spark.cassandra.connection.port", SPARK_CASSANDRA_PORT);
        jobContext.sqlctx().setConf("spark.cassandra.auth.username", SPARK_CASSANDRA_USERNAME);
        jobContext.sqlctx().setConf("spark.cassandra.auth.password", SPARK_CASSANDRA_PASSWORD);
        jobContext.sqlctx().setConf("spark.sql.catalog.ybcatalog",
                "com.datastax.spark.connector.datasource.CassandraCatalog");
        jobContext.sqlctx().setConf("spark.cassandra.output.ignoreNulls", "true");
        jobContext.sqlctx().setConf("spark.cassandra.input.consistency.level", "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.output.consistency.level", "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.query.retry.count", "10");
        jobContext.sqlctx().setConf("spark.cassandra.output.batch.size.rows", "500");
        jobContext.sqlctx().setConf("spark.cassandra.output.concurrent.writes", "3");
        jobContext.sqlctx().setConf("spark.cassandra.connection.remoteConnectionsPerExecutor", "5");
        jobContext.sqlctx().setConf("spark.jdbc.url", SPARK_PM_JDBC_URL);
        jobContext.sqlctx().setConf("spark.jdbc.user", SPARK_PM_JDBC_USERNAME);
        jobContext.sqlctx().setConf("spark.jdbc.password", SPARK_PM_JDBC_PASSWORD);
        return jobContext;
    }

    private static void processEachCQLRow(Row row, Map<String, String> extractedParameters, JobContext jobContext,
            Map<String, String> inputMap) {

        boolean isNodeLevel = Boolean.parseBoolean(extractedParameters.get("isNodeLevel"));
        logger.info("📊 Is Node Level : {}", isNodeLevel);

        if (isNodeLevel) {

            Map<String, Map<String, String>> resultMap = getMapForNodeLevel(row, extractedParameters);
            logger.info("📊 Result Map For Node Level: {}", resultMap);

            processEachResultMap(resultMap, extractedParameters, jobContext, inputMap);
        } else {

            Map<String, Map<String, String>> resultMap = getMapForNonNodeLevel(row, extractedParameters);
            logger.info("📊 Result Map For Non Node Level: {}", resultMap);

            processEachResultMap(resultMap, extractedParameters, jobContext, inputMap);
        }
    }

    private static void processEachResultMap(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {

        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");

        logger.info("📊 KPI Value Map: {}", kpiValueMap);

        String expression = extractedParameters.get("EXPRESSION");

        if (expression == null || expression.isEmpty()) {
            logger.info("📊 Expression is NULL or Empty. Skipping Expression Evaluation.");
            return;
        }

        logger.info("📊 Original Expression : {}", expression);
        StringBuilder optimizedExpression = new StringBuilder(expression);

        if (expression.contains("KPI#")) {
            String[] exp = expression.split("KPI#");
            int i = 0;
            for (String kpi : exp) {
                if (i++ != 0) {
                    int endIndex = kpi.indexOf(')');
                    String kpiId = (endIndex != -1) ? kpi.substring(0, endIndex) : kpi;

                    if (kpiValueMap.containsKey(kpiId)) {
                        String kpiValue = kpiValueMap.get(kpiId);
                        String replacement = (kpiValue.equalsIgnoreCase("-")) ? "NULL" : kpiValue;

                        int startIndex = optimizedExpression.indexOf("((" + "KPI#" + kpiId + "))");
                        if (startIndex != -1) {
                            optimizedExpression.replace(startIndex,
                                    startIndex + ("((" + "KPI#" + kpiId + "))").length(), replacement);
                        }
                    }
                }
            }
        }

        logger.info("📊 Optimized Expression : {}", optimizedExpression.toString());

        evaluateExpression(optimizedExpression.toString(), resultMap, extractedParameters, jobContext, inputMap);
    }

    private static void evaluateExpression(String expression, Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {
        String result = null;

        try {
            Expression evaluatedExpression = new Expression(expression);
            result = evaluatedExpression.eval();
        } catch (Exception e) {
            result = "0";
        }

        logger.info("📊 Evaluated Expression : {} And Result : {}", expression, result);

        if ("1".equalsIgnoreCase(result)) {

            logger.info("📊 Expression Evaluated to TRUE (1). Proceeding with Positive Condition Logic.");
            startAlertGenerationProcess(resultMap, extractedParameters, jobContext, inputMap);

        } else {

            logger.info("📊 Expression Evaluated to FALSE (0). Skipping Alert Generation Process.");
        }
    }

    private static void startAlertGenerationProcess(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {

        String outOfLast = extractedParameters.get("outOfLast");

        if (outOfLast != null && !outOfLast.isEmpty() && !outOfLast.equals("0")) {

            logger.info(
                    "📊 Out of Last is Provided. Proceeding with Alert Generation Process With Consistency Check.");

            Map<String, String> openAlertMap = getOpenAlertMapWithConsistencyCheck(resultMap, extractedParameters,
                    jobContext, inputMap);

            if (openAlertMap == null) {
                logger.info("📊 No Open Alert Map Found, Skipping Alert Generation Process!");
                return;
            }

            logger.info("📊 Open Alert Map With Consistency Check: {}", openAlertMap);

            proceedInsertOrUpdate(openAlertMap);

        } else {

            logger.info(
                    "📊 Out of Last is Not Provided. Proceeding with Alert Generation Process With No Consistency Check.");
            Map<String, String> openAlertMap = getOpenAlertMapWithNoConsistencyCheck(resultMap, extractedParameters);

            if (openAlertMap == null) {
                logger.info("📊 No Open Alert Map Found, Skipping Alert Generation Process!");
                return;
            }

            logger.info("📊 Open Alert Map With No Consistency Check: {}", openAlertMap);

            proceedInsertOrUpdate(openAlertMap);
        }
    }

    private static String convertTimeStampToExpectedFormat(String timestamp) {

        logger.info("📊 Timestamp: {}", timestamp);

        if (timestamp == null || timestamp.isEmpty()) {
            String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            logger.info("📊 Formatted Timestamp: {}", formattedTimestamp);
            return formattedTimestamp;
        }

        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
            OffsetDateTime dateTime = OffsetDateTime.parse(timestamp, inputFormatter);

            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

            String formattedTimestamp = dateTime.format(outputFormatter);
            logger.info("📊 Formatted Timestamp: {}", formattedTimestamp);
            return formattedTimestamp;

        } catch (Exception e) {
            String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            logger.info("📊 Formatted Timestamp: {}", formattedTimestamp);
            return formattedTimestamp;
        }
    }

    private static String safeUpper(String value) {
        return value != null ? value.toUpperCase() : null;
    }

    private static String generateProcessingTime() {
        String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
        logger.info("📊 Generated Processing Time: {}", formattedTimestamp);
        return formattedTimestamp;
    }

    private static Map<String, String> getLatLongUsingEntityId(String entityId) {

        logger.info("📊 Entity ID: {}", entityId);

        Map<String, String> latLongMap = new HashMap<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            String sqlQuery = "SELECT LATITUDE, LONGITUDE FROM NETWORK_ELEMENT WHERE NE_ID = ?";
            connection = getDatabaseConnection("PERFORMANCE");

            if (connection != null) {
                preparedStatement = connection.prepareStatement(sqlQuery);
                preparedStatement.setString(1, entityId);
                resultSet = preparedStatement.executeQuery();

                if (resultSet.next()) {
                    String latitude = resultSet.getString("LATITUDE");
                    String longitude = resultSet.getString("LONGITUDE");

                    latLongMap.put("LATITUDE", latitude != null ? latitude : "");
                    latLongMap.put("LONGITUDE", longitude != null ? longitude : "");
                }
            }
        } catch (SQLException e) {
            logger.error("Database Error @getLatLongUsingEntityId | Message: {}, Error: {}", e.getMessage(), e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (preparedStatement != null)
                    preparedStatement.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                logger.error("Error Closing Resources @getLatLongUsingEntityId | Message: {}, Error: {}",
                        e.getMessage(), e);
            }
        }

        logger.info("📊 Recieved LatLong Map: {}", latLongMap);
        return latLongMap;
    }

    private static Map<String, String> getOpenAlertMapWithNoConsistencyCheck(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters) {

        Map<String, String> openAlertMap = new LinkedHashMap<>();
        String openTime = extractedParameters.get("TIMESTAMP");

        logger.info("📊 Open Time : {}", openTime);
        openTime = convertTimeStampToExpectedFormat(openTime);
        logger.info("📊 Formatted Open Time : {}", openTime);

        String entityId = null;
        String entityName = null;
        String subentity = null;
        String geoL1Name = null;
        String geoL2Name = null;
        String geoL3Name = null;
        String geoL4Name = null;
        String entityType = null;
        String entityStatus = null;
        String latitude = null;
        String longitude = null;

        Map<String, String> nodeDetailsMap = resultMap.get("nodeDetailsMap");
        logger.info("📊 Node Details Map : {}", nodeDetailsMap);

        boolean isNodeLevel = Boolean.parseBoolean(extractedParameters.get("isNodeLevel"));
        logger.info("📊 Is Node Level : {}", isNodeLevel);

        if (isNodeLevel) {

            entityId = nodeDetailsMap.get("ENTITY_ID");
            entityName = nodeDetailsMap.get("ENTITY_NAME");
            subentity = nodeDetailsMap.get("SUBENTITY");
            geoL1Name = nodeDetailsMap.get("GEOGRAPHY_L1_NAME");
            geoL2Name = nodeDetailsMap.get("GEOGRAPHY_L2_NAME");
            geoL3Name = nodeDetailsMap.get("GEOGRAPHY_L3_NAME");
            geoL4Name = nodeDetailsMap.get("GEOGRAPHY_L4_NAME");
            entityType = nodeDetailsMap.get("ENTITY_TYPE");
            entityStatus = nodeDetailsMap.get("ENTITY_STATUS");

            Map<String, String> latLongMap = getLatLongUsingEntityId(entityId);
            latitude = latLongMap.get("LATITUDE");
            longitude = latLongMap.get("LONGITUDE");

        } else {

            entityId = nodeDetailsMap.get("ENTITY_ID");
            entityName = nodeDetailsMap.get("ENTITY_NAME");
            subentity = nodeDetailsMap.get("SUBENTITY");
            geoL1Name = nodeDetailsMap.get("GEOGRAPHY_L1_NAME");
            geoL2Name = nodeDetailsMap.get("GEOGRAPHY_L2_NAME");
            geoL3Name = nodeDetailsMap.get("GEOGRAPHY_L3_NAME");
            geoL4Name = nodeDetailsMap.get("GEOGRAPHY_L4_NAME");
            entityType = nodeDetailsMap.get("ENTITY_TYPE");
            entityStatus = nodeDetailsMap.get("ENTITY_STATUS");

            // String level = extractedParameters.get("level");

            // if (level != null) {
            // level = level.toUpperCase();

            // switch (level) {
            // case "L0":
            // entityName = safeUpper(nodeDetailsMap.get("ENTITY_NAME"));
            // break;

            // case "L1":
            // entityName = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L1_NAME"));
            // geoL1Name = entityName;
            // break;

            // case "L2":
            // entityName = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L2_NAME"));
            // geoL1Name = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L1_NAME"));
            // geoL2Name = entityName;
            // break;

            // case "L3":
            // entityName = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L3_NAME"));
            // geoL1Name = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L1_NAME"));
            // geoL2Name = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L2_NAME"));
            // geoL3Name = entityName;
            // break;

            // case "L4":
            // entityName = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L4_NAME"));
            // geoL1Name = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L1_NAME"));
            // geoL2Name = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L2_NAME"));
            // geoL3Name = safeUpper(nodeDetailsMap.get("GEOGRAPHY_L3_NAME"));
            // geoL4Name = entityName;
            // break;
            // }
            // }
        }
        openAlertMap.put("nodename", resultMap.get("nodeDetailsMap").get("nodename"));
        openAlertMap.put("OPEN_TIME", openTime);
        openAlertMap.put("CHANGE_TIME", openTime);
        openAlertMap.put("REPORTING_TIME", openTime);
        openAlertMap.put("FIRST_RECEPTION_TIME", openTime);
        openAlertMap.put("FIRST_PROCESSING_TIME", generateProcessingTime());
        openAlertMap.put("LAST_PROCESSING_TIME", generateProcessingTime());

        openAlertMap.put("ALARM_EXTERNAL_ID", extractedParameters.get("ALARM_EXTERNAL_ID"));
        openAlertMap.put("ALARM_CODE", extractedParameters.get("ALARM_CODE"));
        openAlertMap.put("ALARM_NAME", extractedParameters.get("ALARM_NAME"));

        openAlertMap.put("CLASSIFICATION", extractedParameters.get("CLASSIFICATION"));
        openAlertMap.put("EVENT_TYPE", extractedParameters.get("EVENT_TYPE"));
        openAlertMap.put("SEVERITY", extractedParameters.get("SEVERITY"));
        openAlertMap.put("ACTUAL_SEVERITY", extractedParameters.get("ACTUAL_SEVERITY"));

        openAlertMap.put("ALARM_STATUS", "OPEN");
        openAlertMap.put("ENTITY_NAME", entityName);
        openAlertMap.put("ENTITY_ID", entityId);
        openAlertMap.put("SUBENTITY", subentity);
        openAlertMap.put("ENTITY_TYPE", entityType);
        openAlertMap.put("ENTITY_STATUS", entityStatus);

        openAlertMap.put("LATITUDE", latitude);
        openAlertMap.put("LONGITUDE", longitude);
        openAlertMap.put("SENDER_NAME", extractedParameters.get("SENDER_NAME"));
        openAlertMap.put("SENDER_IP", "-");

        openAlertMap.put("GEOGRAPHY_L1_NAME", geoL1Name);
        openAlertMap.put("GEOGRAPHY_L2_NAME", geoL2Name);
        openAlertMap.put("GEOGRAPHY_L3_NAME", geoL3Name);
        openAlertMap.put("GEOGRAPHY_L4_NAME", geoL4Name);

        openAlertMap.put("DOMAIN", extractedParameters.get("DOMAIN"));
        openAlertMap.put("VENDOR", extractedParameters.get("VENDOR"));
        openAlertMap.put("TECHNOLOGY", extractedParameters.get("TECHNOLOGY"));
        openAlertMap.put("PROBABLE_CAUSE", extractedParameters.get("PROBABLE_CAUSE"));
        openAlertMap.put("DESCRIPTION", extractedParameters.get("DESCRIPTION"));

        openAlertMap.put("SERVICE_AFFECTING", extractedParameters.get("SERVICE_AFFECTING"));
        openAlertMap.put("MANUALLY_CLOSEABLE", extractedParameters.get("MANUALLY_CLOSEABLE"));
        openAlertMap.put("CORRELATION_FLAG", extractedParameters.get("CORRELATION_FLAG"));
        openAlertMap.put("DESCRIPTION", extractedParameters.get("DESCRIPTION"));
        openAlertMap.put("PROBABLE_CAUSE", extractedParameters.get("PROBABLE_CAUSE"));
        openAlertMap.put("ALARM_GROUP", extractedParameters.get("ALARM_GROUP"));

        String description = extractedParameters.get("DESCRIPTION");
        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");

        String additionalDetails = getAdditionalDetailsForNormalExp(description, kpiValueMap);
        logger.info("Additional Details: {}", additionalDetails);
        openAlertMap.put("ADDITIONAL_DETAIL", additionalDetails);

        return openAlertMap;
    }

    private static String getAdditionalDetailsForNormalExp(String description, Map<String, String> kpiValueMap) {

        if (description == null || description.isEmpty()) {
            return null;
        }

        long kpiHashCount = description.split("KPI#", -1).length - 1;
        if (kpiHashCount != 1) {
            return description;
        }

        Pattern pattern = Pattern.compile("KPI\\((\\d+)-([^)]+)\\)");
        Matcher matcher = pattern.matcher(description);

        if (matcher.find()) {
            String kpiId = matcher.group(1);
            String kpiName = matcher.group(2);
            String kpiValue = kpiValueMap.get(kpiId);

            if (kpiValue != null) {
                return "Threshold is Breached for KPI(" + kpiId + "-" + kpiName + "), Breached Value is " + kpiValue;
            } else {
                return "KPI(" + kpiId + "-" + kpiName + ") found in description, but value is missing in KPI map.";
            }
        }

        return "No KPI pattern found in description.";
    }

    private static void setQueryParamsForCountQuery(PreparedStatement stmt, Map<String, String> params)
            throws SQLException {
        stmt.setString(1, params.get("ALARM_EXTERNAL_ID"));
        stmt.setString(2, params.get("ALARM_CODE"));
        stmt.setString(3, params.get("ALARM_NAME"));
        stmt.setString(4, params.get("SUBENTITY"));
        stmt.setString(5, params.get("ENTITY_NAME"));
        stmt.setString(6, params.get("DOMAIN"));
        stmt.setString(7, params.get("VENDOR"));
        stmt.setString(8, params.get("TECHNOLOGY"));
    }

    private static void proceedInsertOrUpdate(Map<String, String> openAlertMap) {

        logger.info("Generated Alert Map: {}", openAlertMap);

        Connection connection = null;
        PreparedStatement countStmt = null;
        PreparedStatement detailStmt = null;
        ResultSet resultSet = null;

        try {
            connection = getDatabaseConnection("FMS_TEST");

            String countQuery = "SELECT COUNT(*) AS COUNT FROM ALARM WHERE ALARM_EXTERNAL_ID = ? AND ALARM_CODE = ? AND ALARM_NAME = ? "
                    +
                    "AND SUBENTITY = ? AND ENTITY_NAME = ? AND ALARM_STATUS = 'OPEN' AND DOMAIN = ? "
                    +
                    "AND VENDOR = ? AND TECHNOLOGY = ?";

            countStmt = connection.prepareStatement(countQuery);
            setQueryParamsForCountQuery(countStmt, openAlertMap);

            resultSet = countStmt.executeQuery();
            resultSet.next();
            int count = resultSet.getInt("COUNT");

            if (count > 0) {

                String detailQuery = "SELECT OCCURRENCE, ALARM_EXTERNAL_ID, ALARM_CODE, ALARM_NAME, SUBENTITY, ENTITY_NAME, ENTITY_ID, "
                        +
                        "ALARM_STATUS, DOMAIN, VENDOR, TECHNOLOGY, ADDITIONAL_DETAIL FROM ALARM WHERE ALARM_EXTERNAL_ID = ? "
                        +
                        "AND ALARM_CODE = ? AND ALARM_NAME = ? AND SUBENTITY = ? AND ENTITY_NAME = ? "
                        +
                        "AND ALARM_STATUS = 'OPEN' AND DOMAIN = ? AND VENDOR = ? AND TECHNOLOGY = ?";

                detailStmt = connection.prepareStatement(detailQuery);
                setQueryParamsForDetailQuery(detailStmt, openAlertMap);
                ResultSet detailResult = detailStmt.executeQuery();

                if (detailResult.next()) {
                    updateAlarm(openAlertMap, detailResult);
                }

                detailResult.close();

            } else {
                logger.info("Alert Does Not Exist. Inserting the Alert.");
                insertAlarm(openAlertMap);
            }

        } catch (SQLException e) {
            logger.error("Error Executing SQL, Message: {}, Error: {}", e.getMessage(), e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (countStmt != null)
                    countStmt.close();
                if (detailStmt != null)
                    detailStmt.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                logger.error("Error Closing Resources, Message: {}, Error: {}", e.getMessage(), e);
            }
        }
    }

    private static void insertAlarm(Map<String, String> openAlertMap) {

        openAlertMap.put("OCCURRENCE", "1");

        String sqlQuery = "INSERT INTO ALARM (OPEN_TIME, CHANGE_TIME, REPORTING_TIME, FIRST_RECEPTION_TIME, FIRST_PROCESSING_TIME, LAST_PROCESSING_TIME, ALARM_EXTERNAL_ID, ALARM_CODE, ALARM_NAME, CLASSIFICATION, EVENT_TYPE, SEVERITY, ACTUAL_SEVERITY, ALARM_STATUS, ENTITY_NAME, ENTITY_ID, SUBENTITY, ENTITY_TYPE, ENTITY_STATUS, LATITUDE, LONGITUDE, SENDER_NAME, SENDER_IP, GEOGRAPHY_L1_NAME, GEOGRAPHY_L2_NAME, GEOGRAPHY_L3_NAME, GEOGRAPHY_L4_NAME, DOMAIN, VENDOR, TECHNOLOGY, PROBABLE_CAUSE, DESCRIPTION, SERVICE_AFFECTED, MANUALLY_CLOSEABLE, CORRELATION_FLAG, OCCURRENCE, ADDITIONAL_DETAIL, ALARM_GROUP) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = getDatabaseConnection("FMS");
            preparedStatement = connection.prepareStatement(sqlQuery);

            preparedStatement.setString(1, openAlertMap.get("OPEN_TIME"));
            preparedStatement.setString(2, openAlertMap.get("CHANGE_TIME"));
            preparedStatement.setString(3, openAlertMap.get("REPORTING_TIME"));
            preparedStatement.setString(4, openAlertMap.get("FIRST_RECEPTION_TIME"));
            preparedStatement.setString(5, openAlertMap.get("FIRST_PROCESSING_TIME"));
            preparedStatement.setString(6, openAlertMap.get("LAST_PROCESSING_TIME"));
            preparedStatement.setString(7, openAlertMap.get("ALARM_EXTERNAL_ID"));
            preparedStatement.setString(8, openAlertMap.get("ALARM_CODE"));
            preparedStatement.setString(9, openAlertMap.get("ALARM_NAME"));
            preparedStatement.setString(10, openAlertMap.get("CLASSIFICATION"));
            preparedStatement.setString(11, openAlertMap.get("EVENT_TYPE"));
            preparedStatement.setString(12, openAlertMap.get("SEVERITY"));
            preparedStatement.setString(13, openAlertMap.get("ACTUAL_SEVERITY"));
            preparedStatement.setString(14, openAlertMap.get("ALARM_STATUS"));
            preparedStatement.setString(15, openAlertMap.get("ENTITY_NAME"));
            preparedStatement.setString(16, openAlertMap.get("ENTITY_ID"));
            preparedStatement.setString(17, openAlertMap.get("SUBENTITY"));
            preparedStatement.setString(18, openAlertMap.get("ENTITY_TYPE"));
            preparedStatement.setString(19, openAlertMap.get("ENTITY_STATUS"));
            preparedStatement.setString(20, openAlertMap.get("LATITUDE"));
            preparedStatement.setString(21, openAlertMap.get("LONGITUDE"));
            preparedStatement.setString(22, openAlertMap.get("SENDER_NAME"));
            preparedStatement.setString(23, openAlertMap.get("SENDER_IP"));
            preparedStatement.setString(24, openAlertMap.get("GEOGRAPHY_L1_NAME"));
            preparedStatement.setString(25, openAlertMap.get("GEOGRAPHY_L2_NAME"));
            preparedStatement.setString(26, openAlertMap.get("GEOGRAPHY_L3_NAME"));
            preparedStatement.setString(27, openAlertMap.get("GEOGRAPHY_L4_NAME"));
            preparedStatement.setString(28, openAlertMap.get("DOMAIN"));
            preparedStatement.setString(29, openAlertMap.get("VENDOR"));
            preparedStatement.setString(30, openAlertMap.get("TECHNOLOGY"));
            preparedStatement.setString(31, openAlertMap.get("PROBABLE_CAUSE"));
            preparedStatement.setString(32, openAlertMap.get("DESCRIPTION"));
            preparedStatement.setString(33, openAlertMap.get("SERVICE_AFFECTING"));
            preparedStatement.setString(34, openAlertMap.get("MANUALLY_CLOSEABLE"));
            preparedStatement.setString(35, openAlertMap.get("CORRELATION_FLAG"));
            preparedStatement.setString(36, openAlertMap.get("OCCURRENCE"));
            preparedStatement.setString(37, openAlertMap.get("ADDITIONAL_DETAIL"));
            preparedStatement.setString(38, openAlertMap.get("ALARM_GROUP"));

            int result = preparedStatement.executeUpdate();
            if (result > 0) {
                // logger.info(
                // "Alarm Inserted Successfully - Alarm Name: {}, Alarm Identifier: {},
                // Equipment ID : {}, Occurrence: {}, Additional Details: {}",
                // openAlertMap.get("ALARM_NAME"), openAlertMap.get("ALARM_EXTERNAL_ID"),
                // openAlertMap.get("ENTITY_NAME"), openAlertMap.get("OCCURRENCE"),
                // openAlertMap.get("ADDITIONAL_DETAIL"));

                logger.info("\n======================= 🚨 ALARM INSERTED 🚨 =======================\n" +
                        "🔔 Alarm Name        : {}\n" +
                        "🆔 Alarm Identifier  : {}\n" +
                        "🏷️  Equipment ID      : {}\n" +
                        "🔢 Occurrence        : {}\n" +
                        "📝 Additional Details: {}\n" +
                        "===================================================================",
                        openAlertMap.get("ALARM_NAME"),
                        openAlertMap.get("ALARM_EXTERNAL_ID"),
                        openAlertMap.get("ENTITY_NAME"),
                        openAlertMap.get("OCCURRENCE"),
                        openAlertMap.get("ADDITIONAL_DETAIL"));

            }
        } catch (SQLException e) {
            logger.error("Error Executing Alarm Insert SQL query, Message: {}, Error: {}", e.getMessage(), e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                logger.error("Error Closing Resources, Message: {}, Error: {}", e.getMessage(), e);
            }
        }
    }

    private static void updateAlarm(Map<String, String> openAlertMap, ResultSet resultSetToUpdate)
            throws SQLException {
        String sqlQuery = "UPDATE ALARM SET OCCURRENCE = ?, LAST_PROCESSING_TIME = ?, CHANGE_TIME = ?, ADDITIONAL_DETAIL = ? "
                +
                "WHERE ALARM_STATUS = 'OPEN' AND ALARM_EXTERNAL_ID = ? AND ALARM_CODE = ? AND ALARM_NAME = ? AND SUBENTITY = ? AND ENTITY_NAME = ? "
                +
                "AND ALARM_STATUS = ? AND DOMAIN = ? AND VENDOR = ? AND TECHNOLOGY = ?";

        Connection connection = null;
        connection = getDatabaseConnection("FMS");
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

            int currentOccurrence = resultSetToUpdate.getInt("OCCURRENCE");

            logger.info("Alert Already Exists With Occurrence : {}, Updating the Alert With Occurrence : {}",
                    currentOccurrence, currentOccurrence + 1);

            String openTime = openAlertMap.get("OPEN_TIME");
            openTime = convertTimeStampToExpectedFormat(openTime);

            preparedStatement.setInt(1, currentOccurrence + 1);
            preparedStatement.setString(2, generateProcessingTime());
            preparedStatement.setString(3, openTime);
            preparedStatement.setString(4, openAlertMap.get("ADDITIONAL_DETAIL"));
            preparedStatement.setString(5, resultSetToUpdate.getString("ALARM_EXTERNAL_ID"));
            preparedStatement.setString(6, resultSetToUpdate.getString("ALARM_CODE"));
            preparedStatement.setString(7, resultSetToUpdate.getString("ALARM_NAME"));
            preparedStatement.setString(8, resultSetToUpdate.getString("SUBENTITY"));
            preparedStatement.setString(9, resultSetToUpdate.getString("ENTITY_NAME"));
            preparedStatement.setString(10, resultSetToUpdate.getString("ALARM_STATUS"));
            preparedStatement.setString(11, resultSetToUpdate.getString("DOMAIN"));
            preparedStatement.setString(12, resultSetToUpdate.getString("VENDOR"));
            preparedStatement.setString(13, resultSetToUpdate.getString("TECHNOLOGY"));

            preparedStatement.executeUpdate();

            logger.info("\n======================= 🔄 ALARM UPDATED 🔄 =======================\n" +
                    "🔔 Alarm Name        : {}\n" +
                    "🆔 Alarm Identifier  : {}\n" +
                    "🏷️  Equipment ID      : {}\n" +
                    "📝 Additional Details: {}\n" +
                    "🔢 Occurrence        : {}\n" +
                    "===================================================================",
                    resultSetToUpdate.getString("ALARM_NAME"),
                    resultSetToUpdate.getString("ALARM_EXTERNAL_ID"),
                    resultSetToUpdate.getString("ENTITY_NAME"),
                    resultSetToUpdate.getString("ADDITIONAL_DETAIL"),
                    resultSetToUpdate.getInt("OCCURRENCE") + 1);

        }
    }

    private static void setQueryParamsForDetailQuery(PreparedStatement stmt, Map<String, String> params)
            throws SQLException {
        stmt.setString(1, params.get("ALARM_EXTERNAL_ID"));
        stmt.setString(2, params.get("ALARM_CODE"));
        stmt.setString(3, params.get("ALARM_NAME"));
        stmt.setString(4, params.get("SUBENTITY"));
        stmt.setString(5, params.get("ENTITY_NAME"));
        stmt.setString(6, params.get("DOMAIN"));
        stmt.setString(7, params.get("VENDOR"));
        stmt.setString(8, params.get("TECHNOLOGY"));
    }

    private static Map<String, String> getOpenAlertMapWithConsistencyCheck(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {

        boolean isConsistencyMet = isConsistencyMet(extractedParameters, resultMap, jobContext, inputMap);
        if (isConsistencyMet) {
            return getOpenAlertMapWithNoConsistencyCheck(resultMap, extractedParameters);
        } else {
            logger.info("Consistency Not Met, Skipping Alert Generation Process!");
        }
        return null;
    }

    private static boolean isConsistencyMet(Map<String, String> extractedParameters,
            Map<String, Map<String, String>> resultMap, JobContext jobContext, Map<String, String> inputMap) {

        String timestamp = extractedParameters.get("TIMESTAMP");

        return checkConsistency(timestamp, extractedParameters, resultMap, jobContext, inputMap);
    }

    private static boolean checkConsistency(String timestamp, Map<String, String> extractedParameters,
            Map<String, Map<String, String>> resultMap, JobContext jobContext, Map<String, String> inputMap) {

        logger.info("Current Timestamp: {}", timestamp);
        timestamp = reduceFrequencyFromTimestamp(timestamp, jobContext);
        logger.info("Reduced Timestamp: {}", timestamp);

        // String nodename = resultMap.get("nodeDetailsMap").get("nodename");
        // String nodename = resultMap.get("nodeDetailsMap").get("ENTITY_NAME");

        String nodename;

        String level = extractedParameters.get("level");
        if (level.contains("L0")) {
            nodename = resultMap.get("nodeDetailsMap").get("ENTITY_NAME");
        } else {
            nodename = resultMap.get("nodeDetailsMap").get("ENTITY_ID");
        }
        int requiredInstances = Integer.parseInt(extractedParameters.get("instances"));
        int maxAttempts = Integer.parseInt(extractedParameters.get("outOfLast"));

        logger.info("Required Instances: {}, Max Attempts: {}", requiredInstances, maxAttempts);

        int thresholdBreachCount = 1; // Already breached once

        // First attempt check
        if (thresholdBreachCount >= requiredInstances) {
            logger.info("Consistency Met For Attempt 1, Met Instances: {}", thresholdBreachCount);
            return true;
        }

        logger.info("Consistency Not Met For Attempt 1, Proceeding With Next Attempts.");

        for (int attempt = 2; attempt <= maxAttempts; attempt++) {
            logger.info("Attempt {} of {} . Current Timestamp: {}", attempt, maxAttempts, timestamp);

            String cqlQuery = buildCQLQueryForConsistency(timestamp, extractedParameters, nodename);
            logger.info("CQL Query For Attempt {} : {}", attempt, cqlQuery);

            List<Row> rows = getCQLData(cqlQuery, jobContext, inputMap, extractedParameters);

            if (rows != null && !rows.isEmpty()) {
                for (Row row : rows) {

                    if (processEachCQLRowForConsitency(row, extractedParameters)) {

                        thresholdBreachCount++;
                        logger.info("Threshold Breached For Attempt {} : {}", attempt, thresholdBreachCount);

                        if (thresholdBreachCount >= requiredInstances) {
                            logger.info("Consistency Met For Attempt {} : {}", attempt, thresholdBreachCount);
                            return true;
                        }
                    } else {
                        logger.info("Threshold Not Breached For Attempt {} : {}", attempt, thresholdBreachCount);
                    }
                }
            } else {
                logger.info("No Rows Retrieved from Cassandra For Attempt {}", attempt);
            }

            timestamp = reduceFrequencyFromTimestamp(timestamp, jobContext);
        }

        return false;
    }

    private static List<Row> getCQLData(String originalQuery, JobContext jobContext, Map<String, String> inputConfiMap,
            Map<String, String> extractedParametersMap) {

        Map<String, String> splitQueryMap = splitCQLQuery(originalQuery);
        String selectQuery = splitQueryMap.get("selectQuery");
        String filterQuery = splitQueryMap.get("filterQuery");

        logger.info("Filter Query For Consistency: {}", filterQuery);

        Dataset<Row> cqlDF = getCQLDataUsingSpark(filterQuery, jobContext, inputConfiMap, extractedParametersMap);
        cqlDF.createOrReplaceTempView("CQLResult");
        cqlDF.show(false);

        Dataset<Row> filterDF = jobContext.sqlctx().sql(selectQuery);
        filterDF.show(false);

        return filterDF.collectAsList();
    }

    private static Map<String, String> splitCQLQuery(String originalQuery) {
        Map<String, String> result = new HashMap<>();

        // Normalize spacing
        String cleanedQuery = originalQuery.trim().replaceAll("\\s+", " ");

        // Split on the first occurrence of "WHERE"
        int whereIndex = cleanedQuery.toUpperCase().indexOf("WHERE");
        if (whereIndex == -1) {
            result.put("selectQuery", cleanedQuery); // No WHERE clause
            result.put("filterQuery", "");
            return result;
        }

        String selectPart = cleanedQuery.substring(0, whereIndex).trim();
        String filterPart = cleanedQuery.substring(whereIndex + "WHERE".length()).trim();

        result.put("selectQuery", selectPart);
        result.put("filterQuery", filterPart);
        return result;
    }

    private static Dataset<Row> getCQLDataUsingSpark(String cqlFilter, JobContext jobContext,
            Map<String, String> reportWidgetDetails, Map<String, String> extractedParametersMap) {

        String cqlTableName = extractedParametersMap.get("cqlTableName");
        String cqlConsistencyLevel = "ONE";

        Dataset<Row> resultDataFrame = null;

        int maxRetries = 5;
        int currentRetry = 0;
        boolean success = false;

        while (!success && currentRetry < maxRetries) {
            try {
                if (currentRetry > 0) {
                    long backoffMs = (long) Math.pow(2, currentRetry) * 1000;
                    logger.info("Retry Attempt {} - Waiting {} Milliseconds Before Next Attempt", currentRetry,
                            backoffMs);
                    Thread.sleep(backoffMs);
                }

                resultDataFrame = jobContext.sqlctx().read()
                        .format("org.apache.spark.sql.cassandra")
                        .options(Map.of(
                                "table", cqlTableName,
                                "keyspace", "pm",
                                "pushdown", "true",
                                "consistency.level", cqlConsistencyLevel))
                        .load()
                        .filter(cqlFilter);

                resultDataFrame = resultDataFrame.cache();
                success = true;

            } catch (Exception e) {
                currentRetry++;
                logger.error("Attempt {} Failed: Error in Getting CQL Data Using Spark, Message: {}, Error: {}",
                        currentRetry, e.getMessage(), e);

                if (currentRetry < maxRetries) {
                    logger.info("Retrying Query... (Attempt {}/{})", currentRetry + 1, maxRetries);
                } else {
                    logger.error("All Retry Attempts Failed for Query: {}", cqlFilter);
                    throw new RuntimeException("Failed to Execute Cassandra Query After " + maxRetries + " Attempts",
                            e);
                }
            }
        }
        return resultDataFrame;
    }

    private static boolean processEachCQLRowForConsitency(Row row, Map<String, String> extractedParameters) {

        boolean isNodeLevel = Boolean.parseBoolean(extractedParameters.get("isNodeLevel"));

        if (isNodeLevel) {
            Map<String, Map<String, String>> resultMap = getMapForNodeLevel(row, extractedParameters);
            return processEachResultMapForConsitency(resultMap, extractedParameters);
        } else {
            Map<String, Map<String, String>> resultMap = getMapForNonNodeLevel(row, extractedParameters);
            return processEachResultMapForConsitency(resultMap, extractedParameters);
        }

    }

    private static boolean processEachResultMapForConsitency(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters) {

        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");
        String expression = extractedParameters.get("EXPRESSION");
        StringBuilder optimizedExpression = new StringBuilder(expression);

        if (expression.contains("KPI#")) {
            String[] exp = expression.split("KPI#");
            int i = 0;
            for (String kpi : exp) {
                if (i++ != 0) {
                    int endIndex = kpi.indexOf(')');
                    String kpiId = (endIndex != -1) ? kpi.substring(0, endIndex) : kpi;

                    if (kpiValueMap.containsKey(kpiId)) {
                        String kpiValue = kpiValueMap.get(kpiId);
                        String replacement = (kpiValue.equalsIgnoreCase("-")) ? "NULL" : kpiValue;

                        int startIndex = optimizedExpression.indexOf("((" + "KPI#" + kpiId + "))");
                        if (startIndex != -1) {
                            optimizedExpression.replace(startIndex,
                                    startIndex + ("((" + "KPI#" + kpiId + "))").length(), replacement);
                        }
                    }
                }
            }
        }

        logger.info("Optimized Expression: {}", optimizedExpression.toString());
        return isThresholdBreach(optimizedExpression.toString());
    }

    private static boolean isThresholdBreach(String expression) {

        try {
            Expression evaluatedExpression = new Expression(expression);
            String result = evaluatedExpression.eval();
            if (result.equalsIgnoreCase("1")) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private static String buildCQLQueryForConsistency(String timestamp, Map<String, String> extractedParameters,
            String nodename) {

        String kpiCodeList = extractedParameters.get("kpiCodeList");
        String datalevel = extractedParameters.get("DATALEVEL");
        String domain = extractedParameters.get("DOMAIN");
        String vendor = extractedParameters.get("VENDOR");
        String technology = extractedParameters.get("TECHNOLOGY");

        StringBuilder cqlQuery = new StringBuilder();
        cqlQuery.append("SELECT ");

        if (kpiCodeList == null || kpiCodeList.isEmpty()) {
            return null;
        }

        String[] kpiCodes = kpiCodeList.split(",");
        if (kpiCodes.length == 0) {
            return null;
        }

        for (int i = 0; i < kpiCodes.length; i++) {
            String kpiCode = kpiCodes[i].trim().replaceAll("[\\[\\]]", "");
            cqlQuery.append("kpijson['").append(kpiCode).append("']");
            if (i < kpiCodes.length - 1) {
                cqlQuery.append(", ");
            }
        }

        if (datalevel.contains("L0")) {
            cqlQuery.append(", metajson['ENTITY_NAME'], metajson['ENTITY_TYPE']");
        } else if (datalevel.contains("L1")) {
            cqlQuery.append(", metajson['L1'], metajson['ENTITY_TYPE']");
        } else if (datalevel.contains("L2")) {
            cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['ENTITY_TYPE']");
        } else if (datalevel.contains("L3")) {
            cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['L3'], metajson['ENTITY_TYPE']");
        } else if (datalevel.contains("L4")) {
            cqlQuery.append(
                    ", metajson['L1'], metajson['L2'], metajson['L3'], metajson['L4'], metajson['ENTITY_TYPE']");
        } else {
            cqlQuery.append(
                    ", metajson['ENTITY_ID'], metajson['ENTITY_NAME'], metajson['L1'], metajson['L2'], metajson['L3'], metajson['L4'], metajson['ENTITY_TYPE'], metajson['NS']");
        }

        cqlQuery.append(", nodename");
        cqlQuery.append(" FROM ");
        cqlQuery.append("CQLResult");
        cqlQuery.append(" WHERE ");
        cqlQuery.append("domain = '").append(domain).append("' AND ");
        cqlQuery.append("vendor = '").append(vendor).append("' AND ");
        cqlQuery.append("technology = '").append(technology).append("' AND ");

        if (datalevel != null && !datalevel.isEmpty()) {
            cqlQuery.append("datalevel = '").append(datalevel).append("' AND ");
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
        OffsetDateTime odt = OffsetDateTime.parse(timestamp, formatter);
        String date = odt.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        cqlQuery.append(" date = '").append(date).append("' AND ");

        cqlQuery.append(" nodename = '").append(nodename).append("' AND ");

        cqlQuery.append(" timestamp = '").append(timestamp).append("'");

        logger.info("📊 CQL Query For Consistency: {}", cqlQuery.toString());

        return cqlQuery.toString();
    }

    private static Map<String, Map<String, String>> getMapForNonNodeLevel(Row row,
            Map<String, String> extractedParameters) {

        String kpiCodeList = extractedParameters.get("kpiCodeList");

        List<String> kpiCodes = Arrays.stream(kpiCodeList.replace("[", "").replace("]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        logger.info("KPI Codes: {}", kpiCodes);

        Map<String, String> kpiValueMap = new HashMap<>();

        for (String kpiCode : kpiCodes) {
            String kpiColumn = "kpijson[" + kpiCode + "]";
            Object kpiValueObj = row.getAs(kpiColumn);
            String kpiValue = null;

            if (kpiValueObj != null) {
                try {
                    double kpiDouble = Double.parseDouble(kpiValueObj.toString());
                    kpiValue = String.format("%.4f", kpiDouble);
                    kpiValueMap.put(kpiCode, kpiValue);
                } catch (NumberFormatException e) {
                    logger.error("Error Parsing KPI Value, Message: {}, Error: {}", e.getMessage(), e);
                }
            }
        }
        logger.info("KPI Value Map : {}", kpiValueMap);

        Map<String, Map<String, String>> resultMap = new HashMap<>();
        resultMap.put("kpiValueMap", kpiValueMap);

        String level = extractedParameters.get("level");
        logger.info("Map Level: {}", level);
        ;
        Map<String, String> nodeDetailsMap = new HashMap<>();

        // switch (level) {
        // case "L0": {
        // String nename = row.getAs("metajson[ENTITY_NAME]") != null ? (String)
        // row.getAs("metajson[ENTITY_NAME]") : "";

        // nodeDetailsMap.put("ENTITY_NAME", nename);
        // break;
        // }

        // case "L1": {
        // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
        // row.getAs("metajson[L1]") : "";
        // nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        // break;
        // }

        // case "L2": {
        // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
        // row.getAs("metajson[L1]") : "";
        // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
        // row.getAs("metajson[L2]") : "";
        // nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        // nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
        // break;
        // }
        // case "L3": {
        // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
        // row.getAs("metajson[L1]") : "";
        // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
        // row.getAs("metajson[L2]") : "";
        // String geoL3 = row.getAs("metajson[L3]") != null ? (String)
        // row.getAs("metajson[L3]") : "";
        // nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        // nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
        // nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
        // break;
        // }

        // case "L4": {
        // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
        // row.getAs("metajson[L1]") : "";
        // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
        // row.getAs("metajson[L2]") : "";
        // String geoL3 = row.getAs("metajson[L3]") != null ? (String)
        // row.getAs("metajson[L3]") : "";
        // String geoL4 = row.getAs("metajson[L4]") != null ? (String)
        // row.getAs("metajson[L4]") : "";
        // nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        // nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
        // nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
        // nodeDetailsMap.put("GEOGRAPHY_L4_NAME", geoL4);
        // break;
        // }

        // default: {
        // break;
        // }
        // }

        String neid = row.getAs("ENTITY_ID") != null ? (String) row.getAs("ENTITY_ID") : "";
        String nename = row.getAs("ENTITY_NAME") != null ? (String) row.getAs("ENTITY_NAME") : "";
        String subentity = row.getAs("SUBENTITY") != null ? (String) row.getAs("SUBENTITY") : "";
        String geoL1 = row.getAs("L1") != null ? (String) row.getAs("L1") : "";
        String geoL2 = row.getAs("L2") != null ? (String) row.getAs("L2") : "";
        String geoL3 = row.getAs("L3") != null ? (String) row.getAs("L3") : "";
        String geoL4 = row.getAs("L4") != null ? (String) row.getAs("L4") : "";
        String entityType = row.getAs("ENTITY_TYPE") != null ? (String) row.getAs("ENTITY_TYPE") : "";
        String entityStatus = "-";
        String nodename = row.getAs("nodename") != null ? (String) row.getAs("nodename") : "";

        nodeDetailsMap.put("ENTITY_ID", neid);
        nodeDetailsMap.put("ENTITY_NAME", nename);
        nodeDetailsMap.put("SUBENTITY", subentity);
        nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
        nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
        nodeDetailsMap.put("GEOGRAPHY_L4_NAME", geoL4);
        nodeDetailsMap.put("ENTITY_TYPE", entityType);
        nodeDetailsMap.put("ENTITY_STATUS", entityStatus);
        nodeDetailsMap.put("nodename", nodename);

        resultMap.put("nodeDetailsMap", nodeDetailsMap);
        return resultMap;
    }

    private static Map<String, Map<String, String>> getMapForNodeLevel(Row row,
            Map<String, String> extractedParameters) {

        String kpiCodeList = extractedParameters.get("kpiCodeList");

        List<String> kpiCodes = Arrays.stream(kpiCodeList.replace("[", "").replace("]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        Map<String, String> kpiValueMap = new HashMap<>();

        for (String kpiCode : kpiCodes) {

            String kpiColumn = "kpijson[" + kpiCode + "]";
            Object kpiValueObj = row.getAs(kpiColumn);
            String kpiValue = null;

            if (kpiValueObj != null) {
                try {
                    double kpiDouble = Double.parseDouble(kpiValueObj.toString());
                    kpiValue = String.format("%.4f", kpiDouble);
                    kpiValueMap.put(kpiCode, kpiValue);
                } catch (NumberFormatException e) {
                    logger.error("Error Parsing KPI Value, Message: {}, Error: {}", e.getMessage(), e);
                }
            }

        }

        // String neid = row.getAs("metajson[ENTITY_ID]") != null ? (String)
        // row.getAs("metajson[ENTITY_ID]") : "";
        // String neid = row.getAs("metajson[ENTITY_ID]") != null ? (String)
        // row.getAs("metajson[ENTITY_ID]") : "";
        String neid = row.getAs("ENTITY_ID") != null ? (String) row.getAs("ENTITY_ID") : "";

        // String nename = row.getAs("metajson[ENTITY_NAME]") != null ? (String)
        // row.getAs("metajson[ENTITY_NAME]") : "";
        // String nename = row.getAs("metajson[ENTITY_NAME]") != null ? (String)
        // row.getAs("metajson[ENTITY_NAME]") : "";
        String nename = row.getAs("ENTITY_NAME") != null ? (String) row.getAs("ENTITY_NAME") : "";

        String subentity = row.getAs("SUBENTITY") != null ? (String) row.getAs("SUBENTITY") : "";

        // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
        // row.getAs("metajson[L1]") : "";
        // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
        // row.getAs("metajson[L2]") : "";
        // String geoL3 = row.getAs("metajson[L3]") != null ? (String)
        // row.getAs("metajson[L3]") : "";
        // String geoL4 = row.getAs("metajson[L4]") != null ? (String)
        // row.getAs("metajson[L4]") : "";

        String geoL1 = row.getAs("L1") != null ? (String) row.getAs("L1") : "";
        String geoL2 = row.getAs("L2") != null ? (String) row.getAs("L2") : "";
        String geoL3 = row.getAs("L3") != null ? (String) row.getAs("L3") : "";
        String geoL4 = row.getAs("L4") != null ? (String) row.getAs("L4") : "";

        // String entityType = row.getAs("metajson[ENTITY_TYPE]") != null ? (String)
        // row.getAs("metajson[ENTITY_TYPE]") : "";
        // String entityType = row.getAs("metajson[ENTITY_TYPE]") != null ? (String)
        // row.getAs("metajson[ENTITY_TYPE]")
        // : "";
        String entityType = row.getAs("ENTITY_TYPE") != null ? (String) row.getAs("ENTITY_TYPE") : "";

        // String entityStatus = row.getAs("metajson[NS]") != null ? (String)
        // row.getAs("metajson[NS]") : "";
        String entityStatus = "ONAIR";
        String nodename = row.getAs("nodename") != null ? (String) row.getAs("nodename") : "";

        Map<String, String> nodeDetailsMap = new HashMap<>();
        nodeDetailsMap.put("ENTITY_ID", neid);
        nodeDetailsMap.put("ENTITY_NAME", nename);
        nodeDetailsMap.put("SUBENTITY", subentity);

        nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
        nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
        nodeDetailsMap.put("GEOGRAPHY_L4_NAME", geoL4);
        nodeDetailsMap.put("ENTITY_TYPE", entityType);
        nodeDetailsMap.put("ENTITY_STATUS", entityStatus);
        nodeDetailsMap.put("nodename", nodename);

        Map<String, Map<String, String>> resultMap = new HashMap<>();
        resultMap.put("nodeDetailsMap", nodeDetailsMap);
        resultMap.put("kpiValueMap", kpiValueMap);
        return resultMap;
    }

    private static String buildCQLQuery(Map<String, String> finalMap,
            String aggregationLevel, JobContext jobContext) {

        StringBuilder cqlQuery = new StringBuilder();

        String domain = finalMap.get("DOMAIN");
        String vendor = finalMap.get("VENDOR");
        String technology = finalMap.get("TECHNOLOGY");
        String kpiCodeList = finalMap.get("kpiCodeList");

        cqlQuery.append("SELECT ");

        if (kpiCodeList == null || kpiCodeList.isEmpty()) {
            return null;
        }

        String[] kpiCodes = kpiCodeList.split(",");
        if (kpiCodes.length == 0) {
            return null;
        }

        logger.info("📊 KPI Codes: {}", Arrays.toString(kpiCodes));

        for (int i = 0; i < kpiCodes.length; i++) {
            String kpiCode = kpiCodes[i].trim().replaceAll("[\\[\\]]", "");
            cqlQuery.append("kpijson['").append(kpiCode).append("']");
            if (i < kpiCodes.length - 1) {
                cqlQuery.append(", ");
            }
        }

        StringBuilder mysqlQuery = new StringBuilder()
                .append("SELECT DISTINCT CONCAT(TECHNOLOGY, IF(ROWKEY_TECHNOLOGY IS NOT NULL, '_', ''), ")
                .append("COALESCE(NETWORK_TYPE, '')) AS rowKeyAppender ")
                .append("FROM PM_NODE_VENDOR ")
                .append("WHERE domain = '").append(domain).append("' ")
                .append("AND vendor = '").append(vendor).append("' ")
                .append("AND technology = '").append(technology).append("'");

        logger.info("📊 MySQL Query For Data Level Appender: {}", mysqlQuery.toString());

        Dataset<Row> df = executeQuery(mysqlQuery.toString(), jobContext);

        List<String> dataLevelAppenders = df.as(Encoders.STRING()).collectAsList();
        String dataLevelAppender = dataLevelAppenders.isEmpty() ? "" : dataLevelAppenders.get(0);
        logger.info("📊 Data Level Appender: {}", dataLevelAppender);

        String datalevel = "";

        switch (aggregationLevel) {
            case "L0": {
                datalevel = "L0" + "_" + dataLevelAppender;
                break;
            }
            case "L1": {
                datalevel = "L1" + "_" + dataLevelAppender;
                break;
            }
            case "L2": {
                datalevel = "L2" + "_" + dataLevelAppender;
                break;
            }
            case "L3": {
                datalevel = "L3" + "_" + dataLevelAppender;
                break;
            }
            case "L4": {
                datalevel = "L4" + "_" + dataLevelAppender;
                break;
            }
            default: {
                datalevel = aggregationLevel + "_" + dataLevelAppender;
                break;
            }
        }

        logger.info("📊 Generated Data Level: {}", datalevel);

        if (datalevel.contains("L0")) {
            // cqlQuery.append(", metajson['ENTITY_NAME'], metajson['ENTITY_TYPE']");
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L0' AS SUBENTITY, '-' AS L1, '-' AS L2, '-' AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");
        } else if (datalevel.contains("L1")) {
            // cqlQuery.append(", metajson['L1'], metajson['ENTITY_TYPE']");
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L1' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN UPPER(nodename) ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, '-' AS L2, '-' AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else if (datalevel.contains("L2")) {
            // cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['ENTITY_TYPE']");
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L2' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END AS L2, '-' AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else if (datalevel.contains("L3")) {
            // cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['L3'],
            // metajson['ENTITY_TYPE']");
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L3' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END AS L2, CASE WHEN TRIM(metajson['L3']) IS NULL OR TRIM(metajson['L3']) = '' OR TRIM(metajson['L3']) = '-' OR LOWER(TRIM(metajson['L3'])) = 'null' THEN CASE WHEN TRIM(metajson['DL3']) IS NULL OR TRIM(metajson['DL3']) = '' OR TRIM(metajson['DL3']) = '-' OR LOWER(TRIM(metajson['DL3'])) = 'null' THEN '-' ELSE metajson['DL3'] END ELSE metajson['L3'] END AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else if (datalevel.contains("L4")) {
            // cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['L3'],
            // metajson['L4'], metajson['ENTITY_TYPE']");
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L4' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END AS L2, CASE WHEN TRIM(metajson['L3']) IS NULL OR TRIM(metajson['L3']) = '' OR TRIM(metajson['L3']) = '-' OR LOWER(TRIM(metajson['L3'])) = 'null' THEN CASE WHEN TRIM(metajson['DL3']) IS NULL OR TRIM(metajson['DL3']) = '' OR TRIM(metajson['DL3']) = '-' OR LOWER(TRIM(metajson['DL3'])) = 'null' THEN '-' ELSE metajson['DL3'] END ELSE metajson['L3'] END AS L3, CASE WHEN TRIM(metajson['L4']) IS NULL OR TRIM(metajson['L4']) = '' OR TRIM(metajson['L4']) = '-' OR LOWER(TRIM(metajson['L4'])) = 'null' THEN CASE WHEN TRIM(metajson['DL4']) IS NULL OR TRIM(metajson['DL4']) = '' OR TRIM(metajson['DL4']) = '-' OR LOWER(TRIM(metajson['DL4'])) = 'null' THEN '-' ELSE metajson['DL4'] END ELSE metajson['L4'] END AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else {
            // cqlQuery.append(
            // ", metajson['ENTITY_ID'], metajson['ENTITY_NAME'], metajson['L1'],
            // metajson['L2'], metajson['L3'], metajson['L4'], metajson['ENTITY_TYPE'],
            // metajson['NS']");
            cqlQuery.append(
                    ", CASE WHEN TRIM(metajson['NEID']) IS NULL OR TRIM(metajson['NEID']) = '' OR LOWER(TRIM(metajson['NEID'])) = 'null' THEN CASE WHEN TRIM(metajson['ENTITY_ID']) IS NULL OR TRIM(metajson['ENTITY_ID']) = '' OR LOWER(TRIM(metajson['ENTITY_ID'])) = 'null' THEN nodename ELSE metajson['ENTITY_ID'] END ELSE metajson['NEID'] END AS ENTITY_ID, CASE WHEN TRIM(metajson['NAM']) IS NULL OR TRIM(metajson['NAM']) = '' OR LOWER(TRIM(metajson['NAM'])) = 'null' OR TRIM(metajson['NAM']) = TRIM(metajson['NEID']) THEN CASE WHEN TRIM(metajson['ENTITY_NAME']) IS NULL OR TRIM(metajson['ENTITY_NAME']) = '' OR LOWER(TRIM(metajson['ENTITY_NAME'])) = 'null' THEN nodename ELSE metajson['ENTITY_NAME'] END ELSE metajson['NAM'] END AS ENTITY_NAME, UPPER(networktype) AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN (TRIM(metajson['L1']) IS NOT NULL AND TRIM(metajson['L1']) != '' AND LOWER(TRIM(metajson['L1'])) != 'null' AND TRIM(metajson['L1']) != '-') OR (TRIM(metajson['DL1']) IS NOT NULL AND TRIM(metajson['DL1']) != '' AND LOWER(TRIM(metajson['DL1'])) != 'null' AND TRIM(metajson['DL1']) != '-') THEN CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END ELSE '-' END AS L2, CASE WHEN (TRIM(metajson['L2']) IS NOT NULL AND TRIM(metajson['L2']) != '' AND LOWER(TRIM(metajson['L2'])) != 'null' AND TRIM(metajson['L2']) != '-') OR (TRIM(metajson['DL2']) IS NOT NULL AND TRIM(metajson['DL2']) != '' AND LOWER(TRIM(metajson['DL2'])) != 'null' AND TRIM(metajson['DL2']) != '-') THEN CASE WHEN TRIM(metajson['L3']) IS NULL OR TRIM(metajson['L3']) = '' OR TRIM(metajson['L3']) = '-' OR LOWER(TRIM(metajson['L3'])) = 'null' THEN CASE WHEN TRIM(metajson['DL3']) IS NULL OR TRIM(metajson['DL3']) = '' OR TRIM(metajson['DL3']) = '-' OR LOWER(TRIM(metajson['DL3'])) = 'null' THEN '-' ELSE metajson['DL3'] END ELSE metajson['L3'] END ELSE '-' END AS L3, CASE WHEN (TRIM(metajson['L3']) IS NOT NULL AND TRIM(metajson['L3']) != '' AND LOWER(TRIM(metajson['L3'])) != 'null' AND TRIM(metajson['L3']) != '-') OR (TRIM(metajson['DL3']) IS NOT NULL AND TRIM(metajson['DL3']) != '' AND LOWER(TRIM(metajson['DL3'])) != 'null' AND TRIM(metajson['DL3']) != '-') THEN CASE WHEN TRIM(metajson['L4']) IS NULL OR TRIM(metajson['L4']) = '' OR TRIM(metajson['L4']) = '-' OR LOWER(TRIM(metajson['L4'])) = 'null' THEN CASE WHEN TRIM(metajson['DL4']) IS NULL OR TRIM(metajson['DL4']) = '' OR TRIM(metajson['DL4']) = '-' OR LOWER(TRIM(metajson['DL4'])) = 'null' THEN '-' ELSE metajson['DL4'] END ELSE metajson['L4'] END ELSE '-' END AS L4, UPPER(networktype) AS ENTITY_TYPE");

        }

        cqlQuery.append(", nodename");
        cqlQuery.append(" FROM ");
        cqlQuery.append("CQLResult");
        return cqlQuery.toString();
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
            logger.error("⚠️ Exception in Executing Query, Message: " + e.getMessage() + " | Error: " + e);
            return resultDataset;
        }

    }

    public static String reduceFrequencyFromTimestamp(String timestamp, JobContext jobContext) {

        String jobFrequency = jobContext.getParameter("JOB_FREQUENCY");

        long reduceMinutes = 0L;

        switch (jobFrequency.toUpperCase()) {
            case "15 MIN":
                reduceMinutes = 15;
                break;
            case "PERHOUR":
                reduceMinutes = 60;
                break;
            case "PERDAY":
                reduceMinutes = 1440;
                break;
            case "PERWEEK":
                reduceMinutes = 10080;
                break;
            case "PERMONTH":
                reduceMinutes = 43200; // Approx. 30 days
                break;
            default:
                throw new IllegalArgumentException("Unsupported Job Frequency: " + jobFrequency);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
        try {

            OffsetDateTime DATE_TIME = OffsetDateTime.parse(timestamp, formatter);
            OffsetDateTime newTime = DATE_TIME.minusMinutes(reduceMinutes);
            return formatter.format(newTime);
        } catch (Exception e) {

            // OffsetDateTime now = OffsetDateTime.now();
            // int minutes = now.getMinute();
            // int roundedMinutes = (minutes / 15) * 15;

            // OffsetDateTime recentInterval = now.withMinute(roundedMinutes)
            // .withSecond(0)
            // .withNano(0);

            // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd
            // HH:mm:ss.SSSSSSxxxx");
            // return formatter.format(recentInterval);

            // Fallback: use current time, truncate to nearest minute, then subtract
            OffsetDateTime now = OffsetDateTime.now()
                    .withSecond(0)
                    .withNano(0);

            OffsetDateTime fallbackTime = now.minusMinutes(reduceMinutes);
            return formatter.format(fallbackTime);
        }
    }

    private static Connection getDatabaseConnection(String dbName) {

        Connection connection = null;

        try {

            if (dbName.contains("FMS")) {

                Class.forName(SPARK_FM_JDBC_DRIVER);
                connection = DriverManager.getConnection(SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME,
                        SPARK_FM_JDBC_PASSWORD);

            } else if (dbName.contains("PERFORMANCE")) {

                Class.forName(SPARK_PM_JDBC_DRIVER);
                connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                        SPARK_PM_JDBC_PASSWORD);

            }
            return connection;

        } catch (ClassNotFoundException e) {

            logger.error("⚠️ MySQL JDBC Driver Not Found @getDatabaseConnection | Message: {}, Error: {}",
                    e.getMessage(),
                    e);

        } catch (SQLException e) {

            logger.error("⚠️ Database Connection Error @getDatabaseConnection | Message: {}, Error: {}", e.getMessage(),
                    e);

        } catch (Exception e) {

            logger.error("⚠️ Unexpected Exception @getDatabaseConnection | Message: {}, Error: {}", e.getMessage(), e);
        }
        return connection;
    }

}
