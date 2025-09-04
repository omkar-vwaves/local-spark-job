package com.enttribe.pm.job.alert;

import com.enttribe.sparkrunner.processors.Processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enttribe.sparkrunner.util.Expression;

import org.json.JSONObject;

import com.enttribe.sparkrunner.context.JobContext;
import scala.collection.JavaConverters;

public class CustomPMAlertClose extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CustomPMAlertClose.class);
    private static Map<String, String> jobContextMap = new HashMap<>();

    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";

    private static String SPARK_FM_JDBC_URL = "SPARK_FM_JDBC_URL";
    private static String SPARK_FM_JDBC_DRIVER = "SPARK_FM_JDBC_DRIVER";
    private static String SPARK_FM_JDBC_USERNAME = "SPARK_FM_JDBC_USERNAME";
    private static String SPARK_FM_JDBC_PASSWORD = "SPARK_FM_JDBC_PASSWORD";

    private static String SPARK_CASSANDRA_KEYSPACE_PM = "SPARK_CASSANDRA_KEYSPACE_PM";
    private static String SPARK_CASSANDRA_HOST = "SPARK_CASSANDRA_HOST";
    private static String SPARK_CASSANDRA_PORT = "SPARK_CASSANDRA_PORT";
    private static String SPARK_CASSANDRA_DATACENTER = "SPARK_CASSANDRA_DATACENTER";
    private static String SPARK_CASSANDRA_USERNAME = "SPARK_CASSANDRA_USERNAME";
    private static String SPARK_CASSANDRA_PASSWORD = "SPARK_CASSANDRA_PASSWORD";

    public CustomPMAlertClose() {
        super();
        logger.info("CustomPMAlertClose No Argument Constructor Called!");
    }

    public CustomPMAlertClose(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("CustomPMAlertClose Constructor Called with Input DataFrame With ID: {} and Processor Name: {}",
                id,
                processorName);
    }

    public CustomPMAlertClose(Integer id, String processorName) {
        super(id, processorName);
        logger.info("CustomPMAlertClose Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

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

        return startCustomPMAlertCloseProcess(jobContext);
    }

    private static Dataset<Row> startCustomPMAlertCloseProcess(JobContext jobContext) {

        String domain = jobContextMap.get("DOMAIN");
        String vendor = jobContextMap.get("VENDOR");
        String technology = jobContextMap.get("TECHNOLOGY");
        String timestamp = jobContextMap.get("TIMESTAMP");
        String frequency = jobContextMap.get("JOB_FREQUENCY");

        logger.info(
                "📥 Job Context Parameters: DOMAIN={} | VENDOR={} | TECHNOLOGY={} | TIMESTAMP={} | JOB_FREQUENCY={}",
                domain, vendor, technology, timestamp, frequency);

        /*
         * READ ACTIVE CLOSE ALERT RULES
         */

        String activeRuleQuery = getActiveRuleQuery(domain, vendor, technology, frequency);
        Dataset<Row> activeRuleDF = executeQuery(activeRuleQuery, jobContext, "PERFORMANCE");
        if (activeRuleDF.isEmpty()) {
            logger.error("⚠️ No Active Rules Found. Returning Empty Dataset.");
            return activeRuleDF;
        }

        activeRuleDF.createOrReplaceTempView("PERFORMANCE_ALERT");

        /*
         * READ ACTIVE ALARM LIBRARIES
         */

        String activeLibraryQuery = getActiveLibraryQuery(domain, vendor, technology);
        Dataset<Row> activeLibraryDF = executeQuery(activeLibraryQuery, jobContext, "FMS");
        if (activeLibraryDF.isEmpty()) {
            logger.error("⚠️ No Active Libraries Found. Returning Empty Dataset.");
            return activeLibraryDF;
        }

        activeLibraryDF.createOrReplaceTempView("ALARM_LIBRARY");

        /*
         * JOIN ACTIVE ALARM LIBRARIES WITH ACTIVE CLOSE ALERT RULES
         */

        String joinQuery = buildSQLQuery(null, null, null, null);
        logger.info("Generated Join Query: {}", joinQuery);
        Dataset<Row> pmAlertConfDF = jobContext.sqlctx().sql(joinQuery);
        if (pmAlertConfDF.isEmpty()) {
            logger.error("⚠️ No Library Found For Active Rules. Returning Empty Dataset.");
            return pmAlertConfDF;
        }
        pmAlertConfDF.createOrReplaceTempView("PM_ALERT");
        pmAlertConfDF.show();
        logger.info("PM Alert Configuration Displayed Successfully! ✅");

        /*
         * READ OPEN ALARMS TO BE CLOSED
         */

        String openAlarmQuery = buildOpenAlarmQuery(domain, vendor, technology);
        logger.info("Generated Open Alarm Query: {}", openAlarmQuery);

        Dataset<Row> opentAlarmDF = executeQuery(openAlarmQuery, jobContext, "FMS");
        if (opentAlarmDF.isEmpty()) {
            logger.error("⚠️ No Open Alarms Found. Returning Empty Dataset.");
            return opentAlarmDF;
        }
        opentAlarmDF.show();
        opentAlarmDF.createOrReplaceTempView("OPEN_ALARM");
        logger.info("PM Open Alert Displayed Successfully! ✅");

        /*
         * JOIN OPEN ALARMS WITH ACTIVE CLOSE ALERT RULES
         */

        String sparkSQlQuery = "SELECT aa.ALARM_ID_PK, aa.ALARM_EXTERNAL_ID, aa.ALARM_CODE, aa.ALARM_NAME, aa.ENTITY_ID, aa.ENTITY_NAME, aa.SUBENTITY, aa.DOMAIN, aa.VENDOR, aa.TECHNOLOGY, aa.OCCURRENCE, pa.EXPRESSION, pa.CONFIGURATION FROM PM_ALERT pa INNER JOIN OPEN_ALARM aa ON aa.ALARM_EXTERNAL_ID = pa.ALARM_IDENTIFIER";

        Dataset<Row> joinDF = jobContext.sqlctx().sql(sparkSQlQuery);
        if (joinDF.isEmpty()) {
            logger.error("⚠️ No Matching Alerts Found After JOIN. Returning Empty Dataset.");
            return joinDF;
        }
        joinDF.show();
        logger.info("JOIN Data Displayed Successfully! ✅");

        /*
         * COLLECT EACH OPEN ALARM AND PROCESS IT
         */

        List<Row> rowList = joinDF.collectAsList();

        if (rowList.isEmpty()) {
            logger.error("⚠️ No Matching Alerts Found After JOIN. Returning Empty Dataset.");
            return joinDF;
        }

        logger.info("🔁 Starting to Process Each Open Alert (Total: {})", rowList.size());

        for (Row row : rowList) {
            try {
                String entityName = row.getAs("ENTITY_NAME");
                logger.info("🚀 Processing Open Alert With ENTITY_NAME: {}", entityName);
                processEachOpenAlert(row, jobContext);
                logger.info("Processing Open Alert With ENTITY_NAME: {} ✅", entityName);

            } catch (Exception e) {
                logger.error("⚠️ Exception While Processing Alert Row: {} | Message: {} | Error: {}", row,
                        e.getMessage(), e);
            }
        }

        return joinDF;

    }

    private static String getActiveRuleQuery(String domain, String vendor, String technology, String frequency) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT * FROM PERFORMANCE_ALERT pa ");
        sqlQuery.append("WHERE pa.DOMAIN = '").append(domain).append("' AND ");
        sqlQuery.append("pa.VENDOR = '").append(vendor).append("' AND ");
        sqlQuery.append("pa.TECHNOLOGY = '").append(technology).append("' AND ");
        sqlQuery.append("pa.DELETED = 0 AND ");
        sqlQuery.append(
                "UPPER(TRIM(BOTH '\\'' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(pa.CONFIGURATION, \"'closureFrequency':[\", -1), \"]\", 1))) = '")
                .append(frequency.toUpperCase()).append("'");

        logger.info("Active Rule Query: {}", sqlQuery.toString());
        return sqlQuery.toString();
    }

    private static String getActiveLibraryQuery(String domain, String vendor, String technology) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT * FROM ALARM_LIBRARY al ");
        sqlQuery.append("WHERE al.DOMAIN = '").append(domain).append("' AND ");
        sqlQuery.append("al.VENDOR = '").append(vendor).append("' AND ");
        sqlQuery.append("al.TECHNOLOGY = '").append(technology).append("' AND ");
        sqlQuery.append("al.DELETED = 0 AND ");
        sqlQuery.append("al.ENABLED = 1");

        logger.info("Active Library Query: {}", sqlQuery.toString());
        return sqlQuery.toString();
    }

    private static String buildSQLQuery(String domain, String vendor, String technology, String frequency) {

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append(
                "SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(pa.CONFIGURATION, \"'closureexpression':'\", -1), \"'\", 1) AS EXPRESSION, ");

        sqlQuery.append("al.ALARM_IDENTIFIER, ");
        sqlQuery.append("pa.CONFIGURATION ");
        sqlQuery.append("FROM PERFORMANCE_ALERT pa ");
        sqlQuery.append("JOIN ALARM_LIBRARY al ON pa.ALERTID = al.ALARM_IDENTIFIER ");
        // sqlQuery.append("WHERE pa.DOMAIN = '").append(domain).append("' AND ");
        // sqlQuery.append("al.DOMAIN = '").append(domain).append("' AND ");
        // sqlQuery.append("pa.VENDOR = '").append(vendor).append("' AND ");
        // sqlQuery.append("al.VENDOR = '").append(vendor).append("' AND ");
        // sqlQuery.append("pa.TECHNOLOGY = '").append(technology).append("' AND ");
        // sqlQuery.append("al.TECHNOLOGY = '").append(technology).append("' AND ");
        // sqlQuery.append("pa.DELETED = 0 AND ");
        // sqlQuery.append("al.DELETED = 0 AND ");
        // sqlQuery.append(
        // "UPPER(TRIM(BOTH '\\'' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(pa.CONFIGURATION,
        // \"'closureFrequency':[\", -1), \"]\", 1))) = '")
        // .append(frequency.toUpperCase()).append("' AND ");
        // sqlQuery.append("al.ENABLED = 1");

        logger.info("Metric Threshold Rule SQL Query: {}", sqlQuery.toString());
        return sqlQuery.toString();
    }

    private static Dataset<Row> executeQuery(String sqlQuery, JobContext jobContext, String dbName) {

        Dataset<Row> resultDataset = null;

        try {

            if (dbName.contains("FMS")) {

                resultDataset = jobContext.sqlctx().read()
                        .format("jdbc")
                        .option("driver", SPARK_FM_JDBC_DRIVER)
                        .option("url", SPARK_FM_JDBC_URL)
                        .option("user", SPARK_FM_JDBC_USERNAME)
                        .option("password", SPARK_FM_JDBC_PASSWORD)
                        .option("query", sqlQuery)
                        .load();

            } else if (dbName.contains("PERFORMANCE")) {

                resultDataset = jobContext.sqlctx().read()
                        .format("jdbc")
                        .option("driver", SPARK_PM_JDBC_DRIVER)
                        .option("url", SPARK_PM_JDBC_URL)
                        .option("user", SPARK_PM_JDBC_USERNAME)
                        .option("password", SPARK_PM_JDBC_PASSWORD)
                        .option("query", sqlQuery)
                        .load();

            }

            return resultDataset;

        } catch (Exception e) {
            logger.error("⚠️ Exception in Executing Query, Message: " + e.getMessage() + " | Error: " + e);
            return resultDataset;
        }

    }

    private static String buildOpenAlarmQuery(String domain, String vendor, String technology) {
        String openAlarmQuery = "SELECT ALARM_ID_PK, ALARM_EXTERNAL_ID, ALARM_CODE, ALARM_NAME, ENTITY_ID, ENTITY_NAME, SUBENTITY, DOMAIN, VENDOR, TECHNOLOGY, OCCURRENCE FROM ALARM WHERE SENDER_NAME = 'PERFORMANCE_ALERT' AND ALARM_STATUS = 'OPEN' AND DOMAIN = '"
                + domain + "' AND VENDOR = '" + vendor + "' AND TECHNOLOGY = '" + technology + "'";

        logger.info("PM Open Alert SQL Query: {}", openAlarmQuery);
        return openAlarmQuery;
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
        // jobContext.sqlctx().setConf("spark.cassandra.input.consistency.level",
        // "ONE");
        // jobContext.sqlctx().setConf("spark.cassandra.output.consistency.level",
        // "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.query.retry.count", "10");
        jobContext.sqlctx().setConf("spark.cassandra.output.batch.size.rows", "500");
        jobContext.sqlctx().setConf("spark.cassandra.output.concurrent.writes", "3");
        jobContext.sqlctx().setConf("spark.cassandra.connection.remoteConnectionsPerExecutor", "5");
        jobContext.sqlctx().setConf("spark.jdbc.url", SPARK_PM_JDBC_URL);
        jobContext.sqlctx().setConf("spark.jdbc.user", SPARK_PM_JDBC_USERNAME);
        jobContext.sqlctx().setConf("spark.jdbc.password", SPARK_PM_JDBC_PASSWORD);
        return jobContext;
    }

    private static void processEachOpenAlert(Row row, JobContext jobContext) {

        String ALARM_ID_PK = row.getAs("ALARM_ID_PK") != null ? row.getAs("ALARM_ID_PK").toString() : "";
        String ALARM_EXTERNAL_ID = row.getAs("ALARM_EXTERNAL_ID") != null ? row.getAs("ALARM_EXTERNAL_ID").toString()
                : "";
        String ENTITY_ID = row.getAs("ENTITY_ID") != null ? row.getAs("ENTITY_ID").toString() : "";
        String ENTITY_NAME = row.getAs("ENTITY_NAME") != null ? row.getAs("ENTITY_NAME").toString() : "";
        String SUBENTITY = row.getAs("SUBENTITY") != null ? row.getAs("SUBENTITY").toString() : "";
        String DOMAIN = row.getAs("DOMAIN") != null ? row.getAs("DOMAIN").toString() : "";
        String VENDOR = row.getAs("VENDOR") != null ? row.getAs("VENDOR").toString() : "";
        String TECHNOLOGY = row.getAs("TECHNOLOGY") != null ? row.getAs("TECHNOLOGY").toString() : "";
        String EXPRESSION = row.getAs("EXPRESSION") != null ? row.getAs("EXPRESSION").toString() : "";
        String CONFIGURATION = row.getAs("CONFIGURATION") != null ? row.getAs("CONFIGURATION").toString() : "";

        String ALARM_CODE = row.getAs("ALARM_CODE") != null ? row.getAs("ALARM_CODE").toString() : "";
        String ALARM_NAME = row.getAs("ALARM_NAME") != null ? row.getAs("ALARM_NAME").toString() : "";

        Map<String, String> inputMap = new LinkedHashMap<>();
        inputMap.put("ALARM_ID_PK", ALARM_ID_PK);
        inputMap.put("ALARM_EXTERNAL_ID", ALARM_EXTERNAL_ID);
        inputMap.put("ENTITY_ID", ENTITY_ID);
        inputMap.put("ENTITY_NAME", ENTITY_NAME);
        inputMap.put("SUBENTITY", SUBENTITY);
        inputMap.put("DOMAIN", DOMAIN);
        inputMap.put("VENDOR", VENDOR);
        inputMap.put("TECHNOLOGY", TECHNOLOGY);
        inputMap.put("EXPRESSION", EXPRESSION);
        inputMap.put("CONFIGURATION", CONFIGURATION);

        String TIMESTAMP = jobContext.getParameter("TIMESTAMP");
        inputMap.put("TIMESTAMP", TIMESTAMP);

        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        OffsetDateTime dateTime = OffsetDateTime.parse(TIMESTAMP, inputFormatter);
        String DATE = dateTime.format(outputFormatter);
        inputMap.put("DATE", DATE);

        String JOB_FREQUENCY = jobContext.getParameter("JOB_FREQUENCY");
        logger.info("Generated Job Frequency: {}", JOB_FREQUENCY);
        inputMap.put("JOB_FREQUENCY", JOB_FREQUENCY);

        String ROW_KEY_APPENDER = jobContext.getParameter("ROW_KEY_APPENDER");
        logger.info("Generated Row Key Appender: {}", ROW_KEY_APPENDER);
        inputMap.put("ROW_KEY_APPENDER", ROW_KEY_APPENDER);

        String CQL_TABLE_NAME = generateCQLTableNameBasedOnFrequency(jobContext);
        logger.info("Generated CQL Table Name: {}", CQL_TABLE_NAME);
        inputMap.put("CQL_TABLE_NAME", CQL_TABLE_NAME);

        String DATALEVEL = SUBENTITY + "_" + ROW_KEY_APPENDER;
        logger.info("Generated Data Level: {}", DATALEVEL);
        inputMap.put("DATALEVEL", DATALEVEL);

        String LEVEL = SUBENTITY;
        inputMap.put("LEVEL", LEVEL);

        if (SUBENTITY.contains("L0") || SUBENTITY.contains("L1") || SUBENTITY.contains("L2") || SUBENTITY.contains("L3")
                || SUBENTITY.contains("L4")) {

            inputMap.put("IS_NODE_LEVEL", "false");

            if (SUBENTITY.contains("L0")) {
                inputMap.put("NODENAME", "India");
            } else {
                inputMap.put("NODENAME", ENTITY_NAME);
            }
        } else {
            inputMap.put("IS_NODE_LEVEL", "true");
            inputMap.put("NODENAME", ENTITY_ID);
        }

        logger.info("Generated Is Node Level: {}", inputMap.get("IS_NODE_LEVEL"));
        logger.info("Generated Nodename: {}", inputMap.get("NODENAME"));

        List<String> KPI_CODE_LIST = getKPICodeListFromExpression(EXPRESSION);
        logger.info("Generated KPI Code List: {}", KPI_CODE_LIST);
        inputMap.put("KPI_CODE_LIST", KPI_CODE_LIST.toString());

        CONFIGURATION = CONFIGURATION.replace("\"", "");
        JSONObject configJson = new JSONObject(CONFIGURATION);

        String OUT_OF_LAST = "0";
        String INSTANCES = "0";

        JSONObject consistencyJson = configJson.getJSONObject("Consistency");
        if (consistencyJson.has("outOfLast") && !consistencyJson.getString("outOfLast").isEmpty()) {
            OUT_OF_LAST = consistencyJson.getString("outOfLast");
        }

        if (consistencyJson.has("Instances") && !consistencyJson.getString("Instances").isEmpty()) {
            INSTANCES = consistencyJson.getString("Instances");
        }

        inputMap.put("OUT_OF_LAST", OUT_OF_LAST);
        inputMap.put("INSTANCES", INSTANCES);
        inputMap.put("ALARM_CODE", ALARM_CODE);
        inputMap.put("ALARM_NAME", ALARM_NAME);

        DateTimeFormatter inputFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXXXX");
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(
                TIMESTAMP.replace("+0000", "+00:00"), inputFormatter1);
        DateTimeFormatter outputFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String CLOSURE_TIME = offsetDateTime.toLocalDateTime().format(outputFormatter1);
        inputMap.put("CLOSURE_TIME", CLOSURE_TIME);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
        String currentTimeUTC = formatter.format(Instant.now());

        inputMap.put("LAST_PROCESSING_TIME", currentTimeUTC);
        inputMap.put("CLOSURE_RECEPTION_TIME", currentTimeUTC);
        inputMap.put("CLOSURE_PROCESSING_TIME", currentTimeUTC);

        processInputMap(inputMap, jobContext);

    }

    private static List<String> getKPICodeListFromExpression(String expression) {

        if (expression == null || expression.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> kpiCodes = new ArrayList<>();
        Pattern pattern = Pattern.compile("KPI#(\\d+)");
        Matcher matcher = pattern.matcher(expression);

        while (matcher.find()) {
            String kpiCode = matcher.group(1);
            if (!kpiCodes.contains(kpiCode)) {
                kpiCodes.add(kpiCode);
            }
        }
        return kpiCodes;
    }

    private static void processInputMap(Map<String, String> inputMap, JobContext jobContext) {

        logger.info("Generated Input Map: {}", inputMap);

        String cqlFilter = bildCQLLoadFilter(inputMap, jobContext);

        logger.info("Generated CQL Filter: {}", cqlFilter);

        Dataset<Row> cqlResultDF = getCQLData(cqlFilter, inputMap, jobContext);

        if (cqlResultDF.isEmpty()) {
            logger.error("⚠️ No CQL Data Found. Returning Empty Dataset.");
            return;
        }

        cqlResultDF.show(false);
        logger.info("CQL Data Displayed Successfully! ✅");

        List<Row> rowList = cqlResultDF.collectAsList();

        if (rowList.isEmpty()) {
            logger.error("⚠️ No Row List Found. Returning Empty List.");
            return;
        }

        for (Row row : rowList) {

            String entityName = inputMap.get("ENTITY_NAME");

            logger.info("🚀 Processing Row For ENTITY_NAME: {}", entityName);
            processEachCQLRow(row, inputMap, jobContext);
            logger.info("✅ Completed Processing Row For ENTITY_NAME: {}", entityName);
        }

    }

    private static void processEachCQLRow(Row row, Map<String, String> inputMap, JobContext jobContext) {

        boolean isNodeLevel = Boolean.parseBoolean(inputMap.get("IS_NODE_LEVEL"));

        if (isNodeLevel) {
            Map<String, Map<String, String>> resultMap = getMapForNodeLevel(row, inputMap, jobContext);
            logger.info("📊 Result Map For Node Level: {}", resultMap);

            processEachResultMap(resultMap, jobContext, inputMap);
        } else {
            Map<String, Map<String, String>> resultMap = getMapForNonNodeLevel(row, inputMap, jobContext);
            logger.info("📊 Result Map For Non Node Level: {}", resultMap);

            processEachResultMap(resultMap, jobContext, inputMap);
        }

    }

    private static void processEachResultMap(Map<String, Map<String, String>> resultMap, JobContext jobContext,
            Map<String, String> inputMap) {

        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");

        logger.info("📊 KPI Value Map: {}", kpiValueMap);

        String expression = inputMap.get("EXPRESSION");

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

        try {
            evaluateExpression(optimizedExpression.toString(), resultMap, jobContext, inputMap);
        } catch (SQLException e) {
        }
    }

    private static void evaluateExpression(String expression, Map<String, Map<String, String>> resultMap,
            JobContext jobContext, Map<String, String> inputMap) throws SQLException {
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
            startAlertGenerationProcess(resultMap, jobContext, inputMap);

        } else {

            logger.info("📊 Expression Evaluated to FALSE (0). Skipping Alert Generation Process.");
        }
    }

    private static void startAlertGenerationProcess(Map<String, Map<String, String>> resultMap, JobContext jobContext,
            Map<String, String> inputMap) throws SQLException {

        String outOfLast = inputMap.get("OUT_OF_LAST");

        if (outOfLast != null && !outOfLast.isEmpty() && !outOfLast.equals("0")) {

            logger.info("📊 Out of Last is Provided. Proceeding with Alert Generation Process With Consistency Check.");

            boolean isConsistencyMet = isConsistencyMet(resultMap, jobContext, inputMap);

            if (isConsistencyMet) {
                logger.info("✅ Consistency Check PASSED. Proceeding With Alert Updation.");
                updateAlarm(inputMap);
            } else {
                logger.info("⚠️ Consistency Check FAILED. Skipping Alert Updation.");
            }

        } else {

            logger.info(
                    "📊 Out of Last is Not Provided. Proceeding with Alert Generation Process With No Consistency Check.");
            updateAlarm(inputMap);
        }

    }

    private static void updateAlarm(Map<String, String> inputMap) throws SQLException {

        Map<String, String> resultSetToUpdate = getResultSetToUpdate(inputMap);

        logger.info("Generated Map to Update Alarm: {}", resultSetToUpdate);

        String sqlQuery = "UPDATE ALARM SET ALARM_STATUS = ?, SEVERITY = ?, LAST_PROCESSING_TIME = ?, CLOSURE_TIME = ?, CLOSURE_RECEPTION_TIME = ?, CLOSURE_PROCESSING_TIME = ?"
                +
                "WHERE ALARM_EXTERNAL_ID = ? AND ALARM_CODE = ? AND ALARM_NAME = ? AND SUBENTITY = ? AND ENTITY_NAME = ? "
                +
                "AND ALARM_STATUS = 'OPEN' AND DOMAIN = ? AND VENDOR = ? AND TECHNOLOGY = ?";

        Connection connection = null;
        connection = getDatabaseConnection("FMS");
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

            preparedStatement.setString(1, resultSetToUpdate.get("ALARM_STATUS"));
            preparedStatement.setString(2, resultSetToUpdate.get("SEVERITY"));
            preparedStatement.setString(3, resultSetToUpdate.get("LAST_PROCESSING_TIME"));
            preparedStatement.setString(4, resultSetToUpdate.get("CLOSURE_TIME"));
            preparedStatement.setString(5, resultSetToUpdate.get("CLOSURE_RECEPTION_TIME"));
            preparedStatement.setString(6, resultSetToUpdate.get("CLOSURE_PROCESSING_TIME"));
            preparedStatement.setString(7, resultSetToUpdate.get("ALARM_EXTERNAL_ID"));
            preparedStatement.setString(8, resultSetToUpdate.get("ALARM_CODE"));
            preparedStatement.setString(9, resultSetToUpdate.get("ALARM_NAME"));
            preparedStatement.setString(10, resultSetToUpdate.get("SUBENTITY"));
            preparedStatement.setString(11, resultSetToUpdate.get("ENTITY_NAME"));
            preparedStatement.setString(12, resultSetToUpdate.get("DOMAIN"));
            preparedStatement.setString(13, resultSetToUpdate.get("VENDOR"));
            preparedStatement.setString(14, resultSetToUpdate.get("TECHNOLOGY"));

            preparedStatement.executeUpdate();

            logger.info("\n======================= 🔄 ALARM UPDATED 🔄 =======================\n" +
                    "🔔 Alarm Name        : {}\n" +
                    "🆔 Alarm Identifier  : {}\n" +
                    "🏷️  Equipment ID      : {}\n" +
                    "⚙️ Status            : {}\n" +
                    "⚠️ Severity          : {}\n" +
                    "===================================================================",
                    resultSetToUpdate.get("ALARM_NAME"),
                    resultSetToUpdate.get("ALARM_EXTERNAL_ID"),
                    resultSetToUpdate.get("ENTITY_NAME"),
                    resultSetToUpdate.get("ALARM_STATUS"),
                    resultSetToUpdate.get("SEVERITY"));

        }

    }

    public static Map<String, String> getResultSetToUpdate(Map<String, String> inputMap) {

        Map<String, String> resultSetToUpdate = new LinkedHashMap<>();
        resultSetToUpdate.put("ALARM_STATUS", "CLOSED");
        resultSetToUpdate.put("SEVERITY", "CLEARED");
        resultSetToUpdate.put("LAST_PROCESSING_TIME", inputMap.get("LAST_PROCESSING_TIME"));
        resultSetToUpdate.put("CLOSURE_TIME", inputMap.get("CLOSURE_TIME"));
        resultSetToUpdate.put("CLOSURE_RECEPTION_TIME", inputMap.get("CLOSURE_RECEPTION_TIME"));
        resultSetToUpdate.put("CLOSURE_PROCESSING_TIME", inputMap.get("CLOSURE_PROCESSING_TIME"));
        resultSetToUpdate.put("ALARM_EXTERNAL_ID", inputMap.get("ALARM_EXTERNAL_ID"));
        resultSetToUpdate.put("ALARM_CODE", inputMap.get("ALARM_CODE"));
        resultSetToUpdate.put("ALARM_NAME", inputMap.get("ALARM_NAME"));
        resultSetToUpdate.put("SUBENTITY", inputMap.get("SUBENTITY"));
        resultSetToUpdate.put("ENTITY_NAME", inputMap.get("ENTITY_NAME"));
        resultSetToUpdate.put("DOMAIN", inputMap.get("DOMAIN"));
        resultSetToUpdate.put("VENDOR", inputMap.get("VENDOR"));
        resultSetToUpdate.put("TECHNOLOGY", inputMap.get("TECHNOLOGY"));
        return resultSetToUpdate;

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

    private static boolean isConsistencyMet(Map<String, Map<String, String>> resultMap, JobContext jobContext,
            Map<String, String> inputMap) {

        String timestamp = inputMap.get("TIMESTAMP");

        return checkConsistency(timestamp, resultMap, jobContext, inputMap);
    }

    public static String reduceFrequencyFromTimestamp(String timestamp, JobContext jobContext) {

        String jobFrequency = jobContext.getParameter("JOB_FREQUENCY");

        long reduceMinutes = 0L;

        switch (jobFrequency.toUpperCase()) {
            case "15 MIN":
                reduceMinutes = 15;
                break;
            case "QUARTERLY":
                reduceMinutes = 15;
                break;
            case "FIVEMIN":
                reduceMinutes = 5;
                break;
            case "5 MIN":
                reduceMinutes = 5;
                break;
            case "PERHOUR":
                reduceMinutes = 60;
                break;
            case "HOURLY":
                reduceMinutes = 60;
                break;
            case "PERDAY":
                reduceMinutes = 1440;
                break;
            case "DAILY":
                reduceMinutes = 1440;
                break;
            case "PERWEEK":
                reduceMinutes = 10080;
                break;
            case "WEEKLY":
                reduceMinutes = 10080;
                break;
            case "PERMONTH":
                reduceMinutes = 43200;
                break;
            case "MONTHLY":
                reduceMinutes = 43200;
                break;
            default:
                throw new IllegalArgumentException("Unsupported Job Frequency: " + jobFrequency);
        }

        DateTimeFormatter inputFormatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .appendPattern("xx")
                .toFormatter();

        DateTimeFormatter outputFormatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.MICRO_OF_SECOND, 6, 6, true)
                .appendPattern("xx")
                .toFormatter();

        try {
            OffsetDateTime dateTime = OffsetDateTime.parse(timestamp, inputFormatter)
                    .withOffsetSameInstant(ZoneOffset.UTC);
            OffsetDateTime newTime = dateTime.minusMinutes(reduceMinutes);
            return outputFormatter.format(newTime);
        } catch (Exception e) {
            OffsetDateTime fallbackTime = OffsetDateTime.now(ZoneOffset.UTC)
                    .withSecond(0)
                    .withNano(0)
                    .minusMinutes(reduceMinutes);
            return outputFormatter.format(fallbackTime);
        }

    }

    private static boolean checkConsistency(String timestamp, Map<String, Map<String, String>> resultMap,
            JobContext jobContext, Map<String, String> inputMap) {

        logger.info("Current Timestamp: {}", timestamp);
        timestamp = reduceFrequencyFromTimestamp(timestamp, jobContext);
        logger.info("Reduced Timestamp: {}", timestamp);

        String nodename;

        String level = inputMap.get("LEVEL");
        if (level.contains("L0")) {
            nodename = resultMap.get("nodeDetailsMap").get("ENTITY_NAME");
        } else {
            nodename = resultMap.get("nodeDetailsMap").get("ENTITY_ID");
        }
        int requiredInstances = Integer.parseInt(inputMap.get("INSTANCES"));
        int maxAttempts = Integer.parseInt(inputMap.get("OUT_OF_LAST"));

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

            String cqlQuery = buildCQLQueryForConsistency(timestamp, inputMap, nodename);
            logger.info("CQL Query For Attempt {} : {}", attempt, cqlQuery);

            List<Row> rows = getCQLData(cqlQuery, jobContext, inputMap);

            if (rows != null && !rows.isEmpty()) {
                for (Row row : rows) {

                    if (processEachCQLRowForConsitency(row, inputMap, jobContext)) {

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

    private static boolean processEachCQLRowForConsitency(Row row, Map<String, String> inputMap,
            JobContext jobContext) {

        boolean isNodeLevel = Boolean.parseBoolean(inputMap.get("IS_NODE_LEVEL"));

        if (isNodeLevel) {
            Map<String, Map<String, String>> resultMap = getMapForNodeLevel(row, inputMap, jobContext);
            return processEachResultMapForConsitency(resultMap, inputMap);
        } else {
            Map<String, Map<String, String>> resultMap = getMapForNonNodeLevel2(row, inputMap, jobContext);
            return processEachResultMapForConsitency(resultMap, inputMap);
        }

    }

    private static boolean processEachResultMapForConsitency(Map<String, Map<String, String>> resultMap,
            Map<String, String> inputMap) {

        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");
        String expression = inputMap.get("EXPRESSION");
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

    private static String buildCQLQueryForConsistency(String timestamp, Map<String, String> inputMap,
            String nodename) {

        String kpiCodeList = inputMap.get("KPI_CODE_LIST");
        String datalevel = inputMap.get("DATALEVEL");
        String domain = inputMap.get("DOMAIN");
        String vendor = inputMap.get("VENDOR");
        String technology = inputMap.get("TECHNOLOGY");

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
            cqlQuery.append(", metajson['ENTITY_NAME']");
        } else if (datalevel.contains("L1")) {
            cqlQuery.append(", metajson['L1']");
        } else if (datalevel.contains("L2")) {
            cqlQuery.append(", metajson['L1'], metajson['L2']");
        } else if (datalevel.contains("L3")) {
            cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['L3']");
        } else if (datalevel.contains("L4")) {
            cqlQuery.append(", metajson['L1'], metajson['L2'], metajson['L3'], metajson['L4']");
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
        OffsetDateTime utcTime = odt.withOffsetSameInstant(ZoneOffset.UTC);
        String date = utcTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        cqlQuery.append(" date = '").append(date).append("' AND ");

        cqlQuery.append(" nodename = '").append(nodename).append("' AND ");

        String utcTimestampStr = utcTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // cqlQuery.append(" timestamp = '").append(timestamp.toString()).append("'");
        cqlQuery.append(" timestamp = '").append(utcTimestampStr).append("'");

        logger.info("📊 CQL Query For Consistency: {}", cqlQuery.toString());

        return cqlQuery.toString();
    }

    private static List<Row> getCQLData(String originalQuery, JobContext jobContext,
            Map<String, String> inputConfiMap) {

        Map<String, String> splitQueryMap = splitCQLQuery(originalQuery);
        String selectQuery = splitQueryMap.get("selectQuery");
        String filterQuery = splitQueryMap.get("filterQuery");

        logger.info("Filter Query For Consistency: {}", filterQuery);

        Dataset<Row> cqlDF = getCQLDataUsingSpark(filterQuery, jobContext, inputConfiMap);
        cqlDF.createOrReplaceTempView("CQLResult");
        cqlDF.show(false);

        Dataset<Row> filterDF = jobContext.sqlctx().sql(selectQuery);
        filterDF.show(false);

        return filterDF.collectAsList();
    }

    private static Dataset<Row> getCQLDataUsingSpark(String cqlFilter, JobContext jobContext,
            Map<String, String> inputConfiMap) {

        String cqlTableName = inputConfiMap.get("CQL_TABLE_NAME");
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

    private static Map<String, String> splitCQLQuery(String originalQuery) {
        Map<String, String> result = new HashMap<>();
        String cleanedQuery = originalQuery.trim().replaceAll("\\s+", " ");
        int whereIndex = cleanedQuery.toUpperCase().indexOf("WHERE");
        if (whereIndex == -1) {
            result.put("selectQuery", cleanedQuery);
            result.put("filterQuery", "");
            return result;
        }

        String selectPart = cleanedQuery.substring(0, whereIndex).trim();
        String filterPart = cleanedQuery.substring(whereIndex + "WHERE".length()).trim();

        result.put("selectQuery", selectPart);
        result.put("filterQuery", filterPart);
        return result;
    }

    @SuppressWarnings("deprecation")
    private static Map<String, Map<String, String>> getMapForNodeLevel(Row row, Map<String, String> inputMap,
            JobContext jobContext) {

        String kpiCodeList = inputMap.get("KPI_CODE_LIST");

        List<String> kpiCodes = Arrays.stream(kpiCodeList.replace("[", "").replace("]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        Map<String, String> kpiValueMap = new HashMap<>();

        logger.info("kpiCodes : {}", kpiCodes);

        // for (String kpiCode : kpiCodes) {

        // String kpiColumn = "kpijson['" + kpiCode + "']";
        // Object kpiValueObj = row.getAs(kpiColumn);
        // String kpiValue = null;

        // if (kpiValueObj != null) {
        // try {
        // double kpiDouble = Double.parseDouble(kpiValueObj.toString());
        // kpiValue = String.format("%.4f", kpiDouble);
        // kpiValueMap.put(kpiCode, kpiValue);
        // } catch (NumberFormatException e) {
        // logger.error("Error Parsing KPI Value, Message: {}, Error: {}",
        // e.getMessage(), e);
        // }
        // }

        // }

        for (String kpiCode : kpiCodes) {
            scala.collection.immutable.Map<String, String> scalaMap = row.getAs("kpijson");

            if (scalaMap != null) {
                Map<String, String> kpiJsonMap = JavaConverters.mapAsJavaMap(scalaMap);

                String kpiValue = kpiJsonMap.get(kpiCode);
                if (kpiValue != null) {
                    try {
                        double kpiDouble = Double.parseDouble(kpiValue);
                        kpiValueMap.put(kpiCode, String.format("%.4f", kpiDouble));
                    } catch (NumberFormatException e) {
                        logger.error("❌ Error Parsing KPI Value for KPI '{}', Raw Value: '{}', Message: {}",
                                kpiCode, kpiValue, e.getMessage(), e);
                    }
                } else {
                    logger.error("⚠️ KPI '{}' not found in kpijson map.", kpiCode);
                }
            } else {
                logger.error("⚠️ kpijson Map is null for row: {}", row);
            }
        }

        logger.info("KPI Value Map: {}", kpiValueMap);

        scala.collection.immutable.Map<String, String> scalaMap = row.getAs("metajson");
        Map<String, String> metajsonMap = JavaConverters.mapAsJavaMap(scalaMap);

        // String neid = row.getAs("metajson[ENTITY_ID]") != null ? (String)
        // row.getAs("metajson[ENTITY_ID]") : "";
        // String nename = row.getAs("metajson[ENTITY_NAME]") != null ? (String)
        // row.getAs("metajson[ENTITY_NAME]") : "";
        // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
        // row.getAs("metajson[L1]") : "";
        // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
        // row.getAs("metajson[L2]") : "";
        // String geoL3 = row.getAs("metajson[L3]") != null ? (String)
        // row.getAs("metajson[L3]") : "";
        // String geoL4 = row.getAs("metajson[L4]") != null ? (String)
        // row.getAs("metajson[L4]") : "";
        // String entityType = row.getAs("metajson[ENTITY_TYPE]") != null ? (String)
        // row.getAs("metajson[ENTITY_TYPE]") : "";
        // String entityStatus = row.getAs("metajson[NS]") != null ? (String)
        // row.getAs("metajson[NS]") : "";
        // String nodename = row.getAs("nodename") != null ? (String)
        // row.getAs("nodename") : "";

        String neid = metajsonMap.getOrDefault("ENTITY_ID", "");
        String nename = metajsonMap.getOrDefault("ENTITY_NAME", "");
        String geoL1 = metajsonMap.getOrDefault("L1", "");
        String geoL2 = metajsonMap.getOrDefault("L2", "");
        String geoL3 = metajsonMap.getOrDefault("L3", "");
        String geoL4 = metajsonMap.getOrDefault("L4", "");
        String entityType = metajsonMap.getOrDefault("ENTITY_TYPE", "");
        String entityStatus = metajsonMap.getOrDefault("NS", "");

        String nodename = row.getAs("nodename") != null ? row.getAs("nodename").toString() : "";

        Map<String, String> nodeDetailsMap = new HashMap<>();
        nodeDetailsMap.put("ENTITY_ID", neid);
        nodeDetailsMap.put("ENTITY_NAME", nename);
        nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
        nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
        nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
        nodeDetailsMap.put("GEOGRAPHY_L4_NAME", geoL4);
        nodeDetailsMap.put("ENTITY_TYPE", entityType);
        nodeDetailsMap.put("ENTITY_STATUS", entityStatus);
        nodeDetailsMap.put("nodename", nodename);

        logger.info("Node Details Map: {}", nodeDetailsMap);

        Map<String, Map<String, String>> resultMap = new HashMap<>();
        resultMap.put("nodeDetailsMap", nodeDetailsMap);
        resultMap.put("kpiValueMap", kpiValueMap);
        return resultMap;
    }

    @SuppressWarnings("deprecation")
    private static Map<String, Map<String, String>> getMapForNonNodeLevel(Row row, Map<String, String> inputMap,
            JobContext jobContext) {

        String kpiCodeList = inputMap.get("KPI_CODE_LIST");

        List<String> kpiCodes = Arrays.stream(kpiCodeList.replace("[", "").replace("]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        logger.info("KPI Codes: {}", kpiCodes);

        Map<String, String> kpiValueMap = new HashMap<>();

        for (String kpiCode : kpiCodes) {
            scala.collection.immutable.Map<String, String> scalaMap = null;
            try {
                scalaMap = row.getAs("kpijson");
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (scalaMap != null) {
                Map<String, String> kpiJsonMap = JavaConverters.mapAsJavaMap(scalaMap);

                logger.info("kpiJsonMap : {}", kpiJsonMap);

                String kpiValue = kpiJsonMap.get(kpiCode);
                if (kpiValue != null) {
                    try {
                        double kpiDouble = Double.parseDouble(kpiValue);
                        kpiValueMap.put(kpiCode, String.format("%.4f", kpiDouble));
                    } catch (NumberFormatException e) {
                        logger.error("❌ Error Parsing KPI Value for KPI '{}', Raw Value: '{}', Message: {}",
                                kpiCode, kpiValue, e.getMessage(), e);
                    }
                } else {
                    logger.error("⚠️ KPI '{}' not found in kpijson map.", kpiCode);
                }
            } else {
                logger.error("⚠️ kpijson Map is null for row: {}", row);
            }
        }

        logger.info("KPI Value Map : {}", kpiValueMap);

        Map<String, Map<String, String>> resultMap = new HashMap<>();
        resultMap.put("kpiValueMap", kpiValueMap);

        String level = inputMap.get("LEVEL");
        logger.info("Map Level: {}", level);
        ;
        Map<String, String> nodeDetailsMap = new HashMap<>();

        Map<String, String> metaJsonMap = null;

        scala.collection.immutable.Map<String, String> scalaMetaMap = row.getAs("metajson");
        if (scalaMetaMap != null) {
            metaJsonMap = JavaConverters.mapAsJavaMap(scalaMetaMap);
        } else {
            metaJsonMap = new HashMap<>();
        }

        switch (level) {
            case "L0": {
                // String nename = row.getAs("metajson[ENTITY_NAME]") != null ? (String)
                // row.getAs("metajson[ENTITY_NAME]") : "";
                String nename = metaJsonMap.getOrDefault("ENTITY_NAME", "");
                nodeDetailsMap.put("ENTITY_NAME", nename);
                break;
            }

            case "L1": {
                // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
                // row.getAs("metajson[L1]") : "";
                String geoL1 = metaJsonMap.getOrDefault("L1", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                break;
            }

            case "L2": {
                // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
                // row.getAs("metajson[L1]") : "";
                // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
                // row.getAs("metajson[L2]") : "";
                String geoL1 = metaJsonMap.getOrDefault("L1", "");
                String geoL2 = metaJsonMap.getOrDefault("L2", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
                break;
            }
            case "L3": {
                // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
                // row.getAs("metajson[L1]") : "";
                // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
                // row.getAs("metajson[L2]") : "";
                // String geoL3 = row.getAs("metajson[L3]") != null ? (String)
                // row.getAs("metajson[L3]") : "";
                String geoL1 = metaJsonMap.getOrDefault("L1", "");
                String geoL2 = metaJsonMap.getOrDefault("L2", "");
                String geoL3 = metaJsonMap.getOrDefault("L3", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
                nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
                break;
            }

            case "L4": {
                // String geoL1 = row.getAs("metajson[L1]") != null ? (String)
                // row.getAs("metajson[L1]") : "";
                // String geoL2 = row.getAs("metajson[L2]") != null ? (String)
                // row.getAs("metajson[L2]") : "";
                // String geoL3 = row.getAs("metajson[L3]") != null ? (String)
                // row.getAs("metajson[L3]") : "";
                // String geoL4 = row.getAs("metajson[L4]") != null ? (String)
                // row.getAs("metajson[L4]") : "";
                String geoL1 = metaJsonMap.getOrDefault("L1", "");
                String geoL2 = metaJsonMap.getOrDefault("L2", "");
                String geoL3 = metaJsonMap.getOrDefault("L3", "");
                String geoL4 = metaJsonMap.getOrDefault("L4", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
                nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
                nodeDetailsMap.put("GEOGRAPHY_L4_NAME", geoL4);
                break;
            }

            default: {
                break;
            }
        }

        String entityType = metaJsonMap.getOrDefault("ENTITY_TYPE", "");
        nodeDetailsMap.put("ENTITY_TYPE", entityType);

        resultMap.put("nodeDetailsMap", nodeDetailsMap);
        return resultMap;
    }

    @SuppressWarnings("deprecation")
    private static Map<String, Map<String, String>> getMapForNonNodeLevel2(Row row, Map<String, String> inputMap,
            JobContext jobContext) {

        String kpiCodeList = inputMap.get("KPI_CODE_LIST");

        List<String> kpiCodes = Arrays.stream(kpiCodeList.replace("[", "").replace("]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        logger.info("KPI Codes: {}", kpiCodes);

        Map<String, String> kpiValueMap = new HashMap<>();

        // for (String kpiCode : kpiCodes) {

        // @SuppressWarnings("deprecation")
        // Map<String, String> kpiJsonMap = JavaConverters.mapAsJavaMap(scalaMap);

        // logger.info("kpiJsonMap : {}", kpiJsonMap);

        // String kpiValue = kpiJsonMap.get(kpiCode);
        // if (kpiValue != null) {
        // try {
        // double kpiDouble = Double.parseDouble(kpiValue);
        // kpiValueMap.put(kpiCode, String.format("%.4f", kpiDouble));
        // } catch (NumberFormatException e) {
        // logger.error("❌ Error Parsing KPI Value for KPI '{}', Raw Value: '{}',
        // Message: {}",
        // kpiCode, kpiValue, e.getMessage(), e);
        // }
        // } else {
        // logger.error("⚠️ KPI '{}' not found in kpijson map.", kpiCode);

        // }

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

        logger.info("KPI Value Map 2: {}", kpiValueMap);

        Map<String, Map<String, String>> resultMap = new HashMap<>();
        resultMap.put("kpiValueMap", kpiValueMap);

        String level = inputMap.get("LEVEL");
        logger.info("Map Level: {}", level);

        Map<String, String> nodeDetailsMap = new HashMap<>();

        switch (level) {
            case "L0": {
                String nename = row.getAs("metajson[ENTITY_NAME]") != null ? (String) row.getAs("metajson[ENTITY_NAME]")
                        : "";
                // String nename = metaJsonMap.getOrDefault("ENTITY_NAME", "");
                nodeDetailsMap.put("ENTITY_NAME", nename);
                break;
            }

            case "L1": {
                String geoL1 = row.getAs("metajson[L1]") != null ? (String) row.getAs("metajson[L1]") : "";
                // String geoL1 = metaJsonMap.getOrDefault("L1", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                break;
            }

            case "L2": {
                String geoL1 = row.getAs("metajson[L1]") != null ? (String) row.getAs("metajson[L1]") : "";
                String geoL2 = row.getAs("metajson[L2]") != null ? (String) row.getAs("metajson[L2]") : "";
                // String geoL1 = metaJsonMap.getOrDefault("L1", "");
                // String geoL2 = metaJsonMap.getOrDefault("L2", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
                break;
            }
            case "L3": {
                String geoL1 = row.getAs("metajson[L1]") != null ? (String) row.getAs("metajson[L1]") : "";
                String geoL2 = row.getAs("metajson[L2]") != null ? (String) row.getAs("metajson[L2]") : "";
                String geoL3 = row.getAs("metajson[L3]") != null ? (String) row.getAs("metajson[L3]") : "";
                // String geoL1 = metaJsonMap.getOrDefault("L1", "");
                // String geoL2 = metaJsonMap.getOrDefault("L2", "");
                // String geoL3 = metaJsonMap.getOrDefault("L3", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
                nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
                break;
            }

            case "L4": {
                String geoL1 = row.getAs("metajson[L1]") != null ? (String) row.getAs("metajson[L1]") : "";
                String geoL2 = row.getAs("metajson[L2]") != null ? (String) row.getAs("metajson[L2]") : "";
                String geoL3 = row.getAs("metajson[L3]") != null ? (String) row.getAs("metajson[L3]") : "";
                String geoL4 = row.getAs("metajson[L4]") != null ? (String) row.getAs("metajson[L4]") : "";
                // String geoL1 = metaJsonMap.getOrDefault("L1", "");
                // String geoL2 = metaJsonMap.getOrDefault("L2", "");
                // String geoL3 = metaJsonMap.getOrDefault("L3", "");
                // String geoL4 = metaJsonMap.getOrDefault("L4", "");
                nodeDetailsMap.put("GEOGRAPHY_L1_NAME", geoL1);
                nodeDetailsMap.put("GEOGRAPHY_L2_NAME", geoL2);
                nodeDetailsMap.put("GEOGRAPHY_L3_NAME", geoL3);
                nodeDetailsMap.put("GEOGRAPHY_L4_NAME", geoL4);
                break;
            }

            default: {
                break;
            }
        }

        String entityType = row.getAs("metajson[ENTITY_TYPE]") != null ? (String) row.getAs("metajson[ENTITY_TYPE]")
                : "";
        nodeDetailsMap.put("ENTITY_TYPE", entityType);

        logger.info("Node Details Map 2: {}", nodeDetailsMap);

        resultMap.put("nodeDetailsMap", nodeDetailsMap);
        return resultMap;
    }

    private static Dataset<Row> getCQLData(String cqlFilter, Map<String, String> inputMap, JobContext jobContext) {

        Dataset<Row> resultDataFrame = null;
        // String cqlConsistencyLevel = "ONE";

        jobContext.sqlctx().sparkSession().conf().set("spark.sql.session.timeZone", "UTC");

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

                // resultDataFrame = jobContext.sqlctx().read()
                // .format("org.apache.spark.sql.cassandra")
                // .options(Map.of(
                // "table", inputMap.get("CQL_TABLE_NAME"),
                // "keyspace", jobContext.getParameter("SPARK_CASSANDRA_KEYSPACE_PM"),
                // "pushdown", "true",
                // "consistency.level", cqlConsistencyLevel))
                // .load()
                // .filter(cqlFilter);

                resultDataFrame = jobContext.sqlctx().read()
                        .format("org.apache.spark.sql.cassandra")
                        .options(Map.of(
                                "table", inputMap.get("CQL_TABLE_NAME"),
                                "keyspace", jobContext.getParameter("SPARK_CASSANDRA_KEYSPACE_PM"),
                                "pushdown", "true"))
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

    private static String bildCQLLoadFilter(Map<String, String> inputMap, JobContext jobContext) {

        StringBuilder cqlFilter = new StringBuilder();
        cqlFilter.append("domain = '").append(inputMap.get("DOMAIN")).append("' AND ");
        cqlFilter.append("vendor = '").append(inputMap.get("VENDOR")).append("' AND ");
        cqlFilter.append("technology = '").append(inputMap.get("TECHNOLOGY")).append("' AND ");
        cqlFilter.append("datalevel = '").append(inputMap.get("DATALEVEL")).append("' AND ");
        cqlFilter.append("date = '").append(inputMap.get("DATE")).append("' AND ");
        cqlFilter.append("nodename = '").append(inputMap.get("NODENAME")).append("' AND ");
        cqlFilter.append("timestamp = '").append(inputMap.get("TIMESTAMP")).append("'");
        return cqlFilter.toString();
    }

    private static String generateCQLTableNameBasedOnFrequency(JobContext jobContext) {

        String jobFrequency = jobContext.getParameter("JOB_FREQUENCY");

        return switch (jobFrequency.toUpperCase()) {
            case "5 MIN" -> "combine5minpm";
            case "FIVEMIN" -> "combine5minpm";
            case "15 MIN" -> "combinequarterlypm";
            case "QUARTERLY" -> "combinequarterlypm";
            case "PERHOUR" -> "combinehourlypm";
            case "HOURLY" -> "combinehourlypm";
            case "PERDAY" -> "combinedailypm";
            case "DAILY" -> "combinedailypm";
            case "PERWEEK" -> "combineweeklypm";
            case "WEEKLY" -> "combineweeklypm";
            case "PERMONTH" -> "combinemonthlypm";
            case "MONTHLY" -> "combinemonthlypm";
            default -> "combinequarterlypm";
        };
    }

}
