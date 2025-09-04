package com.enttribe.pm.job.alert.loop;

import com.enttribe.sparkrunner.processors.Processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import com.enttribe.sparkrunner.util.Expression;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProcessCloseCQLResult extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ProcessCloseCQLResult.class);
    private static Map<String, String> jobContextMap = new HashMap<>();

    private static final String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static final String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static final String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static final String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";
    private static final String SPARK_FM_JDBC_DRIVER = "SPARK_FM_JDBC_DRIVER";
    private static final String SPARK_FM_JDBC_URL = "SPARK_FM_JDBC_URL";
    private static final String SPARK_FM_JDBC_USERNAME = "SPARK_FM_JDBC_USERNAME";
    private static final String SPARK_FM_JDBC_PASSWORD = "SPARK_FM_JDBC_PASSWORD";
    private static final String SPARK_CASSANDRA_KEYSPACE_PM = "SPARK_CASSANDRA_KEYSPACE_PM"; //
    private static final String SPARK_CASSANDRA_HOST = "SPARK_CASSANDRA_HOST";
    private static final String SPARK_CASSANDRA_PORT = "SPARK_CASSANDRA_PORT";
    private static final String SPARK_CASSANDRA_DATACENTER = "SPARK_CASSANDRA_DATACENTER";
    private static final String SPARK_CASSANDRA_USERNAME = "SPARK_CASSANDRA_USERNAME";
    private static final String SPARK_CASSANDRA_PASSWORD = "SPARK_CASSANDRA_PASSWORD";

    private static String sparkPMJdbcDriver = null;
    private static String sparkPMJdbcUrl = null;
    private static String sparkPMJdbcUsername = null;
    private static String sparkPMJdbcPassword = null;
    private static String sparkFMJdbcDriver = null;
    private static String sparkFMJdbcUrl = null;
    private static String sparkFMJdbcUsername = null;
    private static String sparkFMJdbcPassword = null;
    private static String sparkCassandraKeyspacePM = null;
    private static String sparkCassandraHost = null;
    private static String sparkCassandraPort = null;
    private static String sparkCassandraDatacenter = null;
    private static String sparkCassandraUsername = null;
    private static String sparkCassandraPassword = null;

    public ProcessCloseCQLResult() {
        super();
    }

    public ProcessCloseCQLResult(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
    }

    public ProcessCloseCQLResult(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (this.dataFrame == null || this.dataFrame.isEmpty()) {
            return this.dataFrame;
        }

        long startTime = System.currentTimeMillis();

        logger.info("[ProcessCloseCQLResult] Execution Started!");

        jobContextMap = jobContext.getParameters();

        sparkPMJdbcDriver = jobContextMap.get(SPARK_PM_JDBC_DRIVER);
        sparkPMJdbcUrl = jobContextMap.get(SPARK_PM_JDBC_URL);
        sparkPMJdbcUsername = jobContextMap.get(SPARK_PM_JDBC_USERNAME);
        sparkPMJdbcPassword = jobContextMap.get(SPARK_PM_JDBC_PASSWORD);
        sparkFMJdbcDriver = jobContextMap.get(SPARK_FM_JDBC_DRIVER);
        sparkFMJdbcUrl = jobContextMap.get(SPARK_FM_JDBC_URL);
        sparkFMJdbcUsername = jobContextMap.get(SPARK_FM_JDBC_USERNAME);
        sparkFMJdbcPassword = jobContextMap.get(SPARK_FM_JDBC_PASSWORD);
        sparkCassandraKeyspacePM = jobContextMap.get(SPARK_CASSANDRA_KEYSPACE_PM);
        sparkCassandraHost = jobContextMap.get(SPARK_CASSANDRA_HOST);
        sparkCassandraPort = jobContextMap.get(SPARK_CASSANDRA_PORT);
        sparkCassandraDatacenter = jobContextMap.get(SPARK_CASSANDRA_DATACENTER);
        sparkCassandraUsername = jobContextMap.get(SPARK_CASSANDRA_USERNAME);
        sparkCassandraPassword = jobContextMap.get(SPARK_CASSANDRA_PASSWORD);

        jobContext = setSparkConf(jobContext);
        logger.info("PM JDBC Credentials: Driver={}, URL={}, User={}, Password={}",
                sparkPMJdbcDriver,
                sparkPMJdbcUrl,
                sparkPMJdbcUsername,
                sparkPMJdbcPassword);

        logger.info("FM JDBC Credentials: Driver={}, URL={}, User={}, Password={}",
                sparkFMJdbcUrl,
                sparkFMJdbcDriver,
                sparkFMJdbcUsername,
                sparkFMJdbcPassword);

        logger.info("Cassandra Credentials: Keyspace={}, Host={}, Port={}, Datacenter={}, Username={}, Password={}",
                sparkCassandraKeyspacePM,
                sparkCassandraHost,
                sparkCassandraPort,
                sparkCassandraDatacenter,
                sparkCassandraUsername,
                sparkCassandraPassword);

        this.dataFrame.createOrReplaceTempView("CQLResult");

        String aggregationLevel = jobContextMap.get("aggregationLevel");
        String timestamp = jobContextMap.get("TIMESTAMP");

        logger.info("Processing Timestamp: {} | Aggregation Level: {}", timestamp, aggregationLevel);

        String CURRENT_COUNT = jobContext.getParameter("CURRENT_COUNT");
        logger.info("[ProcessCloseCQLResult] CURRENT_COUNT={}", CURRENT_COUNT);

        String inputConfig = null;
        String extractedParameters = null;
        String nodeAndAggregationDetails = null;
        String kpiCodeNameMapJson = null;

        inputConfig = jobContext.getParameter("INPUT_CONFIGURATIONS" + CURRENT_COUNT);
        nodeAndAggregationDetails = jobContext.getParameter("NODE_AND_AGGREGATION_DETAILS" + CURRENT_COUNT);
        extractedParameters = jobContext.getParameter("EXTRACTED_PARAMETERS" + CURRENT_COUNT);
        kpiCodeNameMapJson = jobContext.getParameter("KPI_CODE_NAME_MAP" + CURRENT_COUNT);

        Map<String, String> inputConfigMap = new ObjectMapper().readValue(inputConfig,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(nodeAndAggregationDetails,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extractedParameters,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> kpiCodeNameMap = new ObjectMapper().readValue(kpiCodeNameMapJson,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> finalMap = new HashMap<>();

        finalMap.putAll(inputConfigMap);
        finalMap.putAll(nodeAndAggregationDetailsMap);
        finalMap.putAll(extraParametersMap);
        extraParametersMap.putAll(kpiCodeNameMap);
        extraParametersMap.putAll(inputConfigMap);

        logger.info("[ProcessCloseCQLResult] Input Config Map: {}", inputConfigMap);
        logger.info("[ProcessCloseCQLResult] Node And Aggregation Details Map: {}", nodeAndAggregationDetailsMap);
        logger.info("[ProcessCloseCQLResult] Extra Parameters Map: {}", extraParametersMap);
        logger.info("[ProcessCloseCQLResult] KPI Code Name Map: {}", extraParametersMap);

        String cqlQuery = buildCQLQuery(finalMap, aggregationLevel, jobContext);
        logger.info("CQL Query On InputDF : {}", cqlQuery);

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
            logger.info("Processing CQL {} With Nodename: {}", rowNumber, row.getAs("nodename"));
            processEachCQLRow(row, extraParametersMap, jobContext, inputConfigMap);
            logger.info("Processing CQL {} With Nodename: {} Completed!", rowNumber++, row.getAs("nodename"));
        }

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.info("[ProcessCloseCQLResult] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes,
                seconds);
        return this.dataFrame;
    }

    private JobContext setSparkConf(JobContext jobContext) {
        jobContext.sqlctx().setConf("spark.sql.caseSensitive", "true");
        jobContext.sqlctx().setConf("spark.cassandra.connection.localDC", sparkCassandraDatacenter);
        jobContext.sqlctx().setConf("spark.cassandra.connection.host", sparkCassandraHost);
        jobContext.sqlctx().setConf("spark.cassandra.connection.port", sparkCassandraPort);
        jobContext.sqlctx().setConf("spark.cassandra.auth.username", sparkCassandraUsername);
        jobContext.sqlctx().setConf("spark.cassandra.auth.password", sparkCassandraPassword);
        jobContext.sqlctx().setConf("spark.sql.catalog.ybcatalog",
                "com.datastax.spark.connector.datasource.CassandraCatalog");
        jobContext.sqlctx().setConf("spark.cassandra.output.ignoreNulls", "true");
        jobContext.sqlctx().setConf("spark.cassandra.input.consistency.level", "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.output.consistency.level", "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.query.retry.count", "10");
        jobContext.sqlctx().setConf("spark.cassandra.output.batch.size.rows", "500");
        jobContext.sqlctx().setConf("spark.cassandra.output.concurrent.writes", "3");
        jobContext.sqlctx().setConf("spark.cassandra.connection.remoteConnectionsPerExecutor", "5");
        jobContext.sqlctx().setConf("spark.jdbc.url", sparkPMJdbcUrl);
        jobContext.sqlctx().setConf("spark.jdbc.user", sparkPMJdbcUsername);
        jobContext.sqlctx().setConf("spark.jdbc.password", sparkPMJdbcPassword);
        return jobContext;
    }

    private static void processEachCQLRow(Row row, Map<String, String> extractedParameters, JobContext jobContext,
            Map<String, String> inputMap) {

        boolean isNodeLevel = Boolean.parseBoolean(extractedParameters.get("isNodeLevel"));
        logger.info("Is Node Level : {}", isNodeLevel);

        if (isNodeLevel) {

            Map<String, Map<String, String>> resultMap = getMapForNodeLevel(row, extractedParameters);
            logger.info("Result Map For Node Level: {}", resultMap);

            processEachResultMap(resultMap, extractedParameters, jobContext, inputMap);
        } else {

            Map<String, Map<String, String>> resultMap = getMapForNonNodeLevel(row, extractedParameters);
            logger.info("Result Map For Non Node Level: {}", resultMap);

            processEachResultMap(resultMap, extractedParameters, jobContext, inputMap);
        }
    }

    private static void processEachResultMap(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {

        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");

        logger.info("KPI Value Map: {}", kpiValueMap);

        String expression = extractedParameters.get("EXPRESSION");

        if (expression == null || expression.isEmpty()) {
            logger.info("Expression is NULL or Empty. Skipping Expression Evaluation.");
            return;
        }

        logger.info("Original Expression : {}", expression);
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

        logger.info("Optimized Expression : {}", optimizedExpression.toString());

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

        logger.info("Evaluated Expression : {} And Result : {}", expression, result);

        if ("1".equalsIgnoreCase(result)) {

            logger.info("Expression Evaluated to TRUE (1). Proceeding with Positive Condition Logic.");
            startAlertGenerationProcess(resultMap, extractedParameters, jobContext, inputMap);

        } else {

            logger.info("Expression Evaluated to FALSE (0). Skipping Alert Generation Process.");
        }
    }

    private static void startAlertGenerationProcess(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {

        String outOfLast = extractedParameters.get("outOfLast");

        if (outOfLast != null && !outOfLast.isEmpty() && !outOfLast.equals("0")) {

            logger.info(
                    "Out of Last is Provided. Proceeding with Alert Generation Process With Consistency Check.");

            proceedWithConsitencyCheck(resultMap, extractedParameters, jobContext, inputMap);

        } else {

            logger.info(
                    "Out of Last is Not Provided. Proceeding with Alert Generation Process With No Consistency Check.");

            produceMessageToKafkaTopic(resultMap, extractedParameters);
        }
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

    private static Map<String, String> produceMessageToKafkaTopic(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters) {

        String entityId = null;
        String entityName = null;
        String subentity = null;
        String geoL1Name = null;
        String geoL2Name = null;
        String geoL3Name = null;
        String geoL4Name = null;
        String entityType = null;
        String entityStatus = null;
        Double latitude = null;
        Double longitude = null;

        Map<String, String> nodeDetailsMap = resultMap.get("nodeDetailsMap");
        boolean isNodeLevel = Boolean.parseBoolean(extractedParameters.get("isNodeLevel"));

        logger.info("Node Details Map : {}", nodeDetailsMap);
        logger.info("Is Node Level : {}", isNodeLevel);

        entityId = nodeDetailsMap.get("ENTITY_ID");
        entityName = nodeDetailsMap.get("ENTITY_NAME");
        subentity = nodeDetailsMap.get("SUBENTITY");
        geoL1Name = nodeDetailsMap.get("GEOGRAPHY_L1_NAME");
        geoL2Name = nodeDetailsMap.get("GEOGRAPHY_L2_NAME");
        geoL3Name = nodeDetailsMap.get("GEOGRAPHY_L3_NAME");
        geoL4Name = nodeDetailsMap.get("GEOGRAPHY_L4_NAME");
        entityType = nodeDetailsMap.get("ENTITY_TYPE");
        entityStatus = nodeDetailsMap.get("ENTITY_STATUS");

        if (isNodeLevel) {
            Map<String, String> latLongMap = getLatLongUsingEntityId(entityId);
            latitude = safeParseDouble(latLongMap, "LATITUDE");
            longitude = safeParseDouble(latLongMap, "LONGITUDE");

        }

        String alarmExternalId = extractedParameters.get("ALARM_EXTERNAL_ID");
        String alarmCode = extractedParameters.get("ALARM_CODE");
        String alarmName = extractedParameters.get("ALARM_NAME");
        String severity = extractedParameters.get("SEVERITY");
        String description = extractedParameters.get("DESCRIPTION");
        String probableCause = extractedParameters.get("EXPRESSION");
        String actualSeverity = extractedParameters.get("ACTUAL_SEVERITY");
        String domain = extractedParameters.get("DOMAIN");
        String vendor = extractedParameters.get("VENDOR");
        String technology = extractedParameters.get("TECHNOLOGY");
        String senderName = extractedParameters.get("SENDER_NAME");
        String classification = extractedParameters.get("CLASSIFICATION");
        String senderIp = "-";
        String kafkaTopicName = jobContextMap.get("KAFKA_TOPIC_NAME");
        String alarmGroup = extractedParameters.get("ALARM_GROUP");
        boolean serviceAffected = safeParseBoolean(extractedParameters, "SERVICE_AFFECTING");
        boolean manualCloseable = safeParseBoolean(extractedParameters, "MANUALLY_CLOSEABLE");
        boolean correlationFlag = safeParseBoolean(extractedParameters, "CORRELATION_FLAG");

        Map<String, String> kpiValueMap = resultMap.get("kpiValueMap");
        String additionalDetails = getAdditionalDetailsForNormalExp(kpiValueMap, extractedParameters);

        String openTime = extractedParameters.get("TIMESTAMP");

        ProcessCloseCQLResult.AlarmWrapper alarmWrapper = new ProcessCloseCQLResult.AlarmWrapper();
        alarmWrapper.setOpenTime(toEpochMillis(openTime));
        alarmWrapper.setChangeTime(toEpochMillis(openTime));
        alarmWrapper.setReportingTime(toEpochMillis(openTime));
        alarmWrapper.setAlarmExternalId(alarmExternalId);
        alarmWrapper.setAlarmCode(alarmCode);
        alarmWrapper.setAlarmName(alarmName);
        alarmWrapper.setSeverity("cleared");
        alarmWrapper.setActualSeverity(actualSeverity);
        alarmWrapper.setDomain(domain);
        alarmWrapper.setVendor(vendor);
        alarmWrapper.setSenderName(senderName);
        alarmWrapper.setTechnology(technology);
        alarmWrapper.setClassification(classification);
        alarmWrapper.setEventType(entityType);
        alarmWrapper.setProbableCause(probableCause);
        alarmWrapper.setEntityId(entityId);
        alarmWrapper.setEntityName(entityName);
        alarmWrapper.setEntityType(entityType);
        alarmWrapper.setEntityStatus(entityStatus);
        alarmWrapper.setLocationId(entityName);
        alarmWrapper.setSubentity(subentity);
        alarmWrapper.setSenderIp(senderIp);
        alarmWrapper.setLatitude(latitude);
        alarmWrapper.setLongitude(longitude);
        alarmWrapper.setServiceAffected(serviceAffected);
        alarmWrapper.setDescription(description);
        alarmWrapper.setManualCloseable(manualCloseable);
        alarmWrapper.setGeographyL1Name(geoL1Name);
        alarmWrapper.setGeographyL2Name(geoL2Name);
        alarmWrapper.setGeographyL3Name(geoL3Name);
        alarmWrapper.setGeographyL4Name(geoL4Name);
        alarmWrapper.setKafkaTopicName(kafkaTopicName);
        alarmWrapper.setCorrelationFlag(correlationFlag);
        alarmWrapper.setNeCategory(entityType);
        alarmWrapper.setAdditionalDetail(additionalDetails);
        alarmWrapper.setAlarmGroup(alarmGroup);

        String kafkaBroker = jobContextMap.get("SPARK_KAFKA_BROKER_ANSIBLE");
        produceMessage(alarmWrapper, kafkaTopicName, kafkaBroker);
        return new LinkedHashMap<>();
    }

    private static boolean safeParseBoolean(Map<String, String> map, String key) {
        if (map == null || !map.containsKey(key)) {
            return false;
        }
        try {
            String value = map.get(key);
            if (value == null) {
                return false;
            }
            return value.equalsIgnoreCase("true") || value.equals("1") || value.equalsIgnoreCase("yes")
                    || value.equalsIgnoreCase("y");
        } catch (Exception e) {
            return false;
        }
    }

    private static Double safeParseDouble(Map<String, String> map, String key) {
        if (map == null || !map.containsKey(key)) {
            return null;
        }
        try {
            return Double.parseDouble(map.get(key));
        } catch (Exception e) {
            return null;
        }
    }

    public static class AlarmWrapper {

        @SerializedName("open_time")
        private Long openTime;

        public Long getOpenTime() {
            return openTime;
        }

        public void setOpenTime(Long openTime) {
            this.openTime = openTime;
        }

        @SerializedName("change_time")
        private Long changeTime;

        public Long getChangeTime() {
            return changeTime;
        }

        public void setChangeTime(Long changeTime) {
            this.changeTime = changeTime;
        }

        @SerializedName("reporting_time")
        private Long reportingTime;

        public Long getReportingTime() {
            return reportingTime;
        }

        public void setReportingTime(Long reportingTime) {
            this.reportingTime = reportingTime;
        }

        @SerializedName("alarm_external_id")
        private String alarmExternalId;

        public String getAlarmExternalId() {
            return alarmExternalId;
        }

        public void setAlarmExternalId(String alarmExternalId) {
            this.alarmExternalId = alarmExternalId;
        }

        @SerializedName("alarm_code")
        private String alarmCode;

        public String getAlarmCode() {
            return alarmCode;
        }

        public void setAlarmCode(String alarmCode) {
            this.alarmCode = alarmCode;
        }

        @SerializedName("alarm_name")
        private String alarmName;

        public String getAlarmName() {
            return alarmName;
        }

        public void setAlarmName(String alarmName) {
            this.alarmName = alarmName;
        }

        @SerializedName("severity")
        private String severity;

        public String getSeverity() {
            return severity;
        }

        public void setSeverity(String severity) {
            this.severity = severity;
        }

        @SerializedName("actualseverity")
        private String actualSeverity;

        public String getActualSeverity() {
            return actualSeverity;
        }

        public void setActualSeverity(String actualSeverity) {
            this.actualSeverity = actualSeverity;
        }

        @SerializedName("domain")
        private String domain;

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        @SerializedName("vendor")
        private String vendor;

        public String getVendor() {
            return vendor;
        }

        public void setVendor(String vendor) {
            this.vendor = vendor;
        }

        @SerializedName("sender_name")
        private String senderName;

        public String getSenderName() {
            return senderName;
        }

        public void setSenderName(String senderName) {
            this.senderName = senderName;
        }

        @SerializedName("technology")
        private String technology;

        public String getTechnology() {
            return technology;
        }

        public void setTechnology(String technology) {
            this.technology = technology;
        }

        @SerializedName("classification")
        private String classification;

        public String getClassification() {
            return classification;
        }

        public void setClassification(String classification) {
            this.classification = classification;
        }

        @SerializedName("event_type")
        private String eventType;

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        @SerializedName("probable_cause")
        private String probableCause;

        public String getProbableCause() {
            return probableCause;
        }

        public void setProbableCause(String probableCause) {
            this.probableCause = probableCause;
        }

        @SerializedName("entity_id")
        private String entityId;

        public String getEntityId() {
            return entityId;
        }

        public void setEntityId(String entityId) {
            this.entityId = entityId;
        }

        @SerializedName("entity_name")
        private String entityName;

        public String getEntityName() {
            return entityName;
        }

        public void setEntityName(String entityName) {
            this.entityName = entityName;
        }

        @SerializedName("entity_type")
        private String entityType;

        public String getEntityType() {
            return entityType;
        }

        public void setEntityType(String entityType) {
            this.entityType = entityType;
        }

        @SerializedName("entity_status")
        private String entityStatus;

        public String getEntityStatus() {
            return entityStatus;
        }

        public void setEntityStatus(String entityStatus) {
            this.entityStatus = entityStatus;
        }

        @SerializedName("location_id")
        private String locationId;

        public String getLocationId() {
            return locationId;
        }

        public void setLocationId(String locationId) {
            this.locationId = locationId;
        }

        @SerializedName("subentity")
        private String subentity;

        public String getSubentity() {
            return subentity;
        }

        public void setSubentity(String subentity) {
            this.subentity = subentity;
        }

        @SerializedName("sender_ip")
        private String senderIp;

        public String getSenderIp() {
            return senderIp;
        }

        public void setSenderIp(String senderIp) {
            this.senderIp = senderIp;
        }

        @SerializedName("latitude")
        private Double latitude;

        public Double getLatitude() {
            return latitude;
        }

        public void setLatitude(Double latitude) {
            this.latitude = latitude;
        }

        @SerializedName("longitude")
        private Double longitude;

        public Double getLongitude() {
            return longitude;
        }

        public void setLongitude(Double longitude) {
            this.longitude = longitude;
        }

        @SerializedName("service_affected")
        private Boolean serviceAffected;

        public Boolean getServiceAffected() {
            return serviceAffected;
        }

        public void setServiceAffected(Boolean serviceAffected) {
            this.serviceAffected = serviceAffected;
        }

        @SerializedName("description")
        private String description;

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        @SerializedName("manually_closeable")
        private Boolean manualCloseable;

        public Boolean getManualCloseable() {
            return manualCloseable;
        }

        public void setManualCloseable(Boolean manualCloseable) {
            this.manualCloseable = manualCloseable;
        }

        @SerializedName("geography_l1_name")
        private String geographyL1Name;

        public String getGeographyL1Name() {
            return geographyL1Name;
        }

        public void setGeographyL1Name(String geographyL1Name) {
            this.geographyL1Name = geographyL1Name;
        }

        @SerializedName("geography_l2_name")
        private String geographyL2Name;

        public String getGeographyL2Name() {
            return geographyL2Name;
        }

        public void setGeographyL2Name(String geographyL2Name) {
            this.geographyL2Name = geographyL2Name;
        }

        @SerializedName("geography_l3_name")
        private String geographyL3Name;

        public String getGeographyL3Name() {
            return geographyL3Name;
        }

        public void setGeographyL3Name(String geographyL3Name) {
            this.geographyL3Name = geographyL3Name;
        }

        @SerializedName("geography_l4_name")
        private String geographyL4Name;

        public String getGeographyL4Name() {
            return geographyL4Name;
        }

        public void setGeographyL4Name(String geographyL4Name) {
            this.geographyL4Name = geographyL4Name;
        }

        @SerializedName("kafka_topic_name")
        private String kafkaTopicName;

        public String getKafkaTopicName() {
            return kafkaTopicName;
        }

        public void setKafkaTopicName(String kafkaTopicName) {
            this.kafkaTopicName = kafkaTopicName;
        }

        @SerializedName("correlation_flag")
        private Boolean correlationFlag;

        public Boolean getCorrelationFlag() {
            return correlationFlag;
        }

        public void setCorrelationFlag(Boolean correlationFlag) {
            this.correlationFlag = correlationFlag;
        }

        @SerializedName("neCategory")
        private String neCategory;

        public String getNeCategory() {
            return neCategory;
        }

        public void setNeCategory(String neCategory) {
            this.neCategory = neCategory;
        }

        @SerializedName("additional_detail")
        private String additionalDetail;

        public String getAdditionalDetail() {
            return additionalDetail;
        }

        public void setAdditionalDetail(String additionalDetail) {
            this.additionalDetail = additionalDetail;
        }

        @SerializedName("alarm_group")
        private String alarmGroup;

        public String getAlarmGroup() {
            return alarmGroup;
        }

        public void setAlarmGroup(String alarmGroup) {
            this.alarmGroup = alarmGroup;
        }
    }

    public static void produceMessage(AlarmWrapper wrapper, String kafkaTopicName, String kafkaBroker) {

        if (wrapper == null) {
            return;
        }

        final String KAFKA_TOPIC_NAME = kafkaTopicName;
        final String KAFKA_BOOTSTRAP_SERVERS = kafkaBroker;

        logger.info("Kafka Broker={}, Kafka Topic Name={}", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NAME);
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("retries", 3);
        props.put("request.timeout.ms", 15000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            String json = gson.toJson(wrapper);

            logger.info("Producing Message={} To Kakfa Topic={}", json, KAFKA_TOPIC_NAME);

            ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC_NAME, json);
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception == null) {

                    logger.info("Sent Message To Topic={} Partition={} Offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    logger.error("Error Producing Message: ", exception.getMessage());
                    exception.printStackTrace();

                }
            });
            future.get();
        } catch (Exception e) {
            logger.error("Failed to produce Kafka message", e);
        }
    }

    public static long toEpochMillis(String timestamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxx");
            OffsetDateTime odt = OffsetDateTime.parse(timestamp, formatter)
                    .withOffsetSameInstant(ZoneOffset.UTC);
            return odt.toInstant().toEpochMilli();
        } catch (Exception e) {
            return Instant.now().toEpochMilli();
        }
    }

    private static String getAdditionalDetailsForNormalExp(Map<String, String> kpiValueMap,
            Map<String, String> extractedParameters) {

        List<String> details = new ArrayList<>();
        for (Map.Entry<String, String> entry : kpiValueMap.entrySet()) {
            String kpiCode = entry.getKey();
            String value = entry.getValue();
            String label = extractedParameters.get(kpiCode);
            if (label != null) {
                details.add(label + "=" + value);
            } else {
                details.add(kpiCode + "=" + value);
            }
        }
        if (details.isEmpty()) {
            return "";
        }
        return String.join(", ", details);
    }

    private static Map<String, String> proceedWithConsitencyCheck(Map<String, Map<String, String>> resultMap,
            Map<String, String> extractedParameters, JobContext jobContext, Map<String, String> inputMap) {

        boolean isConsistencyMet = isConsistencyMet(extractedParameters, resultMap, jobContext, inputMap);
        if (isConsistencyMet) {
            return produceMessageToKafkaTopic(resultMap, extractedParameters);
        } else {
            logger.info("Consistency Not Met, Skipping Alert Generation Process!");
        }
        return new LinkedHashMap<>();
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

        int thresholdBreachCount = 1;
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
        String cleanedQuery = originalQuery.trim().replaceAll("\\s+", " ");
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

        String expr = optimizedExpression.toString();
        boolean result = isThresholdBreach(expr);
        logger.info("Optimized Expression={} | Evaluation Result={}", expr, result);
        return result;
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

        logger.info("CQL Query For Consistency: {}", cqlQuery.toString());

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
        Map<String, String> nodeDetailsMap = new HashMap<>();

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

        String neid = row.getAs("ENTITY_ID") != null ? (String) row.getAs("ENTITY_ID") : "";
        String nename = row.getAs("ENTITY_NAME") != null ? (String) row.getAs("ENTITY_NAME") : "";
        String subentity = row.getAs("SUBENTITY") != null ? (String) row.getAs("SUBENTITY") : "";
        String geoL1 = row.getAs("L1") != null ? (String) row.getAs("L1") : "";
        String geoL2 = row.getAs("L2") != null ? (String) row.getAs("L2") : "";
        String geoL3 = row.getAs("L3") != null ? (String) row.getAs("L3") : "";
        String geoL4 = row.getAs("L4") != null ? (String) row.getAs("L4") : "";
        String entityType = row.getAs("ENTITY_TYPE") != null ? (String) row.getAs("ENTITY_TYPE") : "";
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

        logger.info("KPI Codes: {}", Arrays.toString(kpiCodes));

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

        logger.info("MySQL Query For Data Level Appender: {}", mysqlQuery.toString());

        Dataset<Row> df = executeQuery(mysqlQuery.toString(), jobContext);

        List<String> dataLevelAppenders = df.as(Encoders.STRING()).collectAsList();
        String dataLevelAppender = dataLevelAppenders.isEmpty() ? "" : dataLevelAppenders.get(0);
        logger.info("Data Level Appender: {}", dataLevelAppender);

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

        logger.info("Generated Data Level: {}", datalevel);

        if (datalevel.contains("L0")) {
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L0' AS SUBENTITY, '-' AS L1, '-' AS L2, '-' AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");
        } else if (datalevel.contains("L1")) {
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L1' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN UPPER(nodename) ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, '-' AS L2, '-' AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else if (datalevel.contains("L2")) {
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L2' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END AS L2, '-' AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else if (datalevel.contains("L3")) {
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L3' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END AS L2, CASE WHEN TRIM(metajson['L3']) IS NULL OR TRIM(metajson['L3']) = '' OR TRIM(metajson['L3']) = '-' OR LOWER(TRIM(metajson['L3'])) = 'null' THEN CASE WHEN TRIM(metajson['DL3']) IS NULL OR TRIM(metajson['DL3']) = '' OR TRIM(metajson['DL3']) = '-' OR LOWER(TRIM(metajson['DL3'])) = 'null' THEN '-' ELSE metajson['DL3'] END ELSE metajson['L3'] END AS L3, '-' AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else if (datalevel.contains("L4")) {
            cqlQuery.append(
                    ", UPPER(nodename) AS ENTITY_ID, UPPER(nodename) AS ENTITY_NAME, 'L4' AS SUBENTITY, CASE WHEN TRIM(metajson['L1']) IS NULL OR TRIM(metajson['L1']) = '' OR TRIM(metajson['L1']) = '-' OR LOWER(TRIM(metajson['L1'])) = 'null' THEN CASE WHEN TRIM(metajson['DL1']) IS NULL OR TRIM(metajson['DL1']) = '' OR TRIM(metajson['DL1']) = '-' OR LOWER(TRIM(metajson['DL1'])) = 'null' THEN '-' ELSE metajson['DL1'] END ELSE metajson['L1'] END AS L1, CASE WHEN TRIM(metajson['L2']) IS NULL OR TRIM(metajson['L2']) = '' OR TRIM(metajson['L2']) = '-' OR LOWER(TRIM(metajson['L2'])) = 'null' THEN CASE WHEN TRIM(metajson['DL2']) IS NULL OR TRIM(metajson['DL2']) = '' OR TRIM(metajson['DL2']) = '-' OR LOWER(TRIM(metajson['DL2'])) = 'null' THEN '-' ELSE metajson['DL2'] END ELSE metajson['L2'] END AS L2, CASE WHEN TRIM(metajson['L3']) IS NULL OR TRIM(metajson['L3']) = '' OR TRIM(metajson['L3']) = '-' OR LOWER(TRIM(metajson['L3'])) = 'null' THEN CASE WHEN TRIM(metajson['DL3']) IS NULL OR TRIM(metajson['DL3']) = '' OR TRIM(metajson['DL3']) = '-' OR LOWER(TRIM(metajson['DL3'])) = 'null' THEN '-' ELSE metajson['DL3'] END ELSE metajson['L3'] END AS L3, CASE WHEN TRIM(metajson['L4']) IS NULL OR TRIM(metajson['L4']) = '' OR TRIM(metajson['L4']) = '-' OR LOWER(TRIM(metajson['L4'])) = 'null' THEN CASE WHEN TRIM(metajson['DL4']) IS NULL OR TRIM(metajson['DL4']) = '' OR TRIM(metajson['DL4']) = '-' OR LOWER(TRIM(metajson['DL4'])) = 'null' THEN '-' ELSE metajson['DL4'] END ELSE metajson['L4'] END AS L4, UPPER(networktype) AS ENTITY_TYPE");

        } else {
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
                    .option("driver", sparkPMJdbcDriver)
                    .option("url", sparkPMJdbcUrl)
                    .option("user", sparkPMJdbcUsername)
                    .option("password", sparkPMJdbcPassword)
                    .option("query", sqlQuery)
                    .load();

            return resultDataset;

        } catch (Exception e) {
            logger.error("Exception in Executing Query, Message: " + e.getMessage() + " | Error: " + e);
            return resultDataset;
        }

    }

    public static String reduceFrequencyFromTimestamp(String timestamp, JobContext jobContext) {

        String jobFrequency = jobContext.getParameter("FREQUENCY");

        long reduceMinutes = 0L;

        switch (jobFrequency.toUpperCase()) {

            case "5 MIN":
            case "FIVE_MIN":
            case "FIVEMIN":
                reduceMinutes = 5;
                break;

            case "15 MIN":
            case "QUARTERLY":
                reduceMinutes = 15;
                break;

            case "HOUR":
            case "PERHOUR":
            case "HOURLY":
                reduceMinutes = 60;
                break;

            case "DAY":
            case "PERDAY":
            case "DAILY":
                reduceMinutes = 1440;
                break;

            case "WEEK":
            case "PERWEEK":
            case "WEEKLY":
                reduceMinutes = 10080;
                break;

            case "MONTH":
            case "PERMONTH":
            case "MONTHLY":
                reduceMinutes = 43200;
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

                Class.forName(sparkFMJdbcDriver);
                connection = DriverManager.getConnection(sparkFMJdbcUrl, sparkFMJdbcUsername,
                        sparkFMJdbcPassword);

            } else if (dbName.contains("PERFORMANCE")) {

                Class.forName(sparkPMJdbcDriver);
                connection = DriverManager.getConnection(sparkPMJdbcUrl, sparkPMJdbcUsername,
                        sparkPMJdbcPassword);

            }
            return connection;

        } catch (ClassNotFoundException e) {

            logger.error("MySQL JDBC Driver Not Found @getDatabaseConnection | Message: {}, Error: {}",
                    e.getMessage(),
                    e);

        } catch (SQLException e) {

            logger.error("Database Connection Error @getDatabaseConnection | Message: {}, Error: {}", e.getMessage(),
                    e);

        } catch (Exception e) {

            logger.error("Unexpected Exception @getDatabaseConnection | Message: {}, Error: {}", e.getMessage(), e);
        }
        return connection;
    }

}
