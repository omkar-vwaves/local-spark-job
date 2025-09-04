package com.enttribe.pm.job.alert;

import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.context.JobContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProcessBreachedDF extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessBreachedDF.class);

    // Constants or Default Values
    private static final String DEFAULT_GEOGRAPHY_VALUE = "-";
    private static final String DEFAULT_TIMESTAMP = "2025-01-16 02:30:00.000";
    private static final String DEFAULT_SENDER_IP = "-";
    private static final String DEFAULT_ADDITIONAL_DETAIL = "";
    private static final String DEFAULT_OCCURRENCE = "1";
    private static final String ALARM_STATUS_OPEN = "OPEN";

    // Database and SQL Constants
    private static final int SQL_PARAMETER_INDEX_ENTITY_ID = 1;
    private static final int SAMPLE_LOG_ROW_INDEX = 1;

    // Job Context Parameter Names
    private static final String PARAM_CURRENT_INDEX = "START_INDEX";
    private static final String PARAM_CONFIGURATION_MAP = "CONFIGURATION_MAP";
    private static final String PARAM_NODE_AND_AGGREGATION_DETAILS_MAP = "NODE_AND_AGGREGATION_DETAILS_MAP";
    private static final String PARAM_EXTRACTED_PARAMETERS_MAP = "EXTRACTED_PARAMETERS_MAP";
    private static final String PARAM_TIMESTAMP = "TIMESTAMP";
    private static final String PARAM_DOMAIN = "DOMAIN";
    private static final String PARAM_VENDOR = "VENDOR";

    public ProcessBreachedDF() {
        super();
        logger.info("Process Breached DF Constructor Executed Successfully!");
    }

    public ProcessBreachedDF(Dataset<Row> dataFrame, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataFrame;
        logger.info("Process Breached DF Constructor Executed Successfully!");
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("Process Breached DF Execute And Get Result Dataframe Execution Started!");

        Dataset<Row> breachedDF = this.dataFrame;
        List<Row> breachedRows = breachedDF.collectAsList();

        logger.info("Found {} Breached Rows to Process", breachedRows.size());

        if (breachedRows.isEmpty()) {
            logger.info("No Breached Rows Found!");
            return this.dataFrame;
        }

        String currentIndex = jobContext.getParameter(PARAM_CURRENT_INDEX);
        String configurationMapJson = jobContext.getParameter(PARAM_CONFIGURATION_MAP + currentIndex);
        String nodeAndAggregationDetailsMapJson = jobContext
                .getParameter(PARAM_NODE_AND_AGGREGATION_DETAILS_MAP + currentIndex);
        String extractedParametersMapJson = jobContext.getParameter(PARAM_EXTRACTED_PARAMETERS_MAP + currentIndex);
        String filterLevel = jobContext.getParameter("FILTER_LEVEL" + currentIndex);

        Map<String, String> configurationMap = new ObjectMapper().readValue(configurationMapJson,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(
                nodeAndAggregationDetailsMapJson,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> extractedParametersMap = new ObjectMapper().readValue(extractedParametersMapJson,
                new TypeReference<Map<String, String>>() {
                });

        if (configurationMap == null || configurationMap.isEmpty()) {
            logger.error("Configuration Map is Null or Empty");
            throw new RuntimeException("Configuration Map is Null or Empty");
        }

        if (extractedParametersMap == null || extractedParametersMap.isEmpty()) {
            logger.error("Extracted Parameters Map is Null or Empty");
            throw new RuntimeException("Extracted Parameters Map is Null or Empty");
        }

        Map<String, Map<String, String>> latLongCache = new HashMap<>();

        List<Row> alertRows = new ArrayList<>();

        for (Row row : breachedRows) {

            Map<String, String> metaDataMap = row.getJavaMap(row.fieldIndex("META_DATA_MAP"));
            Map<String, String> kpiCounterMap = row.getJavaMap(row.fieldIndex("KPI_COUNTER_MAP"));

            if (metaDataMap == null) {
                logger.error("Skipping Row with Null META_DATA_MAP");
                continue;
            }

            String entityName = metaDataMap.get("PARENT_ENTITY_NAME");
            String entityId = metaDataMap.get("PARENT_ENTITY_ID");
            String entityType = metaDataMap.get("PARENT_ENTITY_TYPE");
            String entityStatus = metaDataMap.get("ENTITY_STATUS");
            String subEntity = metaDataMap.get("ENTITY_ID");

            String geoL1 = nodeAndAggregationDetailsMap.get("geoL1");
            if (geoL1.equalsIgnoreCase("Custom") && filterLevel.equalsIgnoreCase("L0")) {
                entityId = "Custom";
                entityName = "Custom";
                subEntity = "Custom";

                logger.info("Entity ID: {}, Entity Name: {}, SubEntity: {} Set to Custom", entityId, entityName,
                        subEntity);
            }
            if (filterLevel.equalsIgnoreCase("H1")) {
                subEntity = metaDataMap.get("PARENT_ENTITY_TYPE");
            }

            if (entityName == null || entityId == null || subEntity == null) {
                logger.error(
                        "Skipping Row with Missing Required Fields - Entity Name: {}, Entity ID: {}, SubEntity: {}",
                        entityName, entityId, subEntity);
                continue;
            }

            String geographyL1Name = metaDataMap.get("L1") != null ? metaDataMap.get("L1").toUpperCase()
                    : DEFAULT_GEOGRAPHY_VALUE;
            String geographyL2Name = metaDataMap.get("L2") != null ? metaDataMap.get("L2").toUpperCase()
                    : DEFAULT_GEOGRAPHY_VALUE;
            String geographyL3Name = metaDataMap.get("L3") != null ? metaDataMap.get("L3").toUpperCase()
                    : DEFAULT_GEOGRAPHY_VALUE;
            String geographyL4Name = metaDataMap.get("L4") != null ? metaDataMap.get("L4").toUpperCase()
                    : DEFAULT_GEOGRAPHY_VALUE;

            Map<String, String> alertMap = getAlertMap(configurationMap, nodeAndAggregationDetailsMap,
                    extractedParametersMap, jobContext, kpiCounterMap);

            alertMap.put("ENTITY_NAME", entityName);
            alertMap.put("ENTITY_ID", entityId);
            alertMap.put("ENTITY_TYPE", entityType);
            alertMap.put("SUBENTITY", subEntity);
            alertMap.put("ENTITY_STATUS", entityStatus);
            alertMap.put("GEOGRAPHY_L1_NAME", geographyL1Name);
            alertMap.put("GEOGRAPHY_L2_NAME", geographyL2Name);
            alertMap.put("GEOGRAPHY_L3_NAME", geographyL3Name);
            alertMap.put("GEOGRAPHY_L4_NAME", geographyL4Name);

            Map<String, String> latLongMap;
            if (latLongCache.containsKey(entityId)) {
                latLongMap = latLongCache.get(entityId);
            } else {
                latLongMap = getLatLongUsingEntityId(entityId, jobContext);
                latLongCache.put(entityId, latLongMap);
            }
            alertMap.put("LATITUDE", latLongMap.get("LATITUDE"));
            alertMap.put("LONGITUDE", latLongMap.get("LONGITUDE"));

            Row alertRow = RowFactory.create(alertMap);
            alertRows.add(alertRow);

            if (alertRows.size() == SAMPLE_LOG_ROW_INDEX) {
                logger.info(
                        "Sample Alert Data - ALARM_EXTERNAL_ID: {}, ALARM_CODE: {}, ENTITY_NAME: {}, ENTITY_ID: {}, SUBENTITY: {}",
                        alertMap.get("ALARM_EXTERNAL_ID"), alertMap.get("ALARM_CODE"),
                        alertMap.get("ENTITY_NAME"), alertMap.get("ENTITY_ID"), alertMap.get("SUBENTITY"));
            }
        }

        Dataset<Row> alertDataset = buildAlertDataset(jobContext, alertRows);

        logger.info("Process Breached DF Execute And Get Result Dataframe Executed Successfully!");
        logger.info("Processed {} Breached Rows into {} Alert Rows", breachedRows.size(), alertRows.size());
        return alertDataset;
    }

    public static Dataset<Row> buildAlertDataset(JobContext jobContext, List<Row> rows) {
        StructType schema = new StructType()
                .add("ALERT_MAP", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        return jobContext.createDataFrame(rows, schema);
    }

    private static Map<String, String> getAlertMap(Map<String, String> configurationMap,
            Map<String, String> nodeAndAggregationDetailsMap, Map<String, String> extractedParametersMap,
            JobContext jobContext, Map<String, String> kpiCounterMap) throws Exception {

        Map<String, String> alertMap = new LinkedHashMap<>();

        String timestamp = jobContext.getParameter(PARAM_TIMESTAMP);
        String openTime = convertTimeStampToExpectedFormat(timestamp);
        String processingTime = generateProcessingTime();

        alertMap.put("OPEN_TIME", openTime);
        alertMap.put("CHANGE_TIME", openTime);
        alertMap.put("REPORTING_TIME", openTime);
        alertMap.put("FIRST_RECEPTION_TIME", openTime);
        alertMap.put("FIRST_PROCESSING_TIME", processingTime);
        alertMap.put("LAST_PROCESSING_TIME", processingTime);
        alertMap.put("ALARM_EXTERNAL_ID", configurationMap.get("ALARM_IDENTIFIER"));
        alertMap.put("ALARM_CODE", configurationMap.get("ALARM_ID"));
        alertMap.put("ALARM_NAME", configurationMap.get("ALARM_NAME"));
        alertMap.put("CLASSIFICATION", configurationMap.get("CLASSIFICATION"));
        alertMap.put("EVENT_TYPE", configurationMap.get("EVENT_TYPE"));
        alertMap.put("SEVERITY", extractedParametersMap.get("SEVERITY"));
        alertMap.put("ACTUAL_SEVERITY", configurationMap.get("DEFAULT_SEVERITY"));
        alertMap.put("ALARM_STATUS", ALARM_STATUS_OPEN);
        alertMap.put("SENDER_NAME", configurationMap.get("EMS_TYPE"));
        alertMap.put("SENDER_IP", DEFAULT_SENDER_IP);
        alertMap.put("DOMAIN", jobContext.getParameter(PARAM_DOMAIN));
        alertMap.put("VENDOR", jobContext.getParameter(PARAM_VENDOR));
        alertMap.put("TECHNOLOGY", configurationMap.get("TECHNOLOGY"));
        alertMap.put("PROBABLE_CAUSE", configurationMap.get("EXPRESSION"));
        alertMap.put("DESCRIPTION", configurationMap.get("DESCRIPTION"));
        alertMap.put("SERVICE_AFFECTING", extractedParametersMap.get("SERVICE_AFFECTING"));
        alertMap.put("MANUALLY_CLOSEABLE", extractedParametersMap.get("MANUALLY_CLOSEABLE"));
        alertMap.put("OCCURRENCE", DEFAULT_OCCURRENCE);
        alertMap.put("CORRELATION_FLAG", extractedParametersMap.get("CORRELATION_FLAG"));
        alertMap.put("ALARM_GROUP", configurationMap.get("ALARM_GROUP"));

        String alarmExternalId = alertMap.get("ALARM_EXTERNAL_ID");
        String alarmCode = alertMap.get("ALARM_CODE");
        String domain = alertMap.get("DOMAIN");
        String vendor = alertMap.get("VENDOR");
        String technology = alertMap.get("TECHNOLOGY");

        if (alarmExternalId == null || alarmCode == null || domain == null || vendor == null || technology == null) {
            logger.error(
                    "Missing Critical Alarm Fields - ALARM_EXTERNAL_ID: {}, ALARM_CODE: {}, DOMAIN: {}, VENDOR: {}, TECHNOLOGY: {}",
                    alarmExternalId, alarmCode, domain, vendor, technology);
            throw new RuntimeException("Missing Critical Alarm Fields");
        }

        String geoL1 = nodeAndAggregationDetailsMap.get("geoL1");
        if (geoL1.equalsIgnoreCase("Custom")) {

            String cellList = nodeAndAggregationDetailsMap.get("cellsList");
            if (cellList != null && !cellList.isEmpty()) {
                cellList = cellList.replace("[", "").replace("]", "");
                // alertMap.put("ADDITIONAL_DETAIL", cellList);
            }

        }

        String expression = configurationMap.get("EXPRESSION");
        String additionalDetail = getAdditionalDetail(kpiCounterMap, expression, jobContext);
        logger.info("Additional Detail: {}", additionalDetail);
        alertMap.put("ADDITIONAL_DETAIL", additionalDetail);

        return alertMap;
    }

    private static String getAdditionalDetail(Map<String, String> kpiCounterMap, String expression,
            JobContext jobContext) throws Exception {
        // TODO: Implement the logic to get the additional detail
        // ((IF((((COUNTER#537872))<= 0) || (((COUNTER#537874))>= 0),1,0)))
        // ((IF((((KPI#537872))<= 0) || (((KPI#537874))>= 0),1,0)))

        // Extract both COUNTER# and KPI# ids from expression
        Set<String> idSet = new HashSet<>();
        Matcher matcher = Pattern.compile("(?:KPI|COUNTER)#(\\d+)").matcher(expression);
        while (matcher.find()) {
            idSet.add(matcher.group(1));
        }

        logger.info("Matched IDs: {}", idSet);

        String startIndex = jobContext.getParameter("START_INDEX");
        String configurationMapJson = jobContext.getParameter("CONFIGURATION_MAP" + startIndex);

        Map<String, String> configurationMap = new ObjectMapper().readValue(configurationMapJson,
                new TypeReference<Map<String, String>>() {
                });

        String configuration = configurationMap.get("CONFIGURATION");
        String fixedJson = configuration == null ? "{}" : configuration.replace("'", "\"").replaceAll("^\"|\"$", "");

        JSONObject configJson = new JSONObject(fixedJson);

        Map<String, String> idToNameMap = new HashMap<>();

        if (configJson.has("kpi")) {
            JSONArray kpiArray = configJson.getJSONArray("kpi");
            if (kpiArray != null) {
                for (int i = 0; i < kpiArray.length(); i++) {
                    JSONObject kpiObj = kpiArray.getJSONObject(i);
                    String kpiName = kpiObj.optString("kpiName");
                    String kpiId = kpiName.split("-")[0]; // e.g., "537872-inbound_traffic" → "537872"
                    idToNameMap.put(kpiId, kpiName);
                    logger.info("Mapped {} to {}", kpiId, kpiName);
                }
            }
        }

        // Build the result
        List<String> resultParts = new ArrayList<>();
        for (String id : idSet) {
            String readableName = idToNameMap.getOrDefault(id, id);
            String value = kpiCounterMap.getOrDefault(id, "-");
            resultParts.add(readableName + "=" + value);
        }

        return String.join(", ", resultParts);
    }

    private static Map<String, String> getLatLongUsingEntityId(String entityId, JobContext jobContext) {

        Map<String, String> latLongMap = new LinkedHashMap<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            String sqlQuery = "SELECT LATITUDE, LONGITUDE FROM NETWORK_ELEMENT WHERE NE_ID = ?";
            connection = getDatabaseConnection("PERFORMANCE", jobContext);

            if (connection != null) {
                preparedStatement = connection.prepareStatement(sqlQuery);
                preparedStatement.setString(SQL_PARAMETER_INDEX_ENTITY_ID, entityId);
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

        return latLongMap;
    }

    private static Connection getDatabaseConnection(String dbName, JobContext jobContext) {

        Connection connection = null;

        try {

            if (dbName.contains("FMS")) {

                String SPARK_FM_JDBC_DRIVER = jobContext.getParameter("SPARK_FM_JDBC_DRIVER");
                String SPARK_FM_JDBC_URL = jobContext.getParameter("SPARK_FM_JDBC_URL");
                String SPARK_FM_JDBC_USERNAME = jobContext.getParameter("SPARK_FM_JDBC_USERNAME");
                String SPARK_FM_JDBC_PASSWORD = jobContext.getParameter("SPARK_FM_JDBC_PASSWORD");

                Class.forName(SPARK_FM_JDBC_DRIVER);
                connection = DriverManager.getConnection(SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME,
                        SPARK_FM_JDBC_PASSWORD);

            } else if (dbName.contains("PERFORMANCE")) {

                String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

                Class.forName(SPARK_PM_JDBC_DRIVER);
                connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                        SPARK_PM_JDBC_PASSWORD);

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

    private static String generateProcessingTime() {
        String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
        return formattedTimestamp;
    }

    private static String convertTimeStampToExpectedFormat(String timestamp) {

        if (timestamp == null || timestamp.isEmpty()) {
            String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            return formattedTimestamp;
        }

        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
            OffsetDateTime dateTime = OffsetDateTime.parse(timestamp, inputFormatter);

            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

            String formattedTimestamp = dateTime.format(outputFormatter);
            return formattedTimestamp;

        } catch (Exception e) {
            String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            return formattedTimestamp;
        }
    }
}
