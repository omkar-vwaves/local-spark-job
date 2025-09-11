package com.enttribe.pm.job.alert.otf;

import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.context.JobContext;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

public class OTFGenerateOpenAlert extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(OTFGenerateOpenAlert.class);

    private static final String DEFAULT_GEOGRAPHY_VALUE = "-";
    private static final String DEFAULT_SENDER_IP = "-";
    private static final int SQL_PARAMETER_INDEX_ENTITY_ID = 1;

    private static final String PARAM_CURRENT_INDEX = "START_INDEX";
    private static final String PARAM_CONFIGURATION_MAP = "CONFIGURATION_MAP";
    private static final String PARAM_NODE_AND_AGGREGATION_DETAILS_MAP = "NODE_AND_AGGREGATION_DETAILS_MAP";
    private static final String PARAM_EXTRACTED_PARAMETERS_MAP = "EXTRACTED_PARAMETERS_MAP";
    private static final String PARAM_TIMESTAMP = "TIMESTAMP";
    private static final String PARAM_DOMAIN = "DOMAIN";
    private static final String PARAM_VENDOR = "VENDOR";

    public OTFGenerateOpenAlert() {
        super();
    }

    public OTFGenerateOpenAlert(Dataset<Row> dataFrame, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataFrame;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (this.dataFrame == null || this.dataFrame.isEmpty()) {
            return this.dataFrame;
        }

        this.dataFrame.show();
        logger.info("+++++++++++++++++++++[BREACHED DATA]+++++++++++++++++++++");

        logger.info("OTFGenerateOpenAlert Execution Started!");

        Dataset<Row> breachedDF = this.dataFrame;
        List<Row> breachedRows = breachedDF.collectAsList();
        if (breachedRows.isEmpty()) {
            return this.dataFrame;
        }

        logger.info("Total Breached Rows to Process: {}", breachedRows.size());

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
            throw new RuntimeException("Configuration Map is Null or Empty");
        }

        if (extractedParametersMap == null || extractedParametersMap.isEmpty()) {
            throw new RuntimeException("Extracted Parameters Map is Null or Empty");
        }

        Map<String, Map<String, String>> latLongCache = new HashMap<>();

        for (Row row : breachedRows) {

            Map<String, String> metaDataMap = row.getJavaMap(row.fieldIndex("META_DATA_MAP"));
            Map<String, String> kpiCounterMap = row.getJavaMap(row.fieldIndex("KPI_COUNTER_MAP"));

            if (metaDataMap == null || metaDataMap.isEmpty()) {
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

            OTFGenerateOpenAlert.AlarmWrapper alarmWrapper = getAlarmWrapper(configurationMap,
                    nodeAndAggregationDetailsMap,
                    extractedParametersMap, jobContext, kpiCounterMap);

            Map<String, String> latLongMap;
            if (latLongCache.containsKey(entityId)) {
                latLongMap = latLongCache.get(entityId);
            } else {
                latLongMap = getLatLongUsingEntityId(entityId, jobContext);
                latLongCache.put(entityId, latLongMap);
            }

            alarmWrapper.setEventType(entityType);
            alarmWrapper.setEntityId(entityId);
            alarmWrapper.setEntityName(entityName);
            alarmWrapper.setEntityType(entityType);
            alarmWrapper.setEntityStatus(entityStatus);
            alarmWrapper.setLocationId(entityName);
            alarmWrapper.setSubentity(subEntity);
            alarmWrapper.setLatitude(parseDouble(latLongMap.get("LATITUDE")));
            alarmWrapper.setLongitude(parseDouble(latLongMap.get("LONGITUDE")));
            alarmWrapper.setGeographyL1Name(geographyL1Name);
            alarmWrapper.setGeographyL2Name(geographyL2Name);
            alarmWrapper.setGeographyL3Name(geographyL3Name);
            alarmWrapper.setGeographyL4Name(geographyL4Name);
            alarmWrapper.setNeCategory(entityType);

            logger.info("Alarm Wrapper: {}", alarmWrapper.toString());
            String kafkaBroker = jobContext.getParameter("SPARK_KAFKA_BROKER_ANSIBLE");
            produceMessage(alarmWrapper, alarmWrapper.getKafkaTopicName(), kafkaBroker);

        }
        return this.dataFrame;
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

    private static Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static AlarmWrapper getAlarmWrapper(Map<String, String> configurationMap,
            Map<String, String> nodeAndAggregationDetailsMap, Map<String, String> extractedParametersMap,
            JobContext jobContext, Map<String, String> kpiCounterMap) throws Exception {

        String alarmExternalId = configurationMap.get("ALARM_IDENTIFIER");
        String alarmCode = configurationMap.get("ALARM_ID");
        String domain = jobContext.getParameter(PARAM_DOMAIN);
        String vendor = jobContext.getParameter(PARAM_VENDOR);
        String technology = configurationMap.get("TECHNOLOGY");
        String timestamp = jobContext.getParameter(PARAM_TIMESTAMP);

        if (alarmExternalId == null || alarmCode == null || domain == null || vendor == null || technology == null) {
            logger.error(
                    "Missing Critical Alarm Fields - ALARM_EXTERNAL_ID: {}, ALARM_CODE: {}, DOMAIN: {}, VENDOR: {}, TECHNOLOGY: {}",
                    alarmExternalId, alarmCode, domain, vendor, technology);
            throw new RuntimeException("Missing Critical Alarm Fields");
        }

        String alarmName = configurationMap.get("ALARM_NAME");
        String severity = extractedParametersMap.get("SEVERITY");
        String actualSeverity = configurationMap.get("DEFAULT_SEVERITY");
        String senderName = configurationMap.get("EMS_TYPE");
        String classification = configurationMap.get("CLASSIFICATION");
        String probableCause = configurationMap.get("EXPRESSION");
        String senderIp = DEFAULT_SENDER_IP;
        boolean serviceAffected = safeParseBoolean(extractedParametersMap, "SERVICE_AFFECTING");
        boolean manualCloseable = safeParseBoolean(extractedParametersMap, "MANUALLY_CLOSEABLE");
        boolean correlationFlag = safeParseBoolean(extractedParametersMap, "CORRELATION_FLAG");
        String description = configurationMap.get("DESCRIPTION");
        String alarmGroup = configurationMap.get("ALARM_GROUP");
        String kafkaTopicName = jobContext.getParameter("KAFKA_TOPIC_NAME");
        String expression = configurationMap.get("EXPRESSION");
        String additionalDetail = getAdditionalDetail(kpiCounterMap, expression, jobContext);

        OTFGenerateOpenAlert.AlarmWrapper alarmWrapper = new OTFGenerateOpenAlert.AlarmWrapper();
        alarmWrapper.setOpenTime(toEpochMillis(timestamp));
        alarmWrapper.setChangeTime(toEpochMillis(timestamp));
        alarmWrapper.setReportingTime(toEpochMillis(timestamp));
        alarmWrapper.setAlarmExternalId(alarmExternalId);
        alarmWrapper.setAlarmCode(alarmCode);
        alarmWrapper.setAlarmName(alarmName);
        alarmWrapper.setSeverity(severity);
        alarmWrapper.setActualSeverity(actualSeverity);
        alarmWrapper.setDomain(domain);
        alarmWrapper.setVendor(vendor);
        alarmWrapper.setSenderName(senderName);
        alarmWrapper.setTechnology(technology);
        alarmWrapper.setClassification(classification);
        alarmWrapper.setProbableCause(probableCause);
        alarmWrapper.setSenderIp(senderIp);
        alarmWrapper.setServiceAffected(serviceAffected);
        alarmWrapper.setDescription(description);
        alarmWrapper.setManualCloseable(manualCloseable);
        alarmWrapper.setKafkaTopicName(kafkaTopicName);
        alarmWrapper.setCorrelationFlag(correlationFlag);
        alarmWrapper.setAdditionalDetail(additionalDetail);
        alarmWrapper.setAlarmGroup(alarmGroup);

        return alarmWrapper;
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

    private static String getAdditionalDetail(Map<String, String> kpiCounterMap, String expression,
            JobContext jobContext) throws Exception {
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

        @Override
        public String toString() {
            return "AlarmWrapper{" +
                    "openTime=" + openTime +
                    ", changeTime=" + changeTime +
                    ", reportingTime=" + reportingTime +
                    ", alarmExternalId='" + alarmExternalId + '\'' +
                    ", alarmCode='" + alarmCode + '\'' +
                    ", alarmName='" + alarmName + '\'' +
                    ", severity='" + severity + '\'' +
                    ", actualSeverity='" + actualSeverity + '\'' +
                    ", domain='" + domain + '\'' +
                    ", vendor='" + vendor + '\'' +
                    ", senderName='" + senderName + '\'' +
                    ", technology='" + technology + '\'' +
                    ", classification='" + classification + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", probableCause='" + probableCause + '\'' +
                    ", entityId='" + entityId + '\'' +
                    ", entityName='" + entityName + '\'' +
                    ", entityType='" + entityType + '\'' +
                    ", entityStatus='" + entityStatus + '\'' +
                    ", locationId='" + locationId + '\'' +
                    ", subentity='" + subentity + '\'' +
                    ", senderIp='" + senderIp + '\'' +
                    ", latitude=" + latitude +
                    ", longitude=" + longitude +
                    ", serviceAffected=" + serviceAffected +
                    ", description='" + description + '\'' +
                    ", manualCloseable=" + manualCloseable +
                    ", geographyL1Name='" + geographyL1Name + '\'' +
                    ", geographyL2Name='" + geographyL2Name + '\'' +
                    ", geographyL3Name='" + geographyL3Name + '\'' +
                    ", geographyL4Name='" + geographyL4Name + '\'' +
                    ", kafkaTopicName='" + kafkaTopicName + '\'' +
                    ", correlationFlag=" + correlationFlag +
                    ", neCategory='" + neCategory + '\'' +
                    ", additionalDetail='" + additionalDetail + '\'' +
                    ", alarmGroup='" + alarmGroup + '\'' +
                    '}';
        }
    }

}
