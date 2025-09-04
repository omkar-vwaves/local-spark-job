package com.enttribe.pm.job.alert;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

public class ProcessBreachedDF02 extends Processor {
    private static final Logger logger = LoggerFactory.getLogger(ProcessBreachedDF02.class);

    public ProcessBreachedDF02() {
        logger.info("ProcessBreachedDF02 No Argument Constructor Called!");
    }

    public ProcessBreachedDF02(Dataset<Row> dataFrame, Integer id, String processorName) {
        super(id, processorName);
        logger.info("ProcessBreachedDF02 Constructor Called with Input DataFrame With ID: {} and Processor Name: {}",
                id, processorName);
    }

    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("ProcessBreachedDF02 Execution Started!");

        String CURRENT_INDEX = jobContext.getParameter("START_INDEX");
        logger.info("START_INDEX: {}", CURRENT_INDEX);

        String configurationMapJson = jobContext.getParameter("CONFIGURATION_MAP" + CURRENT_INDEX);
        logger.info("CONFIGURATION MAP: {}", configurationMapJson);
        Map<String, String> configurationMap = new ObjectMapper().readValue(configurationMapJson,
                new TypeReference<Map<String, String>>() {
                });
        String ALARM_EXTERNAL_ID = (String) configurationMap.get("ALARM_EXTERNAL_ID");
        String ALARM_CODE = (String) configurationMap.get("ALARM_CODE");
        String EXPRESSION = (String) configurationMap.get("EXPRESSION");
        String DESCRIPTION = (String) configurationMap.get("DESCRIPTION");
        String nodeAndAggregationDetailsMapJson = jobContext
                .getParameter("NODE_AND_AGGREGATION_DETAILS_MAP" + CURRENT_INDEX);
        logger.info("NODE AND AGGREGATION DETAILS MAP: {}", nodeAndAggregationDetailsMapJson);
        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper()
                .readValue(nodeAndAggregationDetailsMapJson, new TypeReference<Map<String, String>>() {
                });
        String filterLevel = jobContext.getParameter("FILTER_LEVEL" + CURRENT_INDEX);
        logger.info("FILTER LEVEL: {}", filterLevel);
        logger.info("ALARM_EXTERNAL_ID: {}", ALARM_EXTERNAL_ID);
        logger.info("ALARM_CODE: {}", ALARM_CODE);
        logger.info("EXPRESSION: {}", EXPRESSION);
        logger.info("DESCRIPTION: {}", DESCRIPTION);
        List<Row> alertRows = new ArrayList<>();
        List<Row> breachedRows = this.dataFrame.collectAsList();
        if (breachedRows.isEmpty()) {
            logger.info("No Breached Rows Found!");
            return this.dataFrame;
        } else {
            Iterator<Row> breachedRowsIterator = breachedRows.iterator();

            while (breachedRowsIterator.hasNext()) {
                Row row = breachedRowsIterator.next();
                Map<String, String> metaDataMap = row.getJavaMap(row.fieldIndex("META_DATA_MAP"));
                Map<String, String> kpiCounterMap = row.getJavaMap(row.fieldIndex("KPI_COUNTER_MAP"));
                logger.info("META_DATA_MAP: {}", metaDataMap);
                logger.info("KPI_COUNTER_MAP: {}", kpiCounterMap);
                String additionalDetail = getAdditionalDetail(kpiCounterMap, EXPRESSION, jobContext);
                logger.info("ADDITIONAL DETAIL: {}", additionalDetail);
                if (metaDataMap == null || kpiCounterMap == null) {
                    logger.info("Skipping Row with Null META_DATA_MAP or KPI_COUNTER_MAP");
                } else {
                    String entityName = (String) metaDataMap.get("PARENT_ENTITY_NAME");
                    String entityId = (String) metaDataMap.get("PARENT_ENTITY_ID");
                    String entityType = (String) metaDataMap.get("PARENT_ENTITY_TYPE");
                    String subEntity = (String) metaDataMap.get("ENTITY_ID");
                    String geoL1 = (String) nodeAndAggregationDetailsMap.get("geoL1");
                    if (geoL1.equalsIgnoreCase("Custom") && filterLevel.equalsIgnoreCase("L0")) {
                        entityId = "Custom";
                        entityName = "Custom";
                        subEntity = "Custom";
                        logger.info("Entity ID: {}, Entity Name: {}, SubEntity: {} Set to Custom",
                                new Object[] { entityId, entityName, subEntity });
                    }
                    if (filterLevel.equalsIgnoreCase("H1")) {
                        subEntity = (String) metaDataMap.get("PARENT_ENTITY_TYPE");
                    }

                    Map<String, String> alertMap = new HashMap<>();
                    alertMap.put("ALARM_EXTERNAL_ID", ALARM_EXTERNAL_ID);
                    alertMap.put("ALARM_CODE", ALARM_CODE);
                    alertMap.put("PROBABLE_CAUSE", EXPRESSION);
                    alertMap.put("DESCRIPTION", DESCRIPTION);
                    alertMap.put("ENTITY_NAME", entityName);
                    alertMap.put("ENTITY_ID", entityId);
                    alertMap.put("ENTITY_TYPE", entityType);
                    alertMap.put("SUBENTITY", subEntity);
                    alertMap.put("ALARM_STATUS", "CLOSED");
                    alertMap.put("SEVERITY", "CLEARED");
                    String timestamp = jobContext.getParameter("TIMESTAMP");
                    String closureTime = getClosureTimeUsingTimestamp(timestamp);
                    logger.info("CLOSURE TIME: {} With Timestamp: {}", closureTime, timestamp);
                    String currentTimeUTC = getCurrentTimeUTC();
                    alertMap.put("CLOSURE_TIME", closureTime);
                    alertMap.put("LAST_PROCESSING_TIME", currentTimeUTC);
                    alertMap.put("CLOSURE_RECEPTION_TIME", currentTimeUTC);
                    alertMap.put("CLOSURE_PROCESSING_TIME", currentTimeUTC);
                    alertMap.put("ADDITIONAL_DETAIL", additionalDetail);
                    Row alertRow = RowFactory.create(alertMap);
                    alertRows.add(alertRow);
                }
            }

            return buildAlertDataset(jobContext, alertRows);
        }
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

    private static String getCurrentTimeUTC() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
        return formatter.format(Instant.now());
    }

    // private static String getClosureTimeUsingTimestamp(String timestamp) {
    // DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd
    // HH:mm:ss.SSSSSSXXXXX");
    // String fixedTimestamp = timestamp.replace("+0000", "+00:00");
    // OffsetDateTime offsetDateTime = OffsetDateTime.parse(fixedTimestamp,
    // inputFormatter);
    // DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd
    // HH:mm:ss.SSS");
    // return offsetDateTime.toLocalDateTime().format(outputFormatter);
    // }

    private static String getClosureTimeUsingTimestamp(String timestamp) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXXXX");
        String fixedTimestamp = timestamp.replace("+0000", "+00:00");
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(fixedTimestamp, inputFormatter);

        // Convert to UTC
        OffsetDateTime utcDateTime = offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC);

        // Format in UTC
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return utcDateTime.toLocalDateTime().format(outputFormatter);
    }

    public static Dataset<Row> buildAlertDataset(JobContext jobContext, List<Row> rows) {
        StructType schema = (new StructType()).add("ALERT_MAP",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        return jobContext.createDataFrame(rows, schema);
    }
}
