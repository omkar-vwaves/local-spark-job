package com.enttribe.pm.job.alert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ExtractConfiguration extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ExtractConfiguration.class);
    private static Set<String> kpiCodeSet = new HashSet<>();
    private static Set<String> counterIdSet = new HashSet<>();

    public ExtractConfiguration() {
        super();
        logger.info("ExtractConfiguration No Argument Constructor Called!");
    }

    public ExtractConfiguration(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("ExtractConfiguration Constructor Called with Input DataFrame With ID: {} and Processor Name: {}",
                id,
                processorName);
    }

    public ExtractConfiguration(Integer id, String processorName) {
        super(id, processorName);
        logger.info("ExtractConfiguration Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (this.dataFrame == null || this.dataFrame.isEmpty()) {
            logger.info("No Rules Found, to Extract Configuration!");
            jobContext.setParameters("START_INDEX", "0");
            jobContext.setParameters("END_INDEX", "0");
            logger.info("START_INDEX: {} Set to Job Context Successfully!", jobContext.getParameter("START_INDEX"));
            logger.info("END_INDEX: {} Set to Job Context Successfully!", jobContext.getParameter("END_INDEX"));
            return this.dataFrame;
        }

        this.dataFrame = removeDuplicates(this.dataFrame);

        int index = 0;
        for (Row row : this.dataFrame.collectAsList()) {

            setGlobalVariables(row);
            Map<String, String> configurationMap = getConfigurationMap(row);

            Map<String, String> nodeAndAggregationDetailsMap = getNodeAndAggregationDetails(
                    configurationMap.get("CONFIGURATION"));

            Map<String, String> extractedParametersMap = extractParametersFromConfiguration(configurationMap,
                    jobContext);

            String filterLevel = getFilterLevel(extractedParametersMap, nodeAndAggregationDetailsMap);
            String configurationMapJson = new ObjectMapper().writeValueAsString(configurationMap);
            String nodeAndAggregationDetailsMapJson = new ObjectMapper()
                    .writeValueAsString(nodeAndAggregationDetailsMap);
            String extractedParametersMapJson = new ObjectMapper().writeValueAsString(extractedParametersMap);

            logger.info("Configuration Map: {}", configurationMap);
            logger.info("Node And Aggregation Details Map: {}", nodeAndAggregationDetailsMap);
            logger.info("Extracted Parameters Map: {}", extractedParametersMap);
            logger.info("Filter Level: {} Set to Job Context Successfully For Index: {}", filterLevel, index);

            jobContext.setParameters("CONFIGURATION_MAP" + index, configurationMapJson);
            jobContext.setParameters("NODE_AND_AGGREGATION_DETAILS_MAP" + index, nodeAndAggregationDetailsMapJson);
            jobContext.setParameters("EXTRACTED_PARAMETERS_MAP" + index, extractedParametersMapJson);
            jobContext.setParameters("FILTER_LEVEL" + index, filterLevel);

            ++index;
        }

        logger.info("Total Received KPI Codes: {}", String.join(",", kpiCodeSet));
        logger.info("Total Received Counter IDs: {}", String.join(",", counterIdSet));

        jobContext.setParameters("KPI_CODES", String.join(",", kpiCodeSet));
        jobContext.setParameters("COUNTER_IDS", String.join(",", counterIdSet));

        Map<String, Map<String, String>> counterInfoMap = new HashMap<>();
        if (!kpiCodeSet.isEmpty()) {
            counterInfoMap = getCounterInfoMap(jobContext);
        }
        counterInfoMap = getCounterInfoMap2(jobContext, counterInfoMap);

        String counterInfoMapJson = new ObjectMapper().writeValueAsString(counterInfoMap);
        jobContext.setParameters("COUNTER_INFO_MAP", counterInfoMapJson);

        String categoryList = counterInfoMap.entrySet().stream().map(e -> e.getValue().get("CATEGORY_NAME"))
                .distinct().collect(Collectors.joining(","));
        jobContext.setParameters("CATEGORY_LIST", categoryList);

        Map<String, List<Map<String, String>>> catgoryInfoMap = getCatgoryInfoMap(counterInfoMap);

        String categoryInfoMapJson = new ObjectMapper().writeValueAsString(catgoryInfoMap);
        jobContext.setParameters("CATEGORY_INFO_MAP", categoryInfoMapJson);

        String frequency = jobContext.getParameter("FREQUENCY");
        getPMCounterVariableAggrQuery(jobContext, frequency);

        Map<String, Map<String, String>> kpiFormulaFinalMap = new HashMap<>();
        if (!kpiCodeSet.isEmpty()) {
            String kpiCodes = jobContext.getParameter("KPI_CODES");
            kpiFormulaFinalMap = getKpiFormulaMap(kpiCodes, jobContext);
        }

        String kpiFormulaFinalMapJson = new ObjectMapper().writeValueAsString(kpiFormulaFinalMap);
        jobContext.setParameters("KPI_FORMULA_MAP", kpiFormulaFinalMapJson);

        logger.info("Counter Info Map: {}", counterInfoMap);
        logger.info("Category List: {}", categoryList);
        logger.info("Catgory Info Map: {}", catgoryInfoMap);
        logger.info("KPI Formula Final Map: {}", kpiFormulaFinalMap);

        jobContext.setParameters("START_INDEX", "0");
        jobContext.setParameters("END_INDEX", String.valueOf(index));

        logger.info("START_INDEX: {} Set to Job Context Successfully!", jobContext.getParameter("START_INDEX"));
        logger.info("END_INDEX: {} Set to Job Context Successfully!", jobContext.getParameter("END_INDEX"));

        return this.dataFrame;
    }

    private String getFilterLevel(Map<String, String> EXTRACTED_PARAMETERS_MAP,
            Map<String, String> NODE_AND_AGGREGATION_DETAILS_MAP) {

        String level = EXTRACTED_PARAMETERS_MAP.get("LEVEL");
        String isNodeLevel = EXTRACTED_PARAMETERS_MAP.get("IS_NODE_LEVEL");
        String geoL1 = NODE_AND_AGGREGATION_DETAILS_MAP.get("geoL1");

        logger.info("Level: {} and Is Node Level: {} and GeoL1: {}", level, isNodeLevel, geoL1);

        if (level.equalsIgnoreCase("Custom") && isNodeLevel.equalsIgnoreCase("true")) {
            return "H1";
        } else if (level.equalsIgnoreCase("Custom") && isNodeLevel.equalsIgnoreCase("false")) {
            return "L0";
        } else if (level.equalsIgnoreCase("MO")) {
            return "NAM";
        } else if (isNodeLevel.equalsIgnoreCase("true") && geoL1.equalsIgnoreCase("Custom")) {
            return "H1";
        } else {
            throw new RuntimeException("Invalid Level: " + level + " and Is Node Level: " + isNodeLevel);
        }
    }

    private Map<String, Map<String, String>> getCounterInfoMap2(JobContext jobContext,
            Map<String, Map<String, String>> counterInfoMap) throws SQLException {

        String query = "SELECT UPPER(COUNTER_HEADER_NAME) AS COUNTER_HEADER_NAME, KPI_COUNTER_ID_PK AS PM_COUNTER_VARIABLE_ID_PK, UPPER(CATEGORY_ALIAS_NAME) AS CATEGORY_NAME, ATTRIBUTE AS ATTRIBUTE, '' AS SUBCATEGORY1_VALUE, '' AS SUBCATEGORY2_VALUE, '' AS SUBCATEGORY3_VALUE, '' AS SUBCATEGORY4_VALUE,  '' AS SUBCAT_HEADER1, '' AS SUBCAT_HEADER2, '' AS SUBCAT_HEADER3, '' AS SUBCAT_HEADER4, SEQUENCE_NO AS SEQUENCE_NO, PM_CATEGORY_ID_FK AS CATEGORY_ID, '' AS UNIQUE_STRING, NODE_AGGREGATION AS NODE_AGGREGATION, TIME_AGGREGATION AS TIME_AGGREGATION FROM KPI_COUNTER WHERE KPI_COUNTER_ID_PK IN ($COUNTER_IDS)";

        String counterIds = jobContext.getParameter("COUNTER_IDS");
        query = query.replace("$COUNTER_IDS", counterIds);

        logger.info("Counter Info Map Query 2: {}", query);

        ResultSet resultSet = executeQueryAndGetResultSet(query, jobContext);
        Map<String, Map<String, String>> counterInfoMap2 = getCounterInfoMapFromResultSet(resultSet);

        for (Map.Entry<String, Map<String, String>> entry : counterInfoMap2.entrySet()) {
            counterInfoMap.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return counterInfoMap;
    }

    private void setGlobalVariables(Row row) {
        String EXPRESSION = row.getAs("EXPRESSION") != null ? row.getAs("EXPRESSION").toString() : "";
        if (!EXPRESSION.isEmpty()) {
            extractCodes(EXPRESSION);
        }
    }

    private void extractCodes(String expression) {
        Matcher kpiMatcher = Pattern.compile("KPI#(\\d+)").matcher(expression);
        while (kpiMatcher.find()) {
            String kpiCode = kpiMatcher.group(1);
            kpiCodeSet.add(kpiCode);
        }

        Matcher counterMatcher = Pattern.compile("COUNTER#(\\d+)").matcher(expression);
        while (counterMatcher.find()) {
            String counterId = counterMatcher.group(1);
            counterIdSet.add(counterId);
        }
    }

    private String generateDate(String timeKey, String timestamp) {
        logger.info("Generating Date for Time Key={} and Timestamp={}", timeKey, timestamp);
        String key = timeKey == null ? "" : timeKey.trim().toLowerCase();
        String date = "";

        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ");
            ZonedDateTime zdt = ZonedDateTime.parse(timestamp, inputFormatter).withZoneSameInstant(ZoneOffset.UTC);

            switch (key) {
                case "fiveminutekey":
                case "quarterkey":
                    date = zdt.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                    break;
                case "hourkey":
                    date = zdt.format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
                    break;
                case "datekey":
                    date = zdt.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                    break;
                default:
                    logger.error("Unknown Time Key received: '{}'", key);
            }

        } catch (DateTimeParseException e) {
            logger.error("Failed to Parse Timestamp: {} With Error: {}", timestamp, e.getMessage());
        }

        logger.info("Generated Date={} for Time Key={}", date, timeKey);
        return date;
    }

    private void getPMCounterVariableAggrQuery(JobContext jobContext, String frequency) throws Exception {

        logger.info("Get PM Counter Variable Aggr Query, With Frequency={}", frequency);

        StringBuilder NODE_AGGREGATION_QUERY_BUILDER = new StringBuilder();
        StringBuilder COUNTER_WITH_NODE_AGGR_BUILDER = new StringBuilder();
        StringBuilder COUNTER_WITH_TIME_AGGR_BUILDER = new StringBuilder();
        StringBuilder FILTER_QUERY_BUILDER = new StringBuilder();

        StringBuilder mapQuery = new StringBuilder();
        String COUNTER_QUERY_MAP = null;

        Set<String> fiveMinuteKeys = Set.of("5 MIN", "FIVEMIN");
        Set<String> quarterKeys = Set.of("15 MIN", "QUARTERLY");
        Set<String> dateKeys = Set.of("DAILY", "PERDAY", "WEEKLY", "PERWEEK", "MONTHLY", "PERMONTH", "YEARLY",
                "PERYEAR");
        Set<String> hourKeys = Set.of("HOURLY", "PERHOUR");

        String timeKey = "";
        String upperFreq = frequency.toUpperCase();
        if (fiveMinuteKeys.contains(upperFreq)) {
            timeKey = "fiveminutekey ";
        } else if (quarterKeys.contains(upperFreq)) {
            timeKey = "quarterKey ";
        } else if (dateKeys.contains(upperFreq)) {
            timeKey = "dateKey ";
        } else if (hourKeys.contains(upperFreq)) {
            timeKey = "hourKey ";
        } else {
            timeKey = "quarterKey ";
        }

        logger.info("Time Key={}", timeKey);

        String categoryInfoMapJson = jobContext.getParameter("CATEGORY_INFO_MAP");
        @SuppressWarnings("unchecked")
        Map<String, List<Map<String, String>>> categoryInfoMap = new ObjectMapper().readValue(categoryInfoMapJson,
                Map.class);

        String categoryList = jobContext.getParameter("CATEGORY_LIST");

        logger.info("Category List={}", categoryList);
        logger.info("Category Info Map={}", categoryInfoMap);

        COUNTER_WITH_NODE_AGGR_BUILDER.append(
                " SELECT fiveminutekey, quarterKey, dateKey, hourKey, finalKey, categoryname, NAM, FIRST_VALUE(metaData) AS metaData, ");

        for (String category : categoryList.split(",")) {

            String categoryName = category.split("@")[0];
            List<Map<String, String>> categoryInfoList = categoryInfoMap.get(category);
            if (categoryInfoList == null) {
                continue;
            }

            for (Map<String, String> eachCategoryInfoMap : categoryInfoList) {

                String sequenceNo = eachCategoryInfoMap.get("SEQUENCE_NO");
                String pmCounterVariableIdPk = eachCategoryInfoMap.get("PM_COUNTER_VARIABLE_ID_PK");
                String nodeAggregation = eachCategoryInfoMap.get("NODE_AGGREGATION");
                String timeAggregation = eachCategoryInfoMap.get("TIME_AGGREGATION");

                String counterKey = "C" + sequenceNo + "#" + pmCounterVariableIdPk;
                String counterId = counterKey.split("#")[1];
                String nodeAggrVal = nodeAggregation;
                String timeAggrVal = timeAggregation;

                if (!nodeAggrVal.isEmpty() && !timeAggrVal.isEmpty()) {
                    mapQuery.append("'").append(counterKey).append("', `").append(counterKey)
                            .append("`, '").append(counterId).append("', `").append(counterKey).append("`, ");
                }

                logger.info("Generated Map Query={}", mapQuery);

                String timestamp = jobContext.getParameter("TIMESTAMP");
                String date = generateDate(timeKey, timestamp);
                logger.info("Generated Date={} for Time Key={}", date, timeKey);

                if (!nodeAggrVal.isEmpty()) {
                    if (nodeAggrVal.equalsIgnoreCase("AVG")) {
                        COUNTER_WITH_NODE_AGGR_BUILDER.append(nodeAggrVal).append("(CASE WHEN categoryname = '")
                                .append(categoryName)
                                .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                                .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE)  ELSE NULL END) AS `")
                                .append(counterKey).append("`, sum(CASE WHEN categoryname = '").append(categoryName)
                                .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                                .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE) ELSE NULL END) AS `S")
                                .append(counterKey).append("`, COUNT(CASE WHEN categoryname = '").append(categoryName)
                                .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                                .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE) ELSE NULL END) AS `C")
                                .append(counterKey).append("`, ");

                        NODE_AGGREGATION_QUERY_BUILDER.append("sum(`S").append(counterKey).append("`)/sum(`C")
                                .append(counterKey)
                                .append("`) AS `").append(counterKey).append("`, sum(`S").append(counterKey)
                                .append("`) AS `S").append(counterKey).append("`, sum(`C").append(counterKey)
                                .append("`) AS `C").append(counterKey).append("`, ");

                        FILTER_QUERY_BUILDER.append("(`S").append(counterKey).append("`) AS `S").append(counterKey)
                                .append("`, (`C").append(counterKey).append("`) AS `C").append(counterKey).append("`,");
                    } else {
                        COUNTER_WITH_NODE_AGGR_BUILDER.append(nodeAggrVal).append("(CASE WHEN categoryname = '")
                                .append(categoryName)
                                .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                                .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE) ELSE NULL END) AS `")
                                .append(counterKey).append("`, ");

                        NODE_AGGREGATION_QUERY_BUILDER.append(nodeAggrVal).append("(`").append(counterKey)
                                .append("`) AS `")
                                .append(counterKey).append("`, ");

                        FILTER_QUERY_BUILDER.append("(`").append(counterKey).append("`) AS `").append(counterKey)
                                .append("`, ");
                    }
                }

                if (!timeAggrVal.isEmpty()) {
                    COUNTER_WITH_TIME_AGGR_BUILDER.append(timeAggrVal).append("(`").append(counterKey).append("`) AS `")
                            .append(counterKey).append("`,");
                }
            }
        }

        logger.info("COUNTER_WITH_NODE_AGGR_BUILDER={}", COUNTER_WITH_NODE_AGGR_BUILDER);
        logger.info("COUNTER_WITH_TIME_AGGR_BUILDER={}", COUNTER_WITH_TIME_AGGR_BUILDER);
        logger.info("NODE_AGGREGATION_QUERY_BUILDER={}", NODE_AGGREGATION_QUERY_BUILDER);
        logger.info("FILTER_QUERY_BUILDER={}", FILTER_QUERY_BUILDER);
        logger.info("MAP_QUERY={}", mapQuery);

        String mapQueryStr = mapQuery.toString();
        int lastCommaIndex = mapQueryStr.lastIndexOf(",");
        COUNTER_QUERY_MAP = (lastCommaIndex != -1 ? mapQueryStr.substring(0, lastCommaIndex) : mapQueryStr);
        COUNTER_QUERY_MAP = "Map(" + COUNTER_QUERY_MAP + ") AS rawcounters";

        String COUNTER_WITH_NODE_AGGR = COUNTER_WITH_NODE_AGGR_BUILDER.toString();
        String COUNTER_WITH_TIME_AGGR = COUNTER_WITH_TIME_AGGR_BUILDER.toString();
        String NODE_AGGREGATION_QUERY = NODE_AGGREGATION_QUERY_BUILDER.toString();
        String FILTER_QUERY = FILTER_QUERY_BUILDER.toString();

        COUNTER_WITH_NODE_AGGR = COUNTER_WITH_NODE_AGGR.trim().endsWith(",")
                ? COUNTER_WITH_NODE_AGGR.trim().substring(0, COUNTER_WITH_NODE_AGGR.trim().length() - 1)
                : COUNTER_WITH_NODE_AGGR;
        COUNTER_WITH_TIME_AGGR = COUNTER_WITH_TIME_AGGR.trim().endsWith(",")
                ? COUNTER_WITH_TIME_AGGR.trim().substring(0, COUNTER_WITH_TIME_AGGR.trim().length() - 1)
                : COUNTER_WITH_TIME_AGGR;
        NODE_AGGREGATION_QUERY = NODE_AGGREGATION_QUERY.trim().endsWith(",")
                ? NODE_AGGREGATION_QUERY.trim().substring(0, NODE_AGGREGATION_QUERY.trim().length() - 1)
                : NODE_AGGREGATION_QUERY;
        FILTER_QUERY = FILTER_QUERY.trim().endsWith(",")
                ? FILTER_QUERY.trim().substring(0, FILTER_QUERY.trim().length() - 1)
                : FILTER_QUERY;

        String COUNTER_NODE_AGGR_QUERY = "SELECT finalKey, FIRST_VALUE(metaData) AS metaData, " + NODE_AGGREGATION_QUERY
                + " FROM FinalCounterData GROUP BY finalKey";

        String FILTER_QUERY_FINAL = FILTER_QUERY;

        String RAW_FILE_COUNTER_NODE_AGGR_QUERY = COUNTER_WITH_NODE_AGGR
                + " FROM JOINED_RESULT GROUP BY fiveminutekey, quarterKey, finalKey, dateKey, hourKey, NAM, categoryname";

        String COUNTER_TIME_AGGR_QUERY = "SELECT finalKey, FIRST_VALUE(metaData) AS metaData, " + COUNTER_WITH_TIME_AGGR
                + " FROM finalNodeAggrData GROUP BY finalKey ORDER BY finalKey";

        jobContext.setParameters("COUNTER_MAP_QUERY", COUNTER_QUERY_MAP);
        jobContext.setParameters("FILTER_QUERY_FINAL", FILTER_QUERY_FINAL);
        jobContext.setParameters("COUNTER_NODE_AGGR_QUERY", COUNTER_NODE_AGGR_QUERY);
        jobContext.setParameters("COUNTER_TIME_AGGR_QUERY", COUNTER_TIME_AGGR_QUERY);
        jobContext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", RAW_FILE_COUNTER_NODE_AGGR_QUERY);

        logger.info("COUNTER_MAP_QUERY={}", COUNTER_QUERY_MAP);
        logger.info("FILTER_QUERY_FINAL={}", FILTER_QUERY_FINAL);
        logger.info("COUNTER_NODE_AGGR_QUERY={}", COUNTER_NODE_AGGR_QUERY);
        logger.info("COUNTER_TIME_AGGR_QUERY={}", COUNTER_TIME_AGGR_QUERY);
        logger.info("RAW_FILE_COUNTER_NODE_AGGR_QUERY={}", RAW_FILE_COUNTER_NODE_AGGR_QUERY);
    }

    private static Map<String, List<Map<String, String>>> getCatgoryInfoMap(
            Map<String, Map<String, String>> counterInfoMap) {
        Map<String, List<Map<String, String>>> catgoryInfoMap = new LinkedHashMap<>();

        for (Map<String, String> value : counterInfoMap.values()) {

            List<Map<String, String>> infoMapList = catgoryInfoMap.get(value.get("CATEGORY_NAME"));
            if (infoMapList == null) {
                infoMapList = new ArrayList<>();
            }
            Map<String, String> infoMap = new LinkedHashMap<>();
            infoMap.put("SEQUENCE_NO", value.get("SEQUENCE_NO"));
            infoMap.put("COUNTER_HEADER_NAME", value.get("COUNTER_HEADER_NAME"));
            infoMap.put("PM_COUNTER_VARIABLE_ID_PK", value.get("PM_COUNTER_VARIABLE_ID_PK"));
            infoMap.put("UNIQUE_STRING", value.get("UNIQUE_STRING"));
            infoMap.put("NODE_AGGREGATION", value.get("NODE_AGGREGATION"));
            infoMap.put("TIME_AGGREGATION", value.get("TIME_AGGREGATION"));

            // SUB_CATEGORY_HEADER1

            if (value.get("SUB_CATEGORY_HEADER1") != null && !value.get("SUB_CATEGORY_HEADER1").isEmpty()
                    && !value.get("SUB_CATEGORY_HEADER1").equalsIgnoreCase("null")) {

                infoMap.put("SUB_CATEGORY_HEADER1", value.get("SUB_CATEGORY_HEADER1"));
                infoMap.put("SUB_CATEGORY_VALUE1", value.get("SUB_CATEGORY_VALUE1"));
            }

            // SUB_CATEGORY_HEADER2

            if (value.get("SUB_CATEGORY_HEADER2") != null && !value.get("SUB_CATEGORY_HEADER2").isEmpty()
                    && !value.get("SUB_CATEGORY_HEADER2").equalsIgnoreCase("null")) {

                infoMap.put("SUB_CATEGORY_HEADER2", value.get("SUB_CATEGORY_HEADER2"));
                infoMap.put("SUB_CATEGORY_VALUE2", value.get("SUB_CATEGORY_VALUE2"));
            }

            // SUB_CATEGORY_HEADER3

            if (value.get("SUB_CATEGORY_HEADER3") != null && !value.get("SUB_CATEGORY_HEADER3").isEmpty()
                    && !value.get("SUB_CATEGORY_HEADER3").equalsIgnoreCase("null")) {

                infoMap.put("SUB_CATEGORY_HEADER3", value.get("SUB_CATEGORY_HEADER3"));
                infoMap.put("SUB_CATEGORY_VALUE3", value.get("SUB_CATEGORY_VALUE3"));
            }

            // SUB_CATEGORY_HEADER4

            if (value.get("SUB_CATEGORY_HEADER4") != null && !value.get("SUB_CATEGORY_HEADER4").isEmpty()
                    && !value.get("SUB_CATEGORY_HEADER4").equalsIgnoreCase("null")) {

                infoMap.put("SUB_CATEGORY_HEADER4", value.get("SUB_CATEGORY_HEADER4"));
                infoMap.put("SUB_CATEGORY_VALUE4", value.get("SUB_CATEGORY_VALUE4"));
            }

            infoMapList.add(infoMap);
            catgoryInfoMap.put(value.get("CATEGORY_NAME"), infoMapList);
        }
        return catgoryInfoMap;
    }

    private static Map<String, String> extractParametersFromConfiguration(Map<String, String> inputMap,
            JobContext jobContext) {

        try {

            Map<String, String> parameters = new LinkedHashMap<>();

            String configuration = inputMap.get("CONFIGURATION");
            if (configuration != null && configuration.startsWith("\"") && configuration.endsWith("\"")) {
                configuration = configuration.substring(1, configuration.length() - 1);
            }

            if (configuration != null) {
                configuration = configuration.trim().replace("\\\"", "\"");
            }

            JSONObject configJson = new JSONObject(configuration);

            JSONArray nodeJSON = configJson.getJSONArray("node");
            List<String> nodeArray = getStringListFromArray(nodeJSON);
            String node = nodeArray.get(0);

            String classification = configJson.getString("classification");
            classification = classification != null && !classification.isEmpty() ? classification.toUpperCase()
                    : inputMap.get("CLASSIFICATION");

            String serviceAffecting = configJson.getString("serviceaffecting");
            serviceAffecting = serviceAffecting != null && !serviceAffecting.isEmpty() ? serviceAffecting.toLowerCase()
                    : inputMap.get("SERVICE_AFFECTING");

            serviceAffecting = (serviceAffecting.equalsIgnoreCase("true")) ? "0" : "1";

            String severity = "";
            if (configJson.has("priority")) {
                severity = configJson.getString("priority").toUpperCase();
            } else {
                severity = inputMap.get("DEFAULT_SEVERITY");
            }

            String upperSeverity = severity.toUpperCase();
            if ("EMERGENCY".equalsIgnoreCase(upperSeverity)) {
                severity = "CRITICAL";
            }

            String outOfLast = "0";
            String instances = "0";

            JSONObject consistencyJson = configJson.getJSONObject("Consistency");
            if (consistencyJson.has("outOfLast") && !consistencyJson.getString("outOfLast").isEmpty()) {
                outOfLast = consistencyJson.getString("outOfLast");
            }

            if (consistencyJson.has("Instances") && !consistencyJson.getString("Instances").isEmpty()) {
                instances = consistencyJson.getString("Instances");
            }

            JSONArray cellArray = configJson.getJSONArray("cells");
            JSONArray geoL1Array = configJson.getJSONArray("geography_l1");
            JSONArray geoL2Array = configJson.getJSONArray("geography_l2");
            JSONArray geoL3Array = configJson.getJSONArray("geography_l3");
            JSONArray geoL4Array = configJson.getJSONArray("geography_l4");
            JSONArray moArray = configJson.getJSONArray("mo");
            JSONArray nodeArray2 = configJson.getJSONArray("node");

            List<String> cellList = getStringListFromArray(cellArray);
            List<String> geoL1List = getStringListFromArray(geoL1Array);
            List<String> geoL2List = getStringListFromArray(geoL2Array);
            List<String> geoL3List = getStringListFromArray(geoL3Array);
            List<String> geoL4List = getStringListFromArray(geoL4Array);
            List<String> moList = getStringListFromArray(moArray);
            List<String> nodeList = getStringListFromArray(nodeArray2);

            String level = getLevelForReport(geoL1List, geoL2List, geoL3List, geoL4List, cellList, node,
                    geoL1List, inputMap.get("DOMAIN"), moList, nodeList);

            logger.info("Received Level For Alert: {}", level);

            String isNodeLevel = "false";
            if (!level.contains("L0") && !level.contains("L1") && !level.contains("L2")
                    && !level.contains("L3") && !level.contains("L4") && !level.contains("MO")
                    && !level.contains("Custom")) {

                if (geoL1List.get(0).toUpperCase().equalsIgnoreCase("INDIA")) {
                    isNodeLevel = "false";
                } else {
                    isNodeLevel = "true";
                }

            }
            if (level.equalsIgnoreCase(node)) {
                isNodeLevel = "true";
            }

            logger.info("Is Node Level For Alert: {}", isNodeLevel);

            parameters.put("INSTANCES", instances);
            parameters.put("OUT_OF_LAST", outOfLast);
            parameters.put("LEVEL", level);
            parameters.put("CLASSIFICATION", classification);
            parameters.put("SEVERITY", severity);
            parameters.put("IS_NODE_LEVEL", isNodeLevel);
            parameters.put("SERVICE_AFFECTING", serviceAffecting);

            String manuallyCloseable = inputMap.get("MANUAL_CLEARED");
            if (manuallyCloseable != null && manuallyCloseable.equalsIgnoreCase("true")) {
                manuallyCloseable = "1";
            } else {
                manuallyCloseable = "0";
            }

            String correlationEnable = inputMap.get("CORRELATION_ENABLE");
            if (correlationEnable != null && correlationEnable.equalsIgnoreCase("true")) {
                correlationEnable = "1";
            } else {
                correlationEnable = "0";
            }

            parameters.put("MANUALLY_CLOSEABLE", manuallyCloseable);
            parameters.put("CORRELATION_FLAG", correlationEnable);

            return parameters;

        } catch (Exception e) {
            logger.error("Error In Extracting Parameters From Configuration, Message: {}, Error: {}", e.getMessage(),
                    e);
            e.printStackTrace();
            return new LinkedHashMap<>();
        }
    }

    public static String getLevelForReport(List<String> geoL1List, List<String> geoL2List, List<String> geoL3List,
            List<String> geoL4List, List<String> cells, String node,
            List<String> coreDomains, String DOMAIN, List<String> moList, List<String> nodeList) {

        logger.info(
                "Getting Level For Alert With Inputs: GeoL1List={}, GeoL2List={}, GeoL3List={}, GeoL4List={}, Cells={}, Node={}, CoreDomains={}, DOMAIN={}, MoList={}, NodeList={}",
                geoL1List, geoL2List, geoL3List, geoL4List, cells, node, coreDomains, DOMAIN, moList, nodeList);

        boolean isNodeAggregated = nodeList.stream().anyMatch(s -> s.contains("AGGREGATED"));
        boolean isMoAggregated = moList.stream().anyMatch(s -> s.contains("AGGREGATED"));
        boolean isNodeIndividual = nodeList.stream().anyMatch(s -> s.contains("INDIVIDUAL"));
        boolean isMoIndividual = moList.stream().anyMatch(s -> s.contains("INDIVIDUAL"));

        if (geoL1List.contains("Custom")) {
            if (isNodeAggregated && isMoAggregated) {
                return "Custom";
            } else if (isNodeIndividual && isMoAggregated) {
                return getNodeName(node);
            } else if (isNodeIndividual && isMoIndividual) {
                return "MO";
            }
        }

        if (isMoIndividual) {
            return "MO";
        }

        boolean isClubbed = node != null && node.toUpperCase().contains("AGGREGATED");

        boolean isCoreDomain = coreDomains.contains(DOMAIN);
        String geoL1 = geoL1List != null && !geoL1List.isEmpty() ? geoL1List.get(0).toUpperCase() : "";
        String geoL2 = geoL2List != null && !geoL2List.isEmpty() ? geoL2List.get(0).toUpperCase() : "";
        String geoL3 = geoL3List != null && !geoL3List.isEmpty() ? geoL3List.get(0).toUpperCase() : "";
        String geoL4 = geoL4List != null && !geoL4List.isEmpty() ? geoL4List.get(0).toUpperCase() : "";

        if (!isClubbed && !geoL1.equalsIgnoreCase("INDIA")) {
            return getNodeName(node);
        }

        if (geoL1.equalsIgnoreCase("INDIA")) {
            return "L0";
        }

        if (!geoL1.contains("CLUBBED")) {
            if (!geoL2.contains("CLUBBED")) {
                if (!geoL3.contains("CLUBBED")) {
                    if (!geoL4.contains("CLUBBED")) {
                        return isClubbed ? "L4" : getNodeName(node);
                    } else {
                        return "L3";
                    }
                } else {
                    return "L2";
                }
            } else {
                if (isCoreDomain) {
                    return isClubbed ? "L1" : getNodeName(node);
                } else {
                    return geoL1.contains("India") ? "L0" : "L1";
                }
            }
        }

        return "L0";
    }

    public static String getNodeName(String nodeString) {

        if (nodeString == null) {
            return "";
        }

        int lastDashIndex = nodeString.lastIndexOf('-');
        String part = (lastDashIndex != -1) ? nodeString.substring(0, lastDashIndex) : nodeString;

        int spaceIndex = part.indexOf(' ');
        String node = (spaceIndex != -1) ? part.substring(spaceIndex + 1) : part;

        node = node.trim().toUpperCase();
        return node.replace(" ", "-");
    }

    private static Map<String, String> getConfigurationMap(Row row) {

        String PERFORMANCE_ALERT_ID_PK = "";
        String CONFIGURATION = "";
        String EXPRESSION = "";
        String DOMAIN = "";
        String VENDOR = "";
        String ALERTID = "";
        String NAME = "";
        String DESCRIPTION = "";
        String TECHNOLOGY = "";
        String ALARM_IDENTIFIER = "";
        String ALARM_NAME = "";
        String CLASSIFICATION = "";
        String NETYPE = "";
        String DEFAULT_SEVERITY = "";
        String EMS_TYPE = "";
        String EVENT_TYPE = "";
        String ALARM_ID = "";
        String SERVICE_AFFECTING = "";
        String CORRELATION_ENABLE = "";
        String MANUAL_CLEARED = "";
        String PROBABLE_CAUSE = "";
        String PRIORITY = "";
        String ALARM_LAYER = "";
        String ALARM_GROUP = "";
        String EQUIPMENT_TYPE = "";
        String IS_SOUTH_BOUND_INTEGRATION = "";

        PERFORMANCE_ALERT_ID_PK = row.getAs("PERFORMANCE_ALERT_ID_PK") != null
                ? row.getAs("PERFORMANCE_ALERT_ID_PK").toString()
                : "";
        CONFIGURATION = row.getAs("CONFIGURATION") != null ? row.getAs("CONFIGURATION").toString() : "";
        EXPRESSION = row.getAs("EXPRESSION") != null ? row.getAs("EXPRESSION").toString() : "";
        DOMAIN = row.getAs("DOMAIN") != null ? row.getAs("DOMAIN").toString() : "";
        VENDOR = row.getAs("VENDOR") != null ? row.getAs("VENDOR").toString() : "";
        ALERTID = row.getAs("ALERTID") != null ? row.getAs("ALERTID").toString() : "";
        NAME = row.getAs("NAME") != null ? row.getAs("NAME").toString() : "";
        DESCRIPTION = row.getAs("DESCRIPTION") != null ? row.getAs("DESCRIPTION").toString() : "";
        TECHNOLOGY = row.getAs("TECHNOLOGY") != null ? row.getAs("TECHNOLOGY").toString() : "";
        ALARM_IDENTIFIER = row.getAs("ALARM_IDENTIFIER") != null ? row.getAs("ALARM_IDENTIFIER").toString() : "";
        ALARM_NAME = row.getAs("ALARM_NAME") != null ? row.getAs("ALARM_NAME").toString() : "";
        CLASSIFICATION = row.getAs("CLASSIFICATION") != null ? row.getAs("CLASSIFICATION").toString().trim() : "";
        NETYPE = row.getAs("NETYPE") != null ? row.getAs("NETYPE").toString() : "";
        DEFAULT_SEVERITY = row.getAs("DEFAULT_SEVERITY") != null ? row.getAs("DEFAULT_SEVERITY").toString() : "";
        EMS_TYPE = row.getAs("EMS_TYPE") != null ? row.getAs("EMS_TYPE").toString() : "";
        EVENT_TYPE = row.getAs("EVENT_TYPE") != null ? row.getAs("EVENT_TYPE").toString() : "";
        ALARM_ID = row.getAs("ALARM_ID") != null ? row.getAs("ALARM_ID").toString() : "";
        SERVICE_AFFECTING = row.getAs("SERVICE_AFFECTING") != null ? row.getAs("SERVICE_AFFECTING").toString() : "";
        CORRELATION_ENABLE = row.getAs("CORRELATION_ENABLE") != null ? row.getAs("CORRELATION_ENABLE").toString()
                : "";
        MANUAL_CLEARED = row.getAs("MANUAL_CLEARED") != null ? row.getAs("MANUAL_CLEARED").toString() : "";
        // PROBABLE_CAUSE = row.getAs("PROBABLE_CAUSE") != null ?
        // row.getAs("PROBABLE_CAUSE").toString() : "";
        PROBABLE_CAUSE = row.getAs("DESCRIPTION") != null ? row.getAs("DESCRIPTION").toString() : "";
        PRIORITY = row.getAs("PRIORITY") != null ? row.getAs("PRIORITY").toString() : "";
        ALARM_LAYER = row.getAs("ALARM_LAYER") != null ? row.getAs("ALARM_LAYER").toString() : "";
        ALARM_GROUP = row.getAs("ALARM_GROUP") != null ? row.getAs("ALARM_GROUP").toString() : "";
        EQUIPMENT_TYPE = row.getAs("EQUIPMENT_TYPE") != null ? row.getAs("EQUIPMENT_TYPE").toString() : "";
        IS_SOUTH_BOUND_INTEGRATION = row.getAs("IS_SOUTH_BOUND_INTEGRATION") != null
                ? row.getAs("IS_SOUTH_BOUND_INTEGRATION").toString()
                : "";

        Map<String, String> configurationMap = new LinkedHashMap<>();
        configurationMap.put("PERFORMANCE_ALERT_ID_PK", PERFORMANCE_ALERT_ID_PK);
        configurationMap.put("CONFIGURATION", CONFIGURATION);
        configurationMap.put("EXPRESSION", EXPRESSION);
        configurationMap.put("DOMAIN", DOMAIN);
        configurationMap.put("VENDOR", VENDOR);
        configurationMap.put("ALERTID", ALERTID);
        configurationMap.put("NAME", NAME);
        configurationMap.put("DESCRIPTION", DESCRIPTION);
        configurationMap.put("TECHNOLOGY", TECHNOLOGY);
        configurationMap.put("ALARM_IDENTIFIER", ALARM_IDENTIFIER);
        configurationMap.put("ALARM_NAME", ALARM_NAME);
        configurationMap.put("CLASSIFICATION", CLASSIFICATION);
        configurationMap.put("NETYPE", NETYPE);
        configurationMap.put("DEFAULT_SEVERITY", DEFAULT_SEVERITY);
        configurationMap.put("EMS_TYPE", EMS_TYPE);
        configurationMap.put("EVENT_TYPE", EVENT_TYPE);
        configurationMap.put("ALARM_ID", ALARM_ID);
        configurationMap.put("SERVICE_AFFECTING", SERVICE_AFFECTING);
        configurationMap.put("CORRELATION_ENABLE", CORRELATION_ENABLE);
        configurationMap.put("MANUAL_CLEARED", MANUAL_CLEARED);
        configurationMap.put("PROBABLE_CAUSE", PROBABLE_CAUSE);
        configurationMap.put("PRIORITY", PRIORITY);
        configurationMap.put("ALARM_LAYER", ALARM_LAYER);
        configurationMap.put("ALARM_GROUP", ALARM_GROUP);
        configurationMap.put("EQUIPMENT_TYPE", EQUIPMENT_TYPE);
        configurationMap.put("IS_SOUTH_BOUND_INTEGRATION", IS_SOUTH_BOUND_INTEGRATION);

        return configurationMap;
    }

    private static Map<String, String> getNodeAndAggregationDetails(String configuration) {

        Map<String, String> nodeAndAggregationDetails = new LinkedHashMap<>();

        try {
            String fixedJson = configuration.replace("'", "\"").replaceAll("^\"|\"$", "");

            JSONObject jsonObject = new JSONObject(fixedJson);

            JSONArray geoL1JSONArray = jsonObject.getJSONArray("geography_l1");
            JSONArray geoL2JSONArray = jsonObject.getJSONArray("geography_l2");
            JSONArray geoL3JSONArray = jsonObject.getJSONArray("geography_l3");
            JSONArray geoL4JSONArray = jsonObject.getJSONArray("geography_l4");
            JSONArray nodeArray = jsonObject.getJSONArray("node");
            JSONArray moArray = jsonObject.getJSONArray("mo");
            JSONArray cellsArray = jsonObject.getJSONArray("cells");

            List<String> geoL1List = getStringListFromArray(geoL1JSONArray);
            List<String> geoL2List = getStringListFromArray(geoL2JSONArray);
            List<String> geoL3List = getStringListFromArray(geoL3JSONArray);
            List<String> geoL4List = getStringListFromArray(geoL4JSONArray);
            List<String> nodeList = getStringListFromArray(nodeArray);
            List<String> moList = getStringListFromArray(moArray);
            List<String> cellsList = getStringListFromArray(cellsArray);

            String geoL1 = (!geoL1List.isEmpty() && geoL1List.get(0) != null && !geoL1List.get(0).isEmpty())
                    ? geoL1List.get(0).toUpperCase()
                    : "";
            String geoL2 = (!geoL2List.isEmpty() && geoL2List.get(0) != null && !geoL2List.get(0).isEmpty())
                    ? geoL2List.get(0).toUpperCase()
                    : "";
            String geoL3 = (!geoL3List.isEmpty() && geoL3List.get(0) != null && !geoL3List.get(0).isEmpty())
                    ? geoL3List.get(0).toUpperCase()
                    : "";
            String geoL4 = (!geoL4List.isEmpty() && geoL4List.get(0) != null && !geoL4List.get(0).isEmpty())
                    ? geoL4List.get(0).toUpperCase()
                    : "";
            String node = (!nodeList.isEmpty() && nodeList.get(0) != null && !nodeList.get(0).isEmpty())
                    ? nodeList.get(0).toUpperCase()
                    : "";
            String mo = (!moList.isEmpty() && moList.get(0) != null && !moList.get(0).isEmpty())
                    ? moList.get(0).toUpperCase()
                    : "";

            String netype = jsonObject.getString("netype");
            netype = netype != null && !netype.isEmpty() ? netype.toUpperCase() : "";

            logger.info("Provided Node & Aggragtaion Details: geoL1={}, geoL2={}, geoL3={}, geoL4={}, node={}, mo={}",
                    geoL1, geoL2, geoL3, geoL4, node, mo);

            nodeAndAggregationDetails.put("geoL1", geoL1);
            nodeAndAggregationDetails.put("geoL2", geoL2);
            nodeAndAggregationDetails.put("geoL3", geoL3);
            nodeAndAggregationDetails.put("geoL4", geoL4);
            nodeAndAggregationDetails.put("node", node);
            nodeAndAggregationDetails.put("mo", mo);
            nodeAndAggregationDetails.put("netype", netype);
            nodeAndAggregationDetails.put("geoL1List", geoL1List.toString());
            nodeAndAggregationDetails.put("geoL2List", geoL2List.toString());
            nodeAndAggregationDetails.put("geoL3List", geoL3List.toString());
            nodeAndAggregationDetails.put("geoL4List", geoL4List.toString());
            nodeAndAggregationDetails.put("nodeList", nodeList.toString());

            String isGeoL1MultiSelect = !geoL1.isEmpty() && !geoL1.contains("CLUBBED") && !geoL1.contains("INDIVIDUAL")
                    && !geoL1.contains("INDIA") && !geoL1.contains("CUSTOM") ? "true" : "false";
            String isGeoL2MultiSelect = !geoL2.isEmpty() && !geoL2.contains("CLUBBED") && !geoL2.contains("INDIVIDUAL")
                    ? "true"
                    : "false";
            String isGeoL3MultiSelect = !geoL3.isEmpty() && !geoL3.contains("CLUBBED") && !geoL3.contains("INDIVIDUAL")
                    ? "true"
                    : "false";
            String isGeoL4MultiSelect = !geoL4.isEmpty() && !geoL4.contains("CLUBBED") && !geoL4.contains("INDIVIDUAL")
                    ? "true"
                    : "false";
            // For Custom Node Case:
            String isNodeMultiSelect = "";
            if (geoL1.contains("Custom")) {
                isNodeMultiSelect = "true";
            } else {
                isNodeMultiSelect = "false";
            }
            // String isMoMultiSelect = !mo.contains("AGGREGATED") &&
            // !mo.contains("INDIVIDUAL") ? "true" : "false";

            nodeAndAggregationDetails.put("isGeoL1MultiSelect", isGeoL1MultiSelect);
            nodeAndAggregationDetails.put("isGeoL2MultiSelect", isGeoL2MultiSelect);
            nodeAndAggregationDetails.put("isGeoL3MultiSelect", isGeoL3MultiSelect);
            nodeAndAggregationDetails.put("isGeoL4MultiSelect", isGeoL4MultiSelect);
            nodeAndAggregationDetails.put("isNodeMultiSelect", isNodeMultiSelect);
            nodeAndAggregationDetails.put("cellsList", cellsList.toString());
            // nodeAndAggregationDetails.put("isMoMultiSelect", isMoMultiSelect);

        } catch (Exception e) {
            logger.error("Error In Getting Node And Aggregation Details, Message: {}, Error: {}", e.getMessage(), e);
        }

        return nodeAndAggregationDetails;

    }

    public static List<String> getStringListFromArray(JSONArray array) {

        if (array == null || array.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> list = new ArrayList<>(array.length());

        for (int i = 0; i < array.length(); i++) {

            try {
                list.add(array.getString(i));
            } catch (Exception e) {
                logger.error("Error Parsing String from JSONArray At index {}, Message: {}, Error: {}", i,
                        e.getMessage(), e);
            }

        }

        return list;
    }

    public static Dataset<Row> removeDuplicates(Dataset<Row> df) {
        String[] allColumns = df.columns();
        Set<String> seen = new LinkedHashSet<>();
        List<String> deduplicatedColumns = java.util.Arrays.stream(allColumns)
                .filter(seen::add)
                .collect(Collectors.toList());
        return df.selectExpr(deduplicatedColumns.toArray(new String[0]));
    }

    public static Map<String, Map<String, String>> getCounterInfoMap(JobContext jobContext) throws SQLException {

        Map<String, Map<String, String>> counterInfoMap = new LinkedHashMap<>();

        String counterInfoMapQuery = getCounterInfoMapQuery(jobContext);

        ResultSet resultSet = executeQueryAndGetResultSet(counterInfoMapQuery, jobContext);

        if (resultSet == null) {
            logger.error("No Data Found In Counter Info Map Query Result Set!");
            return new LinkedHashMap<>();
        }

        counterInfoMap = getCounterInfoMapFromResultSet(resultSet);

        return counterInfoMap;
    }

    private static Map<String, Map<String, String>> getCounterInfoMapFromResultSet(ResultSet resultSet)
            throws SQLException {
        Map<String, Map<String, String>> counterInfoMap = new LinkedHashMap<>();

        while (resultSet.next()) {
            String COUNTER_HEADER_NAME = resultSet.getString(1);
            String PM_COUNTER_VARIABLE_ID_PK = resultSet.getString(2);
            String CATEGORY_NAME = resultSet.getString(3) + "@" + resultSet.getString(14);
            String SUBCATEGORY1_VALUE = resultSet.getString(5);
            String SUBCATEGORY2_VALUE = resultSet.getString(6);
            String SUBCATEGORY3_VALUE = resultSet.getString(7);
            String SUBCATEGORY4_VALUE = resultSet.getString(8);
            String SUBCAT_HEADER1 = resultSet.getString(9);
            String SUBCAT_HEADER2 = resultSet.getString(10);
            String SUBCAT_HEADER3 = resultSet.getString(11);
            String SUBCAT_HEADER4 = resultSet.getString(12);
            String SEQUENCE_NO = resultSet.getString(13);
            String UNIQUE_STRING = resultSet.getString(15);
            String NODE_AGGREGATION = resultSet.getString(16);
            String TIME_AGGREGATION = resultSet.getString(17);

            Map<String, String> infoMap = counterInfoMap.get(COUNTER_HEADER_NAME);
            if (infoMap == null) {
                infoMap = new LinkedHashMap<>();
            }
            infoMap.put("COUNTER_HEADER_NAME", COUNTER_HEADER_NAME);
            infoMap.put("PM_COUNTER_VARIABLE_ID_PK", PM_COUNTER_VARIABLE_ID_PK);
            infoMap.put("CATEGORY_NAME", CATEGORY_NAME);
            infoMap.put("SEQUENCE_NO", SEQUENCE_NO);
            infoMap.put("UNIQUE_STRING", UNIQUE_STRING);
            infoMap.put("NODE_AGGREGATION", NODE_AGGREGATION);
            infoMap.put("TIME_AGGREGATION", TIME_AGGREGATION);
            prepareinfoMap(SUBCATEGORY1_VALUE, SUBCAT_HEADER1, infoMap, "SUB_CATEGORY_HEADER1", "SUB_CATEGORY_VALUE1");
            prepareinfoMap(SUBCATEGORY2_VALUE, SUBCAT_HEADER2, infoMap, "SUB_CATEGORY_HEADER2", "SUB_CATEGORY_VALUE2");
            prepareinfoMap(SUBCATEGORY3_VALUE, SUBCAT_HEADER3, infoMap, "SUB_CATEGORY_HEADER3", "SUB_CATEGORY_VALUE3");
            prepareinfoMap(SUBCATEGORY4_VALUE, SUBCAT_HEADER4, infoMap, "SUB_CATEGORY_HEADER4", "SUB_CATEGORY_VALUE4");

            String COUNTER_INFO_MAP_KEY = "C" + SEQUENCE_NO + "#" + PM_COUNTER_VARIABLE_ID_PK;
            counterInfoMap.put(COUNTER_INFO_MAP_KEY, infoMap);
        }
        return counterInfoMap;
    }

    private static void prepareinfoMap(String subCategoryValue, String subCategoryHeader, Map<String, String> infoMap,
            String headerColumn, String valueColumn) {
        if (subCategoryValue != null && !subCategoryValue.equalsIgnoreCase("null")
                && !subCategoryValue.contains("INDIVIDUAL") && !subCategoryValue.contains("AGGREGATED")) {
            infoMap.put(headerColumn, subCategoryHeader);
            if (infoMap.get(valueColumn) != null && !infoMap.get(valueColumn).equalsIgnoreCase("null")) {
                subCategoryValue = infoMap.get(valueColumn) + "','" + subCategoryValue;
                infoMap.put(valueColumn, subCategoryValue);
            } else {
                infoMap.put(valueColumn, subCategoryValue);
            }
        }
    }

    private static ResultSet executeQueryAndGetResultSet(String query, JobContext jobContext) {

        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

        logger.info(
                "Executing Query: {} With Inputs: SPARK_PM_JDBC_DRIVER={}, SPARK_PM_JDBC_URL={}, SPARK_PM_JDBC_USERNAME={}, SPARK_PM_JDBC_PASSWORD={}",
                query, SPARK_PM_JDBC_DRIVER, SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

        try {
            Class.forName(SPARK_PM_JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                    SPARK_PM_JDBC_PASSWORD);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            logger.info("Query Executed Successfully!");
            return resultSet;
        } catch (Exception e) {
            logger.error("Error In Getting Connection, Message: {}, Error: {}", e.getMessage(), e);
        }
        return null;
    }

    private static String getCounterInfoMapQuery(JobContext jobContext) {

        String counterInfoMapQuery = "SELECT DISTINCT UPPER(REPLACE(cv.COUNTER, ' ', '')) AS COUNTER_HEADER_NAME, cv.PM_COUNTER_VARIABLE_ID_PK AS PM_COUNTER_VARIABLE_ID_PK, UPPER(REPLACE(kc.CATEGORY_ALIAS_NAME, ' ', '')) AS CATEGORY_NAME, cv.ATTRIBUTE AS ATTRIBUTE, cv.SUBCATEGORY1_VALUE AS SUBCATEGORY1_VALUE, cv.SUBCATEGORY2_VALUE AS SUBCATEGORY2_VALUE, cv.SUBCATEGORY3_VALUE AS SUBCATEGORY3_VALUE, cv.SUBCATEGORY4_VALUE AS SUBCATEGORY4_VALUE, CONCAT('C', subcat1.SEQUENCE_NO) AS SUBCAT_HEADER1, CONCAT('C', subcat2.SEQUENCE_NO) AS SUBCAT_HEADER2, CONCAT('C', subcat3.SEQUENCE_NO) AS SUBCAT_HEADER3, CONCAT('C', subcat4.SEQUENCE_NO) AS SUBCAT_HEADER4, kc.SEQUENCE_NO, UPPER(REPLACE(pc.PM_CATEGORY_ID_PK, ' ', '')) AS CATEGORY_ID, cv.UNIQUE_STRING, cv.NODE_AGGREGATION AS NODE_AGGREGATION, cv.TIME_AGGREGATION AS TIME_AGGREGATION FROM KPI_FORMULA kpi INNER JOIN FORMULA_COUNTER_MAPPING map ON kpi.KPI_FORMULA_ID_PK = map.KPI_FORMULA_ID_FK INNER JOIN PM_COUNTER_VARIABLE cv ON cv.PM_COUNTER_VARIABLE_ID_PK = map.PM_COUNTER_VARIABLE_ID_FK INNER JOIN KPI_COUNTER kc ON UPPER(kc.KPI_COUNTER_ID_PK) = UPPER(cv.KPI_COUNTER_ID_FK) INNER JOIN PM_CATEGORY pc ON pc.PM_CATEGORY_ID_PK = cv.PM_CATEGORY_ID_FK LEFT JOIN KPI_COUNTER subcat1 ON subcat1.KPI_COUNTER_ID_PK = cv.KPI_COUNTER1_ID_FK LEFT JOIN KPI_COUNTER subcat2 ON subcat2.KPI_COUNTER_ID_PK = cv.KPI_COUNTER2_ID_FK LEFT JOIN KPI_COUNTER subcat3 ON subcat3.KPI_COUNTER_ID_PK = cv.KPI_COUNTER3_ID_FK LEFT JOIN KPI_COUNTER subcat4 ON subcat4.KPI_COUNTER_ID_PK = cv.KPI_COUNTER4_ID_FK WHERE kpi.DOMAIN = '$DOMAIN' AND kpi.VENDOR = '$VENDOR' AND kpi.TECHNOLOGY = '$TECHNOLOGY'";

        counterInfoMapQuery = counterInfoMapQuery.replace("$DOMAIN", jobContext.getParameter("DOMAIN"))
                .replace("$VENDOR", jobContext.getParameter("VENDOR"))
                .replace("$TECHNOLOGY", jobContext.getParameter("TECHNOLOGY"));

        if (jobContext.getParameter("KPI_CODES") != null && !jobContext.getParameter("KPI_CODES").isEmpty()) {
            counterInfoMapQuery = counterInfoMapQuery + " AND kpi.KPI_CODE IN ($KPI_CODES)";
            counterInfoMapQuery = counterInfoMapQuery.replace("$KPI_CODES", jobContext.getParameter("KPI_CODES"));
        }

        return counterInfoMapQuery;
    }

    private static String getKpiFormulaQuery(JobContext jobContext) {

        String kpiFormulaQuery = "SELECT DISTINCT CONCAT(kf.KPI_CODE, '##', kf.KPI_FORMULA_DESC) AS KPI_CODE_FORMULA, CAST(COALESCE(CONCAT('C', kc.SEQUENCE_NO, '#', pmc.PM_COUNTER_VARIABLE_ID_PK), 'null') AS BINARY) AS BINARY_VALUE, pmc.UNIQUE_STRING AS UNIQUE_STRING FROM KPI_FORMULA kf LEFT JOIN ( FORMULA_COUNTER_MAPPING fcm JOIN PM_COUNTER_VARIABLE pmc ON fcm.PM_COUNTER_VARIABLE_ID_FK = pmc.PM_COUNTER_VARIABLE_ID_PK ) ON fcm.KPI_FORMULA_ID_FK = kf.KPI_FORMULA_ID_PK LEFT JOIN GENERIC_KPI_MAPPING gkm ON kf.KPI_FORMULA_ID_PK = gkm.KPI_FORMULA_ID_FK LEFT JOIN PM_GENERIC_KPI gk ON gkm.PM_GENERIC_KPI_ID_FK = gk.PM_GENERIC_KPI_ID_PK LEFT JOIN KPI_COUNTER kc ON kc.KPI_COUNTER_ID_PK = pmc.KPI_COUNTER_ID_FK WHERE kf.DOMAIN = '$DOMAIN' AND kf.VENDOR = '$VENDOR' AND kf.KPI_CODE IN ($KPI_CODES) AND kf.DELETED = 0";

        kpiFormulaQuery = kpiFormulaQuery.replace("$DOMAIN", jobContext.getParameter("DOMAIN"))
                .replace("$VENDOR", jobContext.getParameter("VENDOR"))
                .replace("$KPI_CODES", jobContext.getParameter("KPI_CODES"));

        return kpiFormulaQuery;

    }

    private static Map<String, Map<String, String>> getKpiFormulaMap(String kpiCodes, JobContext jobContext) {

        Map<String, Map<String, String>> kpiFormulaFinalMap = new LinkedHashMap<>();

        logger.info("Getting KPI Formula Map With KPI Codes={}", kpiCodes);
        try {

            String kpiFormulaQuery = getKpiFormulaQuery(jobContext);
            ResultSet resultSet = executeQueryAndGetResultSet(kpiFormulaQuery, jobContext);

            while (resultSet.next()) {

                String kpiCodeFormula = resultSet.getString(1);
                String binaryValue = resultSet.getString(2);
                String uniqueString = resultSet.getString(3);

                String kpiCode = kpiCodeFormula.split("##")[0];
                String kpiDesc = kpiCodeFormula.split("##")[1];

                String key = kpiCode + "##" + kpiDesc;

                if (!kpiFormulaFinalMap.isEmpty() && kpiFormulaFinalMap.containsKey(key)) {
                    Map<String, String> counterUniqueStringMap = kpiFormulaFinalMap.get(key);
                    counterUniqueStringMap.put(binaryValue, uniqueString);
                    kpiFormulaFinalMap.put(key, counterUniqueStringMap);
                } else {
                    Map<String, String> counterUniqueStringMap = new LinkedHashMap<>();
                    counterUniqueStringMap.put(binaryValue, uniqueString);
                    kpiFormulaFinalMap.put(key, counterUniqueStringMap);
                }
            }

            return kpiFormulaFinalMap;

        } catch (Exception e) {
            logger.error("Exception While Fetching KPI Formula Map, Message={}, Error={}", e.getMessage(), e);
        }

        return kpiFormulaFinalMap;
    }
}
