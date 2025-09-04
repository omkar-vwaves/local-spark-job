package com.enttribe.pm.job.alert.loop;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenAlertExtractConf extends Processor {
    private static Logger logger = LoggerFactory.getLogger(OpenAlertExtractConf.class);

    public OpenAlertExtractConf() {
    }

    public OpenAlertExtractConf(Dataset<Row> dataFrame, int id, String processName) {
        super(id, processName);
    }

    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        if (this.dataFrame == null || this.dataFrame.isEmpty()) {
            jobContext.setParameters("FINAL_COUNT", "0");
            jobContext.setParameters("CURRENT_COUNT", "0");
            return this.dataFrame;
        }
        this.dataFrame.show();
        logger.info("++++++++++++[JOIN RULE AND LIBRARY]++++++++++++");

        List<Row> ruleList = this.dataFrame.collectAsList();

        String size = String.valueOf(ruleList.size());
        jobContext.setParameters("FINAL_COUNT", size);
        jobContext.setParameters("CURRENT_COUNT", "0");

        logger.info("CURRENT_COUNT={}", "0");
        logger.info("FINAL_COUNT={}", size);

        int finalCount = 0;
        for (Row row : ruleList) {
            Map<String, String> inputMap = new HashMap<>();
            inputMap.put("EXPRESSION", getStringSafe(row, "EXPRESSION"));
            inputMap.put("DESCRIPTION", getStringSafe(row, "DESCRIPTION"));
            inputMap.put("ALARM_IDENTIFIER", getStringSafe(row, "ALARM_IDENTIFIER"));
            inputMap.put("ALARM_NAME", getStringSafe(row, "ALARM_NAME"));
            inputMap.put("CLASSIFICATION", getStringSafe(row, "CLASSIFICATION"));
            inputMap.put("NETYPE", getStringSafe(row, "NETYPE"));
            inputMap.put("DEFAULT_SEVERITY", getStringSafe(row, "DEFAULT_SEVERITY"));
            inputMap.put("EMS_TYPE", getStringSafe(row, "EMS_TYPE"));
            inputMap.put("ALARM_ID", getStringSafe(row, "ALARM_ID"));
            inputMap.put("SERVICE_AFFECTING", getStringSafe(row, "SERVICE_AFFECTING"));
            inputMap.put("PROBABLE_CAUSE", getStringSafe(row, "PROBABLE_CAUSE"));
            inputMap.put("MANUAL_CLEARED", getStringSafe(row, "MANUAL_CLEARED"));
            inputMap.put("EVENT_TYPE", getStringSafe(row, "EVENT_TYPE"));
            inputMap.put("DOMAIN", getStringSafe(row, "DOMAIN"));
            inputMap.put("VENDOR", getStringSafe(row, "VENDOR"));
            inputMap.put("TECHNOLOGY", getStringSafe(row, "TECHNOLOGY"));
            inputMap.put("CORRELATION_ENABLE", getStringSafe(row, "CORRELATION_ENABLE"));
            inputMap.put("ALARM_GROUP", getStringSafe(row, "ALARM_GROUP"));
            inputMap.put("CONFIGURATION", getStringSafe(row, "CONFIGURATION"));

            logger.info("Configuration Map [Index={}]=====> {}", finalCount, inputMap);

            Map<String, String> extractedParametersMap = extractParametersFromConfiguration(inputMap, jobContext);
            logger.info("Extracted Parameters Map [Index={}]=====> {}", finalCount, extractedParametersMap);

            Map<String, String> nodeAndAggregationDetailsMap = getNodeAndAggregationDetails(inputMap);
            logger.info("Node and Aggregation Details Map [Index={}] =====> {}", finalCount,
                    nodeAndAggregationDetailsMap);

            Map<String, String> kpiCodeNameMap = getKpiCodeNameMap(inputMap);
            logger.info("KPI Code Name Map [Index={}]=====> {}", finalCount, kpiCodeNameMap);

            String inputConfig = new ObjectMapper().writeValueAsString(inputMap);
            String extractedParameters = new ObjectMapper().writeValueAsString(extractedParametersMap);
            String nodeAndAggregationDetails = new ObjectMapper().writeValueAsString(nodeAndAggregationDetailsMap);
            String kpiCodeNameMapJson = new ObjectMapper().writeValueAsString(kpiCodeNameMap);

            jobContext.setParameters("INPUT_CONFIGURATIONS" + finalCount, inputConfig);
            jobContext.setParameters("EXTRACTED_PARAMETERS" + finalCount, extractedParameters);
            jobContext.setParameters("NODE_AND_AGGREGATION_DETAILS" + finalCount, nodeAndAggregationDetails);
            jobContext.setParameters("KPI_CODE_NAME_MAP" + finalCount, kpiCodeNameMapJson);
            finalCount++;
        }
        return this.dataFrame;
    }

    private static Map<String, String> getKpiCodeNameMap(Map<String, String> inputMap) {

        String configuration = inputMap.get("CONFIGURATION");
        configuration = configuration.replace("\"", "");

        JSONObject configJson = new JSONObject(configuration);

        JSONArray kpiArray = configJson.getJSONArray("kpi");

        Map<String, String> kpiCodeNameMap = new LinkedHashMap<>();

        for (int i = 0; i < kpiArray.length(); i++) {
            JSONObject kpiObj = kpiArray.getJSONObject(i);
            String kpiName = kpiObj.optString("kpiName");
            String kpiId = kpiName.split("-")[0];
            kpiCodeNameMap.put(kpiId, kpiName);
        }

        return kpiCodeNameMap;
    }

    private static Map<String, String> getNodeAndAggregationDetails(Map<String, String> inputMap) {

        Map<String, String> nodeAndAggregationDetails = new LinkedHashMap<>();

        try {
            String configRaw = inputMap.get("CONFIGURATION");
            String fixedJson = configRaw == null ? "{}" : configRaw.replace("'", "\"").replaceAll("^\"|\"$", "");

            logger.info("fixedJson : {}", fixedJson);

            JSONObject jsonObject;
            try {
                jsonObject = new JSONObject(fixedJson);
            } catch (Exception e) {
                logger.error("Invalid JSON in CONFIGURATION: {}", e.getMessage());
                jsonObject = new JSONObject();
            }

            JSONArray geoL1JSONArray = jsonObject.has("geography_l1") ? jsonObject.optJSONArray("geography_l1")
                    : new JSONArray();
            JSONArray geoL2JSONArray = jsonObject.has("geography_l2") ? jsonObject.optJSONArray("geography_l2")
                    : new JSONArray();
            JSONArray geoL3JSONArray = jsonObject.has("geography_l3") ? jsonObject.optJSONArray("geography_l3")
                    : new JSONArray();
            JSONArray geoL4JSONArray = jsonObject.has("geography_l4") ? jsonObject.optJSONArray("geography_l4")
                    : new JSONArray();
            JSONArray nodeArray = jsonObject.has("node") ? jsonObject.optJSONArray("node") : new JSONArray();

            List<String> geoL1List = geoL1JSONArray != null ? getStringListFromArray(geoL1JSONArray)
                    : new ArrayList<>();
            List<String> geoL2List = geoL2JSONArray != null ? getStringListFromArray(geoL2JSONArray)
                    : new ArrayList<>();
            List<String> geoL3List = geoL3JSONArray != null ? getStringListFromArray(geoL3JSONArray)
                    : new ArrayList<>();
            List<String> geoL4List = geoL4JSONArray != null ? getStringListFromArray(geoL4JSONArray)
                    : new ArrayList<>();
            List<String> nodeList = nodeArray != null ? getStringListFromArray(nodeArray) : new ArrayList<>();

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

            String netype = "";
            try {
                netype = jsonObject.has("netype") ? jsonObject.getString("netype") : "";
                netype = netype != null && !netype.isEmpty() ? netype.toUpperCase() : "";
            } catch (Exception e) {
                logger.warn("netype not found or invalid in CONFIGURATION: {}", e.getMessage());
                netype = "";
            }

            logger.info("Provided Node & Aggregation Details: geoL1={}, geoL2={}, geoL3={}, geoL4={}, node={}",
                    geoL1, geoL2, geoL3, geoL4, node);

            nodeAndAggregationDetails.put("geoL1", geoL1);
            nodeAndAggregationDetails.put("geoL2", geoL2);
            nodeAndAggregationDetails.put("geoL3", geoL3);
            nodeAndAggregationDetails.put("geoL4", geoL4);
            nodeAndAggregationDetails.put("node", node);
            nodeAndAggregationDetails.put("netype", netype);
            nodeAndAggregationDetails.put("geoL1List", geoL1List.toString());
            nodeAndAggregationDetails.put("geoL2List", geoL2List.toString());
            nodeAndAggregationDetails.put("geoL3List", geoL3List.toString());
            nodeAndAggregationDetails.put("geoL4List", geoL4List.toString());
            nodeAndAggregationDetails.put("nodeList", nodeList.toString());

            String isGeoL1MultiSelect = (!geoL1.contains("CLUBBED") && !geoL1.contains("INDIVIDUAL")
                    && !geoL1.contains("India") && !geoL1.isEmpty()) ? "true" : "false";
            String isGeoL2MultiSelect = (!geoL2.contains("CLUBBED") && !geoL2.contains("INDIVIDUAL") && geoL2.isEmpty())
                    ? "true"
                    : "false";
            String isGeoL3MultiSelect = (!geoL3.contains("CLUBBED") && !geoL3.contains("INDIVIDUAL") && geoL3.isEmpty())
                    ? "true"
                    : "false";
            String isGeoL4MultiSelect = (!geoL4.contains("CLUBBED") && !geoL4.contains("INDIVIDUAL") && geoL4.isEmpty())
                    ? "true"
                    : "false";
            String isNodeMultiSelect = (!node.contains("CLUBBED") && !node.contains("INDIVIDUAL")) ? "true" : "false";

            nodeAndAggregationDetails.put("isGeoL1MultiSelect", isGeoL1MultiSelect);
            nodeAndAggregationDetails.put("isGeoL2MultiSelect", isGeoL2MultiSelect);
            nodeAndAggregationDetails.put("isGeoL3MultiSelect", isGeoL3MultiSelect);
            nodeAndAggregationDetails.put("isGeoL4MultiSelect", isGeoL4MultiSelect);
            nodeAndAggregationDetails.put("isNodeMultiSelect", isNodeMultiSelect);

        } catch (Exception e) {
            logger.error("Error in getting Node and Aggregation Details, Message: {}, Error: {}", e.getMessage(), e);
            // Never throw, always return a map (possibly empty or with partial data)
        }

        return nodeAndAggregationDetails;

    }

    private static Map<String, String> extractParametersFromConfiguration(Map<String, String> inputMap,
            JobContext jobContext) {

        try {

            String configuration = inputMap.get("CONFIGURATION");
            configuration = configuration.replace("\"", "");

            Map<String, String> parameters = new LinkedHashMap<>();

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
                severity = configJson.getString("priority");
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

            List<String> cellList = getStringListFromArray(cellArray);
            List<String> geoL1List = getStringListFromArray(geoL1Array);
            List<String> geoL2List = getStringListFromArray(geoL2Array);
            List<String> geoL3List = getStringListFromArray(geoL3Array);
            List<String> geoL4List = getStringListFromArray(geoL4Array);

            String level = getLevelForReport(geoL1List, geoL2List, geoL3List, geoL4List, cellList, node,
                    geoL1List, inputMap.get("DOMAIN"));

            String isNodeLevel = "false";
            if (!level.contains("L0") && !level.contains("L1") && !level.contains("L2")
                    && !level.contains("L3") && !level.contains("L4")) {

                if (geoL1List.get(0).toUpperCase().equalsIgnoreCase("INDIA")) {
                    isNodeLevel = "false";
                } else {
                    isNodeLevel = "true";
                }

            }

            // String dataLevelAppender = jobContext.getParameter("ROW_KEY_APPENDER");
            String DOMAIN = inputMap.get("DOMAIN");
            String VENDOR = inputMap.get("VENDOR");
            String TECHNOLOGY = inputMap.get("TECHNOLOGY");

            StringBuilder mysqlQuery = new StringBuilder()
                    .append("SELECT DISTINCT CONCAT(TECHNOLOGY, IF(ROWKEY_TECHNOLOGY IS NOT NULL, '_', ''), ")
                    .append("COALESCE(NETWORK_TYPE, '')) AS rowKeyAppender ")
                    .append("FROM PM_NODE_VENDOR ")
                    .append("WHERE domain = '").append(DOMAIN).append("' ")
                    .append("AND vendor = '").append(VENDOR).append("' ")
                    .append("AND technology = '").append(TECHNOLOGY).append("'");

            logger.info("MySQL Query For Data Level Appender: {}", mysqlQuery.toString());

            Dataset<Row> df = executeQuery(mysqlQuery.toString(), jobContext);

            List<String> dataLevelAppenders = df.as(Encoders.STRING()).collectAsList();
            String dataLevelAppender = dataLevelAppenders.isEmpty() ? "" : dataLevelAppenders.get(0);

            logger.info("Data Level Appender: {}", dataLevelAppender);

            String datalevel = "";

            switch (level) {
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
                    datalevel = level + "_" + dataLevelAppender;
                    break;
                }
            }

            logger.info("Generated DataLevel is : {}", datalevel);

            parameters.put("instances", instances);
            parameters.put("outOfLast", outOfLast);
            parameters.put("datalevel", datalevel);
            parameters.put("level", level);
            parameters.put("geoL1List", geoL1List.toString());
            parameters.put("geoL2List", geoL2List.toString());
            parameters.put("geoL3List", geoL3List.toString());
            parameters.put("geoL4List", geoL4List.toString());
            parameters.put("CLASSIFICATION", classification);
            parameters.put("SEVERITY", severity);
            parameters.put("ACTUAL_SEVERITY", inputMap.get("DEFAULT_SEVERITY"));
            parameters.put("SUBENTITY", level);

            parameters.put("DOMAIN", inputMap.get("DOMAIN").toUpperCase());
            parameters.put("VENDOR", inputMap.get("VENDOR").toUpperCase());
            parameters.put("TECHNOLOGY", inputMap.get("TECHNOLOGY").toUpperCase());

            parameters.put("TIMESTAMP", jobContext.getParameter("TIMESTAMP"));
            parameters.put("isNodeLevel", isNodeLevel);

            parameters.put("EXPRESSION", inputMap.get("EXPRESSION"));
            parameters.put("ALARM_EXTERNAL_ID", inputMap.get("ALARM_IDENTIFIER"));
            parameters.put("ALARM_CODE", inputMap.get("ALARM_ID"));
            parameters.put("ALARM_NAME", inputMap.get("ALARM_NAME"));

            parameters.put("PROBABLE_CAUSE", inputMap.get("PROBABLE_CAUSE"));
            parameters.put("DESCRIPTION", inputMap.get("DESCRIPTION"));
            parameters.put("ALARM_GROUP", inputMap.get("ALARM_GROUP"));
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
            parameters.put("EVENT_TYPE", inputMap.get("EVENT_TYPE"));
            parameters.put("SENDER_NAME", inputMap.get("EMS_TYPE"));

            String cqlTableName = generateCQLTableNameBasedOnFrequency(jobContext);
            parameters.put("cqlTableName", cqlTableName);

            String expression = inputMap.get("EXPRESSION");
            List<String> kpiCodeList = getKPICodeListFromExpression(expression);

            parameters.put("kpiCodeList", kpiCodeList.toString());

            return parameters;

        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static Dataset<Row> executeQuery(String sqlQuery, JobContext jobContext) {

        Dataset<Row> resultDataset = null;
        String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
        String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
        String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
        String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

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

    public static String getLevelForReport(List<String> geoL1List, List<String> geoL2List, List<String> geoL3List,
            List<String> geoL4List, List<String> cells, String node,
            List<String> coreDomains, String DOMAIN) {

        boolean isClubbed = node != null && node.toUpperCase().contains("CLUBBED");

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

    private static String generateCQLTableNameBasedOnFrequency(JobContext jobContext) {

        String jobFrequency = jobContext.getParameter("FREQUENCY");

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

    private String getStringSafe(Row row, String column) {
        try {
            Object value = row.getAs(column);
            return (value != null) ? value.toString().trim() : "";
        } catch (Exception e) {
            return "";
        }
    }
}