package com.enttribe.pm.job.report.otf;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.joda.time.LocalDateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.lang.DateUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.gson.Gson;

import gnu.trove.map.hash.THashMap;

public class OTFReportConfigParser
        implements UDF9<String, String, String, String, String, String, String, String, String, Row>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(OTFReportConfigParser.class);
    public JobContext jobcontext;

    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";

    private static String SPARK_PLATFORM_JDBC_DRIVER = "SPARK_PLATFORM_JDBC_DRIVER";
    private static String SPARK_PLATFORM_JDBC_URL = "SPARK_PLATFORM_JDBC_URL";
    private static String SPARK_PLATFORM_JDBC_USERNAME = "SPARK_PLATFORM_JDBC_USERNAME";
    private static String SPARK_PLATFORM_JDBC_PASSWORD = "SPARK_PLATFORM_JDBC_PASSWORD";

    private static boolean isLocal = false;

    @Override
    public Row call(String reportWidgetIdPk, String domain, String vendor, String reportName,
            String reportConfiguration, String geography, String reportLevel, String reportType, String technology)
            throws Exception {

        Map<String, String> jobContextMap = jobcontext.getParameters();
        SPARK_PM_JDBC_DRIVER = jobContextMap.get("SPARK_PM_JDBC_DRIVER");
        SPARK_PM_JDBC_URL = jobContextMap.get("SPARK_PM_JDBC_URL");
        SPARK_PM_JDBC_USERNAME = jobContextMap.get("SPARK_PM_JDBC_USERNAME");
        SPARK_PM_JDBC_PASSWORD = jobContextMap.get("SPARK_PM_JDBC_PASSWORD");

        SPARK_PLATFORM_JDBC_DRIVER = jobContextMap.get("SPARK_PLATFORM_JDBC_DRIVER");
        SPARK_PLATFORM_JDBC_URL = jobContextMap.get("SPARK_PLATFORM_JDBC_URL");
        SPARK_PLATFORM_JDBC_USERNAME = jobContextMap.get("SPARK_PLATFORM_JDBC_USERNAME");
        SPARK_PLATFORM_JDBC_PASSWORD = jobContextMap.get("SPARK_PLATFORM_JDBC_PASSWORD");

        logger.info(
                "\"PERFORMANCE\" Crendetials: SPARK_PM_JDBC_DRIVER={}, SPARK_PM_JDBC_URL={}, SPARK_PM_JDBC_USERNAME={}, SPARK_PM_JDBC_PASSWORD={}",
                SPARK_PM_JDBC_DRIVER, SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

        logger.info(
                "\"PLATFORM\" Crendetials: SPARK_PLATFORM_JDBC_DRIVER={}, SPARK_PLATFORM_JDBC_URL={}, SPARK_PLATFORM_JDBC_USERNAME={}, SPARK_PLATFORM_JDBC_PASSWORD={}",
                SPARK_PLATFORM_JDBC_DRIVER, SPARK_PLATFORM_JDBC_URL, SPARK_PLATFORM_JDBC_USERNAME,
                SPARK_PLATFORM_JDBC_PASSWORD);

        logger.info(
                "OTFReportConfigParser UDF Input: domain={}, vendor={}, reportName={}, geography={}, reportLevel={}, reportType={}, technology={}",
                domain, vendor, reportName, geography, reportLevel, reportType, technology);

        JSONObject configJson = new JSONObject(reportConfiguration);
        logger.info("Parsed ExtractPMReportConfiguration JSON: {}", configJson.toString());

        List<String> l1List = getListFromJsonArray(getJsonArray(configJson, "geography_l1"));
        List<String> l2List = getListFromJsonArray(getJsonArray(configJson, "geography_l2"));
        List<String> l3List = getListFromJsonArray(getJsonArray(configJson, "geography_l3"));
        List<String> l4List = getListFromJsonArray(getJsonArray(configJson, "geography_l4"));
        List<String> cells = getListFromJsonArray(getJsonArray(configJson, "cells"));

        JSONArray nodeJsonArray = getJsonArray(configJson, "node");
        String nodeString = getStringValueFromJsonArray(nodeJsonArray);
        if (nodeString.contains("Site")) {
            nodeString = nodeString.replace("Site", "EnodeB");
        }

        if (nodeString.toUpperCase().contains("INDIVIDUAL")) {
            jobcontext.setParameters("NODE_LEVEL", "NODE_LEVEL");
        } else {
            jobcontext.setParameters("NODE_LEVEL", "NODE_LEVEL_FALSE");
        }

        logger.info("Geography Levels: L1={}, L2={}, L3={}, L4={}, Node={}", l1List, l2List, l3List, l4List,
                nodeString);
        // L1=[All GEOGRAPHYL1 - Clubbed], L2=[All GEOGRAPHYL2 - Clubbed],
        // L3=[All GEOGRAPHYL3 - Clubbed], L4=[All GEOGRAPHYL4 - Clubbed]

        JSONArray otherGeographyJsonArray = getJsonArray(configJson, "otherGeography");
        List<String> otherGeographyIdList = getListFromJsonArray(otherGeographyJsonArray);
        String otherGeographyIds = (!otherGeographyIdList.isEmpty()
                && !otherGeographyIdList.contains(PMConstants.BLANK))
                        ? String.join(PMConstants.COMMA + "P", otherGeographyIdList)
                        : null;
        otherGeographyIds = "P" + otherGeographyIds;

        String geography_l1 = (!l1List.isEmpty() && !l1List.contains(PMConstants.BLANK))
                ? String.join(PMConstants.COMMA, l1List)
                : null;

        String cell = String.join(Symbol.COMMA_STRING, cells);
        jobcontext.setParameters("cellList", cell);
        jobcontext.setParameters("otherGeographyIds", otherGeographyIds);
        jobcontext.setParameters("geography_l1", geography_l1);

        logger.info("Geography L1={}, Cell={}, Other Geography IDs={}, Set to JobContext Successfully!", geography_l1,
                cell, otherGeographyIds);

        String level = getLevelForReport(l1List, l2List, l3List, l4List, cells, nodeString);

        String node = getNodeName(nodeString);
        String nodeVal = getNodeVal(nodeString);
        String metaColumns = getValueFromJson(configJson, "metaColumns");

        // String[] keyArray = metaColumns.split("#")[0].split(",");
        // String[] valueArray = metaColumns.split("#")[1].split(",");

        // String keyString = "";
        // String valueString = "";

        // for (int i = 0; i < keyArray.length; i++) {
        // if (!(keyArray[i].contains("DT") || keyArray[i].contains("HR"))) {
        // keyString = keyString + keyArray[i] + ",";
        // valueString = valueString + valueArray[i] + ",";
        // }else{

        // }
        // }
        // // Remove trailing commas if present
        // if (keyString.endsWith(",")) {
        // keyString = keyString.substring(0, keyString.length() - 1);
        // }
        // if (valueString.endsWith(",")) {
        // valueString = valueString.substring(0, valueString.length() - 1);
        // }
        // metaColumns = keyString + "#" + valueString;

        logger.info("Report Level={}, Node={}, NodeVal={}, Input MetaColumns={}", level, node, nodeVal, metaColumns);

        String childNode = !StringUtils.substringAfterLast(node, Symbol.HYPHEN_STRING).isEmpty()
                ? StringUtils.substringAfterLast(node, Symbol.HYPHEN_STRING)
                : null;
        logger.info("Child Node={}", childNode); // null

        String pmNodeConfgnByNEType = getSystemConfigurationValueByName("PM_NODE_BY_NETYPE");

        List<String> coreDomains = getValueListFromJsonByKey(pmNodeConfgnByNEType, CORE_DOMAINS);
        coreDomains.add("CORE");
        logger.info("Core Domains={}", coreDomains); // [CORE]

        String neType = getNetypeForCustomCells(domain, vendor, technology);
        jobcontext.setParameters("netype", neType);
        logger.info("Received NE Type: {}", neType); // INTERFACE,ROUTER,SWITCH

        updateParamaterInJobContext(configJson, reportName, geography, level, reportType, metaColumns, domain, vendor,
                technology, nodeVal, node, coreDomains);

        List<String> parentNodes = getValueListFromJsonByKey(pmNodeConfgnByNEType, "PARENT_NODE");
        List<String> baseNodes = getValueListFromJsonByKey(pmNodeConfgnByNEType, "BASE_NODE");

        logger.info("Parent Nodes={}", parentNodes);
        // [VCU, ODSC_VCU, IDSC_VCU, FEMTO_VCU, RIUD_VCU]
        logger.info("Base Nodes={}", baseNodes);
        // [CELL, Small cell, SMALL_CELL, ODSC, IDSC, FEMTO, DAS, GNB_CELL, RIUD]

        String parentNode = !parentNodes.isEmpty() ? String.join(",", parentNodes) : null;
        String baseNode = !baseNodes.isEmpty() ? String.join(",", baseNodes) : null;

        logger.info("Parent Node={}", parentNode);
        // VCU,ODSC_VCU,IDSC_VCU,FEMTO_VCU,RIUD_VCU
        logger.info("Base Node={}", baseNode);
        // CELL,Small cell,SMALL_CELL,ODSC,IDSC,FEMTO,DAS,GNB_CELL,RIUD

        jobcontext.setParameters("PARENT_NODE", parentNode);
        jobcontext.setParameters("BASE_NODE", baseNode);
        jobcontext.setParameters("childNode", childNode);
        jobcontext.setParameters("nodeVal", nodeVal);

        logger.info("Child Node={} | Node Val={} | Set to Job Context Successfully!", childNode, nodeVal);

        String neTypes = getNETypeList(domain, vendor, node, pmNodeConfgnByNEType, technology);
        logger.info("NE Types={}", neTypes);
        // 'INTERFACE','ROUTER','SWITCH'

        String panGeographyName = getSystemConfigurationValueByName("GeographyL0");

        String queryForGeoDetails = getDynamicQueryForReportConfig(reportConfiguration, domain, vendor, level, neTypes,
                pmNodeConfgnByNEType, panGeographyName, reportType, reportWidgetIdPk, technology);

        jobcontext.setParameters("dynamicQuery", queryForGeoDetails);
        jobcontext.setParameters("domain", domain);
        jobcontext.setParameters("vendor", vendor);
        jobcontext.setParameters("GeographyL0", panGeographyName);

        logger.info("Dynamic Query={}, Domain={}, Vendor={}", queryForGeoDetails, domain, vendor);

        Map<String, List<String>> listOfGeography = getQueryForGeographyLevels(reportConfiguration, domain, vendor,
                panGeographyName, l1List, l2List, l3List, l4List, nodeString);
        jobcontext.setParameters("listOfGeography", new Gson().toJson(listOfGeography));
        logger.info("List Of Geography={}", listOfGeography);

        String trendOTFReport = jobcontext.getParameter("trendOTFReport");
        String moColumns = jobcontext.getParameter("moColumns");

        logger.info("Trend OTF Report={}, MO Columns={}", trendOTFReport, moColumns);

        if (trendOTFReport.equalsIgnoreCase("true")) {
            logger.info("Trend OTF Report is Enabled!");

            String queryForDataFrameChange = getQueryForDataFrameChange(metaColumns, configJson, moColumns);
            String queryForDataFrameNBHChange = getQueryForDataFrameNBHChange(metaColumns, configJson, moColumns);
            jobcontext.setParameters("queryForDataFrameChange", queryForDataFrameChange);
            jobcontext.setParameters("queryForDataFrameNBHChange", queryForDataFrameNBHChange);
        } else {
            logger.info("Trend OTF Report is Disabled!");
        }

        String frequency = getStringValueFromJsonArray(getJsonArray(configJson, "frequency"));
        jobcontext.setParameters("frequency", frequency);
        logger.info("Frequency='{}' Set To JobContext Successfully!", frequency);

        HashMap<String, String> contextMap = jobcontext.getParameters();
        getMagnetParameterConfig(jobcontext, contextMap);
        jobcontext.setParameters("technology", technology);

        reportName = reportName.replaceAll(" ", "_");
        // jobcontext.getParameters().forEach((key, value) -> {
        // com.enttribe.pm.job.report.otf.App.jobContext.setParameters(key, value);
        // });

        return RowFactory.create(reportName, frequency, queryForGeoDetails);
    }

    private static String getQueryForDataFrameChange(String metaColumn, JSONObject configJson, String moColumns)
            throws JSONException {
        String dynamicQuery = null;
        logger.info("configJson: {}", configJson.toString());
        // String kpi = getValueFromJson(configJson, PMConstants.KPI_JSONKEY);
        String kpi = getValueFromJsonArray(configJson, PMConstants.KPI_JSONKEY);
        List<String> kpiIdList = getKpicodeGoupWiseMap(kpi);
        String collectList = "";
        moColumns = (StringUtils.isEmpty(moColumns) || moColumns == null) ? Symbol.EMPTY_STRING : ",moHeirachy";
        String metaColumns = metaColumn.replaceAll("DATE,TIME", "");
        metaColumns = metaColumns.startsWith(",") ? metaColumns.replaceFirst(",", "") : metaColumns;
        try {
            // String sql="select
            // D,V,L1,L2,L3,L4,NODE,ENB,concat_ws('##',Collect_list(concat(`1065#0`,'@@',DATE,'
            // ',TIME))) as `1065#0`,concat_ws('##',Collect_list(Concat(`1066#1`,'@@',DATE,'
            // ',TIME))) as `1066#1`,concat_ws('##',Collect_list(Concat(`9123#2`,'@@',DATE,'
            // ',TIME))) as `9123#2` from CSVTABLE group by D,V,L1,L2,L3,L4,NODE,ENB";
            for (int i = 0; i <= kpiIdList.size() - 1; i++) {
                if (i == kpiIdList.size() - 1) {
                    collectList += "concat_ws('##',Collect_list(concat(COALESCE(`" + kpiIdList.get(i)
                            + "`,'-'),'@@',DATE,' ',TIME))) as `" + kpiIdList.get(i) + "`";
                } else {
                    collectList += "concat_ws('##',Collect_list(concat(COALESCE(`" + kpiIdList.get(i)
                            + "`,'-'),'@@',DATE,' ',TIME)))  as `" + kpiIdList.get(i) + "`,";
                }

            }

            dynamicQuery = !StringUtils.substringBefore(metaColumns, "#").equalsIgnoreCase(Symbol.EMPTY_STRING)
                    ? "select " + StringUtils.substringBefore(metaColumns, "#") + moColumns + " ," + collectList
                            + " from PMOTFData group by " + StringUtils.substringBefore(metaColumns, "#") + moColumns
                            + ""
                    : "select " + collectList + moColumns + " from PMOTFData ";

            logger.info("sql for querytablechange : {} ", dynamicQuery);
        } catch (Exception e) {
            logger.error("Exception While getting query for report due to : {}", e.getMessage());
        }
        return dynamicQuery;
    }

    private static String getQueryForDataFrameNBHChange(String metaColumn, JSONObject configJson, String moColumns)
            throws JSONException {
        String dynamicQuery = null;
        // String kpi = getValueFromJson(configJson, PMConstants.KPI_JSONKEY);
        String kpi = getValueFromJsonArray(configJson, PMConstants.KPI_JSONKEY);
        List<String> kpiIdList = getKpicodeGoupWiseMap(kpi);
        String collectList = "";
        String metaColumns = metaColumn.replaceAll("DATE,TIME", "");
        moColumns = (StringUtils.isEmpty(moColumns) || moColumns == null) ? Symbol.EMPTY_STRING : ",moHeirachy";
        metaColumns = metaColumns.startsWith(",") ? metaColumns.replaceFirst(",", "") : metaColumns;
        try {
            // String sql="select
            // D,V,L1,L2,L3,L4,NODE,ENB,concat_ws('##',Collect_list(concat(`1065#0`,'@@',DATE,'
            // ',TIME))) as `1065#0`,concat_ws('##',Collect_list(Concat(`1066#1`,'@@',DATE,'
            // ',TIME))) as `1066#1`,concat_ws('##',Collect_list(Concat(`9123#2`,'@@',DATE,'
            // ',TIME))) as `9123#2` from CSVTABLE group by D,V,L1,L2,L3,L4,NODE,ENB";
            for (int i = 0; i <= kpiIdList.size() - 1; i++) {
                if (i == kpiIdList.size() - 1) {
                    collectList += "concat_ws('##',Collect_list(concat(COALESCE(`" + kpiIdList.get(i)
                            + "`,'-'),'@@',DATE))) as `" + kpiIdList.get(i) + "`";

                } else {
                    collectList += "concat_ws('##',Collect_list(concat(COALESCE(`" + kpiIdList.get(i)
                            + "`,'-'),'@@',DATE))) as `" + kpiIdList.get(i) + "` ,";
                }

            }
            dynamicQuery = !StringUtils.substringBefore(metaColumns, "#").equalsIgnoreCase(Symbol.EMPTY_STRING)
                    ? "select " + StringUtils.substringBefore(metaColumns, "#") + moColumns + "," + collectList
                            + " from PMOTFData group by " + StringUtils.substringBefore(metaColumns, "#") + moColumns
                            + ""
                    : "select  " + collectList + moColumns + " from PMOTFData  ";
            logger.info("sql for querytablechange : {} ", dynamicQuery);
        } catch (Exception e) {
            logger.error("Exception While getting query for report due to : {}", e.getMessage());
        }
        return dynamicQuery;
    }

    private static List<String> getKpicodeGoupWiseMap(String kpiJson) {
        List<String> kpiGroupMap = new ArrayList<>();
        try {
            int kpicount = 0;
            int counterkpi = 0;
            String kpiid = "";
            JSONArray kpiJsonArray = new JSONArray(kpiJson);
            for (Integer i = 0; i < kpiJsonArray.length(); i++) {
                JSONObject jsonObject = kpiJsonArray.getJSONObject(i);
                String kpiName = getJsonValue(jsonObject, "kpiName", Symbol.EMPTY_STRING);
                String type = getValueFromJson(jsonObject, "type");
                if ("counter".equalsIgnoreCase(type)) {
                    kpiid = StringUtils.substringBefore(kpiName, "-"); // + "#" + "" + counterkpi + "";
                    counterkpi++;
                } else {
                    kpiid = StringUtils.substringBefore(kpiName, "-"); // + "#" + "" + kpicount + "";
                    kpicount++;
                }
                if (!kpiGroupMap.contains(kpiid)) {
                    kpiGroupMap.add(kpiid);
                }
            }
        } catch (Exception e) {
            logger.error("Exception parse KPI Json :{}", ExceptionUtils.getStackTrace(e));
        }
        return kpiGroupMap;
    }

    private static String getJsonValue(JSONObject configJson, String jsonKey, String jsonValueKey)
            throws JSONException {
        if (configJson.has(jsonKey) && !configJson.isNull(jsonKey)) {
            jsonValueKey = configJson.getString(jsonKey);
        }
        return jsonValueKey;
    }

    @SuppressWarnings("unchecked")
    private String getNETypeList(String domain, String vendor, String node, String systemConfig, String technology) {
        String neType = null;
        String neTypeList = null;

        logger.info("Getting NE Type List for Domain={}, Vendor={}, Node={}, Technology={}", domain, vendor, node,
                technology);

        try {
            String nodeType = node.trim().replaceAll(PMConstants.SPACE, Symbol.UNDERSCORE_STRING)
                    .replaceAll(Symbol.HYPHEN_STRING, Symbol.UNDERSCORE_STRING);
            String neTypeKey = ("REPORTS_NETYPE_" + domain + Symbol.UNDERSCORE + vendor + Symbol.UNDERSCORE + technology
                    + Symbol.UNDERSCORE + nodeType).toUpperCase();
            Map<String, Object> jsonMap = new ObjectMapper().readValue(systemConfig, Map.class);
            neType = jsonMap.get(neTypeKey) != null ? (String) jsonMap.get(neTypeKey) : null;
            if (neType != null) {
                neTypeList = "'" + neType.replaceAll(PMConstants.COMMA, "','") + "'";
            }
        } catch (IOException e) {
            logger.error("Exception in Getting NE Type List, Message={}, Error={}", e.getMessage(), e);
        }
        return neTypeList;
    }

    private void getMagnetParameterConfig(JobContext jobcontext, Map<String, String> contextMap) throws Exception {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        String magnetConfig = "{}";
        String writeValueAsString = null;
        Map<String, Map<String, String>> magnetParamMap = null;

        try {
            ObjectMapper mapper = new ObjectMapper();
            try {
                if (!isLocal) {
                    String sql = "SELECT CONFIG_VALUE FROM BASE_CONFIGURATION WHERE CONFIG_TYPE = 'PM' AND CONFIG_TAG = 'CONFIG' AND CONFIG_KEY = 'MAGNET_PARAM_CONFIG'";
                    ResultSet resultSet = createConnectionPlatform(sql, contextMap);
                    while (resultSet.next()) {
                        magnetConfig = resultSet.getString(1);
                    }
                } else {
                    magnetConfig = PMConstants.MAGNET_PARAM_CONFIG;
                }
                magnetParamMap = mapper.readValue(magnetConfig, new TypeReference<Map<String, Map<String, String>>>() {
                });
                logger.info("magnetParamMap size in class CreateMetaData - {}", magnetParamMap.size());
            } catch (Exception e) {
                logger.error("Exception While Fetching Magnet Parameter Config, Message={}, Error={}", e.getMessage(),
                        e);
            } finally {
                doFinally(connection, preparedStatement);
            }

            String key = jobcontext.getParameter("domain") + "_" + jobcontext.getParameter("technology");
            writeValueAsString = mapper.writeValueAsString(magnetParamMap.get(key));

            logger.info("Magnet Parameter Config={}, Key={}", writeValueAsString, key);
            jobcontext.setParameters("MAGNET_PARAM_JSON", writeValueAsString != null ? writeValueAsString : "{}");
        } catch (Exception e) {
            logger.error("Error While Fetching Magnet Parameter Config, Message={}, Error={}", e.getMessage(), e);
        }
    }

    private ResultSet createConnectionPlatform(String sqlQuery, Map<String, String> contextMap) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        final String JDBC_DRIVER = contextMap.get("SPARK_PLATFORM_JDBC_DRIVER");
        final String DB_URL = contextMap.get("SPARK_PLATFORM_JDBC_URL");
        final String USER = contextMap.get("SPARK_PLATFORM_JDBC_USERNAME");
        final String PASS = contextMap.get("SPARK_PLATFORM_JDBC_PASSWORD");
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("Error while fetching data from DB - {}", e.getMessage());
        } finally {
            doFinally(conn, stmt);
        }
        return rs;
    }

    private ResultSet createConnection(String sqlQuery, Map<String, String> contextMap) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
        final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
        final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
        final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

        logger.info("JDBC Driver={}, JDBC URL={}, JDBC Username={}, JDBC Password={}", jdbcDriver, jdbcUrl,
                jdbcUsername, jdbcPassword);

        try {
            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(sqlQuery);
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("Error While Fetching Data From DB, Message={}, Error={}", e.getMessage(), e);
        } finally {
            doFinally(connection, preparedStatement);
        }
        return resultSet;
    }

    private void doFinally(Connection conn, PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("Error in Closing Statement in parse - {}", e.getMessage());
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Error in Closing connection in parse- {}", e.getMessage());
            }
        }
    }

    private List<String> getValueListFromJsonByKey(String json, String jsonKey) {
        String value = null;
        List<String> returList = new ArrayList<>();
        try {
            Map<String, Object> jsonMap = new ObjectMapper().readValue(json, Map.class);
            value = jsonMap.get(jsonKey) != null ? (String) jsonMap.get(jsonKey) : null;
            if (value != null) {
                returList.addAll(Arrays.asList(value.split(PMConstants.COMMA)));
            }
        } catch (IOException e) {
            logger.error("Exception in getting systemValueList  :{}", ExceptionUtils.getStackTrace(e));
        }
        return returList;
    }

    private static Map<String, Map<String, String>> kpiFormulaFinalMap = null;
    private static String colFilter;

    private void updateParamaterInJobContext(JSONObject configJson, String reportName, String geography,
            String reportLevel, String reportType, String metaColumn, String domain, String vendor, String technology,
            String nodeVal, String nodetype, List<String> coreDomains) throws Exception {

        String neType = getValueFromJson(configJson, "netype");

        logger.info(
                "Update Paramater In Job Context: reportName={}, domain={}, vendor={}, technology={}, nodeVal={}, node={}, neType={}, reportLevel={}",
                reportName, domain, vendor, technology, nodeVal, nodetype, neType, reportLevel);

        // reportName=DYNAMIC-ANALYSIS-REPORT,
        // domain=TRANSPORT, vendor=CISCO, technology=COMMON,
        // nodeVal=CLUBBED, node=ROUTER, neType=Router

        if (coreDomains.contains(domain)) {
            jobcontext.setParameters("netype", neType);
        }
        String bandType = getValueFromJson(configJson, "bandType");
        String frequency = getStringValueFromJsonArray(getJsonArray(configJson, "frequency"));
        String node = getStringValueFromJsonArray(getJsonArray(configJson, "node"));
        String neStatus = getValueFromJson(configJson, "neStatus");
        String analysisObject = getValueFromJson(configJson, "anlysisLevelObject");
        String fromDate = getValueFromJson(configJson, "fromDate");
        String toDate = getValueFromJson(configJson, "toDate");
        String duration = getValueFromJson(configJson, "duration");
        String utilizationkey = getValueFromJson(configJson, "utilization_key");

        JSONArray kpiArray = configJson.getJSONArray("kpi");
        String kpi = kpiArray.toString();

        String siteCategory = getValueFromJson(configJson, "siteCategory");
        String targetArea = getValueFromJson(configJson, "targetArea");
        String hourList = getStringValueFromJsonArray(getJsonArray(configJson, "perHourList"));
        String specificDate = getStringValueFromJsonArray(getJsonArray(configJson, "specificDate"));
        String specificDateList = getStringValueFromJsonArray(getJsonArray(configJson, "specificDateList"));
        String specificKey = configJson.has("specificDateList") ? "true" : "false";
        String corePodLevel = getValueFromJson(configJson, "level");
        String dayNo = getValueFromJson(configJson, "numberOfInstance");
        String dayno = "";
        if (dayNo != null && !dayNo.isEmpty()) {
            dayno = dayNo;
        } else if (duration.equalsIgnoreCase("HOURLY")
                && (dayNo == null || dayNo.isEmpty())) {
            dayno = "";
        } else {
            dayno = "1";
        }

        logger.info("Processed Day Parameters: dayNo={}, dayno={}, specificKey={}", dayNo, dayno, specificKey);
        // dayNo=, dayno=1, specificKey=false

        Set<String> finalColumn = new HashSet<>();

        String metaColumns = "";

        boolean isDateTimeAvailable = false;

        // For neid-nename logic

        // String copyMetaCols = metaColumn;

        String tempColumns = metaColumn.split(Symbol.HASH_STRING)[0].toUpperCase();

        if (tempColumns != null && tempColumns.contains("DT") && tempColumns.contains("HR")) {
            metaColumn = metaColumn.replace("DT", "DATE");
            metaColumn = metaColumn.replace("HR", "TIME");
            isDateTimeAvailable = true;

        }
        boolean isNodeLevel = false;
        if (!reportLevel.contains("L0") && !reportLevel.contains("L1") && !reportLevel.contains("L2")
                && !reportLevel.contains("L3") && !reportLevel.contains("L4")) {
            isNodeLevel = true;
        }

        jobcontext.setParameters("isNodeLevel", String.valueOf(isNodeLevel));
        logger.info("isNodeLevel={} Set to Job Context Successfully!", isNodeLevel);

        if (isDateTimeAvailable) {
            if (isNodeLevel) {
                metaColumns = "DL1,DL2,DL3,DL4,T,H1,H2,ENB,CEL,NAM,BND,NS,M33";
            } else {
                metaColumns = "DL1,DL2,DL3,DL4,T,H1,H2,ENB,CEL,NAM,BND,NS,M33";
            }

        } else {
            metaColumns = "DATE,TIME,DL1,DL2,DL3,DL4,T,H1,H2,ENB,CEL,NEID,NAM,BND,NS,M33";
        }
        logger.info("Updated metaColumns={}", metaColumns);

        String finalColumns = metaColumns + "," + metaColumn.split(Symbol.HASH_STRING)[0];
        finalColumn = Stream.of(finalColumns.trim().split(",")).collect(Collectors.toSet());
        String finalMetaColumns = String.join(",", finalColumn);

        jobcontext.setParameters("finalMetaColumns", finalMetaColumns);

        logger.info("Processed Meta Columns: finalMetaColumns={}", finalMetaColumns);
        // NS,DL1,H1,DL3,H2,TIME,DL2,CEL,M33,DL4,DATE,nodename,T,ENB,BND,Country,NAM

        String cellOwner = configJson.has("cellOwner")
                ? getStringValueFromJsonArray(configJson.getJSONArray("cellOwner"))
                : null;
        String filterLevel = configJson.has("filterLevel")
                ? getStringValueFromJsonArray(configJson.getJSONArray("filterLevel"))
                : null;
        String magnetFields = configJson.has("magnetFields")
                ? getStringValueFromJsonArray(configJson.getJSONArray("magnetFields"))
                : null;

        String jvidColumn = configJson.has("jvidColumn")
                ? getStringValueFromJsonArray(configJson.getJSONArray("jvidColumn"))
                : null;
        String beaconHeader = configJson.has("BeaconHeader") ? getValueFromJson(configJson, "BeaconHeader") : null;

        logger.info(
                "Processed Optional Parameters: cellOwner={}, filterLevel={}, magnetFields={}, jvidColumn={}, beaconHeader={}",
                cellOwner, filterLevel, magnetFields, jvidColumn, beaconHeader);
        // cellOwner=null, filterLevel=null, magnetFields=null, jvidColumn=null,
        // beaconHeader=null

        jobcontext.setParameters("BeaconHeader", beaconHeader);
        jobcontext.setParameters("jvidColumn", jvidColumn);
        String trendOTFReport = configJson.has("isTrend") ? getValueFromJson(configJson, "isTrend") : "false";
        String threshold = configJson.has("Threshold") ? "true" : "false";

        logger.info("Processed Other Parameters: trendOTFReport={}, threshold={}", trendOTFReport, threshold);
        // trendOTFReport=false, threshold=false

        jobcontext.setParameters("Threshold", threshold);
        jobcontext.setParameters("cellOwner", cellOwner);
        jobcontext.setParameters("magnetFields", magnetFields);
        jobcontext.setParameters("trendOTFReport", trendOTFReport);
        jobcontext.setParameters("analysisObject", analysisObject);
        jobcontext.setParameters("specificDate", specificDate);
        jobcontext.setParameters("specificKey", specificKey);
        jobcontext.setParameters("specificDateList", specificDateList);
        String reportFormatType = getValueFromJson(configJson, "reportFormatType");
        String excludeCR = getValueFromJson(configJson, "excludeCR");

        logger.info("Processed Other Parameters: reportFormatType={}, excludeCR={}", reportFormatType, excludeCR);
        // reportFormatType=excel, excludeCR=false

        jobcontext.setParameters("corePodLevel", corePodLevel);
        jobcontext.setParameters("reportFormatType", reportFormatType);
        jobcontext.setParameters("bandType", bandType);
        jobcontext.setParameters("frequencies", frequency);
        jobcontext.setParameters("frequency", frequency);
        jobcontext.setParameters("node", node);
        jobcontext.setParameters("neStatus", neStatus);
        jobcontext.setParameters("fromDate", fromDate);
        jobcontext.setParameters("toDate", toDate);
        jobcontext.setParameters("duration", duration);
        jobcontext.setParameters("utilization_key", utilizationkey);
        jobcontext.setParameters("kpi", kpi);
        jobcontext.setParameters("perHourList", hourList);
        jobcontext.setParameters("reportName", reportName);
        jobcontext.setParameters("geography", geography);
        jobcontext.setParameters("reportLevel", reportLevel);
        jobcontext.setParameters("nodeVal", nodeVal);
        jobcontext.setParameters("reportType", reportType);
        jobcontext.setParameters("metaColumn", metaColumn);
        jobcontext.setParameters("siteCategory", siteCategory);
        jobcontext.setParameters("targetArea", targetArea);
        jobcontext.setParameters("excludeCR", excludeCR);
        jobcontext.setParameters("domain", domain);
        jobcontext.setParameters("vendor", vendor);
        jobcontext.setParameters("technology", technology);

        String normalKPICounter = "";
        Set<String> counterMap = new HashSet<>();
        Set<String> categoryList = new HashSet<>();
        Map<String, Map<String, String>> counterWithvalues = new HashMap<>();
        specificDate = specificKey.equalsIgnoreCase("true") ? specificDateList : specificDate;
        List<String> quarterlyDateList = getQuarterlyDateList(frequency, fromDate, toDate, specificDate, duration,
                dayno);
        logger.info("Generated Quarterly DateList={}", quarterlyDateList);

        String moHierarchy = getValueFromJson(configJson, "moHierarchy");
        String comparisionType = getValueFromJson(configJson, "comparisionType");

        logger.info("Retrieved moHierarchy={}, comparisionType={}", moHierarchy, comparisionType);
        // moHierarchy=null, comparisionType=KPI

        jobcontext.setParameters("comparisionType", comparisionType);

        String kpiString = getValueFromJsonArray(configJson, "kpi");
        JSONArray kpiJson = new JSONArray(kpiString);
        Map<String, List<String>> timeShiftWiseDateMap = new HashMap<>();
        Map<String, List<String>> finaltimeShiftWiseDateMap = new HashMap<>();
        Map<String, String> genericKPIWiseFormulaDesc = new HashMap<>();

        Set<String> finalkpiIdList = new HashSet<>();
        if (StringUtils.isNotEmpty(comparisionType) && comparisionType.equalsIgnoreCase("Counter")) {

            String moKpis = null;
            for (Integer i = 0; i < kpiJson.length(); i++) {
                JSONObject jsonObject1 = kpiJson.getJSONObject(i);
                String kpiName = getValueFromJson(jsonObject1, "kpiName");
                String kpiCode = kpiName.split(Symbol.HYPHEN_STRING)[0];
                String type1 = getValueFromJson(jsonObject1, "type");
                if (!"counter".equalsIgnoreCase(type1)) {
                    if (moKpis != null && kpiCode != null) {
                        moKpis = moKpis + Symbol.COMMA_STRING + kpiCode;
                    } else {
                        moKpis = kpiCode;
                    }
                }
            }
            jobcontext.setParameters("kpiIdsWithSubKpi", moKpis);
            jobcontext.setParameters("kpiIds", moKpis);
            logger.info("Getting mo hierarchy is :{}  ", moHierarchy);
            Map<String, Map<String, String>> counterInfoMap = new HashMap<String, Map<String, String>>();
            Map<String, String> counterAggrMap = new HashMap<>();
            Map<String, List<Map<String, String>>> catgoryInfoForMOReport = getCounterListMapByCategory(configJson,
                    counterInfoMap, counterAggrMap);
            categoryList = catgoryInfoForMOReport.entrySet().stream().map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            jobcontext.setParameters("catgoryInfoMap", new Gson().toJson(catgoryInfoForMOReport));
            jobcontext.setParameters("counterAggrMap", new Gson().toJson(counterAggrMap));
            jobcontext.setParameters("categoryString", String.join(",", categoryList));
            jobcontext.setParameters("counterInfoMap", new Gson().toJson(counterInfoMap));
            String finalCounterInfo = counterInfoMap.entrySet().stream()
                    .map(e -> "C" + e.getValue().get("sequenceNo") + "#" + e.getValue().get("pmcountervariableid_pk"))
                    .distinct().collect(Collectors.joining(","));
            jobcontext.setParameters("finalCounterInfo", finalCounterInfo);
            timeShiftWiseDateMap = getTimeShiftMapForCounters(quarterlyDateList, frequency, finaltimeShiftWiseDateMap);
            logger.info("Getting timeShiftWiseDateMap is :{}", timeShiftWiseDateMap);
            jobcontext.setParameters("timeShiftWiseDateMap", new Gson().toJson(timeShiftWiseDateMap));

        } else {
            logger.info("============= Processing Comparison Type: KPI ===========");
            Set<String> kpiList = new HashSet<>();
            Set<String> counterIdList = new HashSet<>();
            Set<String> genericKpiList = new HashSet<>();
            Set<String> timeShiftedKPIs = new HashSet<>();
            Set<String> normalKPIs = new HashSet<>();
            Set<String> kpiWithSubKPI = new HashSet<>();
            Set<String> timeShiftWithSubkpi = new HashSet<>();
            Map<String, Set<String>> map = new HashMap<>();
            setKPIsAndCounterList(kpi, kpiList, counterIdList, genericKpiList, finalkpiIdList);

            logger.info("Configuration: KPI List={}, Counter List={}, Generic KPI List={}", kpiList, counterIdList,
                    genericKpiList);
            // KPI List=[1088, 1086, 1083], Counter List=[29406, 29403, 29408], Generic KPI
            // List=[]

            Map<String, List<String>> genericKpiMappingByGenericKpiList = new HashMap<>();

            String kpiCode = null;
            jobcontext.setParameters("MoKPI", kpiCode);

            if (CollectionUtils.isNotEmpty(genericKpiList)) {
                genericKpiList = getGenericKpiList(jobcontext, genericKpiList, genericKPIWiseFormulaDesc);
                genericKpiMappingByGenericKpiList = getGenericKpiMappingByGenericKpiList(jobcontext, genericKpiList,
                        kpiList);
            }
            jobcontext.setParameters("genericKPIWiseFormulaDesc", new Gson().toJson(genericKPIWiseFormulaDesc));
            jobcontext.setParameters("genericKpiMappingByGenericKpiList",
                    new Gson().toJson(genericKpiMappingByGenericKpiList));

            logger.info("Configuration: Generic KPI Wise Formula Desc={}, Generic KPI Mapping By Generic KPI List={}",
                    genericKPIWiseFormulaDesc, genericKpiMappingByGenericKpiList);
            // Generic KPI Wise Formula Desc={}, Generic KPI Mapping By Generic KPI List={}

            getNormalAndTimeShiftKPIs(kpiList, normalKPIs, timeShiftedKPIs);
            normalKPICounter = String.join(",", normalKPIs);
            if (!counterIdList.isEmpty()) {
                normalKPICounter = normalKPICounter + "," + String.join(",", counterIdList);
            }
            jobcontext.setParameters("normalKPICounter", normalKPICounter);
            logger.info("Normal KPICounter={} Set to Job Context Successfully!", normalKPICounter);
            // 1088,1086,1083,29406,29403,29408

            kpiWithSubKPI = getNormalSubkpiWithKPI(normalKPIs, kpiWithSubKPI, timeShiftedKPIs);
            logger.info("KPI With Sub KPI={} ", kpiWithSubKPI);
            // KPI With Sub KPI=[1088, 1086, 1083]

            timeShiftWithSubkpi = getTimeShiftSubkpiWithKPI(timeShiftedKPIs, timeShiftWithSubkpi, map);
            logger.info("TimeShift With Sub KPI={} ", timeShiftWithSubkpi);
            // TimeShift With Sub KPI=[]

            Set<String> timeShiftkpiIds = timeShiftWithSubkpi.stream().filter(e -> !e.contains("TimeShift"))
                    .collect(Collectors.toSet());
            logger.info("TimeShift KPI IDs={} ", timeShiftkpiIds);
            // TimeShift KPI IDs=[]

            if (!CollectionUtils.isEmpty(kpiWithSubKPI)) {
                timeShiftWiseDateMap.put("0", quarterlyDateList);
                finaltimeShiftWiseDateMap.put("0", quarterlyDateList);
            }
            kpiWithSubKPI.addAll(timeShiftkpiIds);
            logger.info("Final KPI IDs={} and TimeShift KPI IDs={} ", kpiWithSubKPI, timeShiftkpiIds);
            // Final KPI IDs=[1088, 1086, 1083] and TimeShift KPI IDs=[]

            String kpiWithSubKPIs = kpiWithSubKPI.stream().filter(e -> !e.isEmpty()).distinct()
                    .collect(Collectors.joining(","));

            jobcontext.setParameters("kpiIdsWithSubKpi", kpiWithSubKPIs);
            logger.info("KPI IDs With Sub KPI={} Set to Job Context Successfully!", kpiWithSubKPIs);
            // 1088,1086,1083

            timeShiftWiseDateMap = getTimeShifWiseQuarterList(frequency, timeShiftWithSubkpi, quarterlyDateList,
                    finaltimeShiftWiseDateMap);

            jobcontext.setParameters("timeShiftWiseDateMap", new Gson().toJson(timeShiftWiseDateMap));
            logger.info("TimeShift Wise Date Map={} Set to Job Context Successfully!", timeShiftWiseDateMap);
            // {0=[25012*]}

            String counterIds = counterIdList.stream().collect(Collectors.joining(","));
            Map<String, Map<String, String>> counterInfoMap = getCounterInfoMap(jobcontext);
            jobcontext.setParameters("counterInfoMap", new Gson().toJson(counterInfoMap));
            logger.info("Counter Info Map={} Set to Job Context Successfully!", counterInfoMap);

            setCategoryListAndCounterMap(domain, vendor, kpiWithSubKPIs, counterIds, counterMap, categoryList,
                    counterWithvalues);
            logger.info("Counter With Value={}", counterWithvalues);

            Map<String, List<Map<String, String>>> catgoryInfoMap = getCatgoryInfoMap(counterInfoMap);
            jobcontext.setParameters("catgoryInfoMap", new Gson().toJson(catgoryInfoMap));
            logger.info("Category Info Map={} Set to Job Context Successfully!", catgoryInfoMap);

            String categoryString = counterInfoMap.entrySet().stream().map(e -> e.getValue().get("categoryName"))
                    .distinct().collect(Collectors.joining(","));
            String countersId = counterInfoMap.entrySet().stream().map(e -> e.getValue().get("pmcountervariableid_pk"))
                    .distinct().collect(Collectors.joining(","));
            String finalCounterInfo = counterInfoMap.entrySet().stream()
                    .map(e -> "C" + e.getValue().get("sequenceNo") + "#" + e.getValue().get("pmcountervariableid_pk"))
                    .distinct().collect(Collectors.joining(","));

            jobcontext.setParameters("countersId", countersId);
            jobcontext.setParameters("categoryString", categoryString);

            logger.info("Counters ID={}, Category String={} Set to Job Context Successfully!", countersId,
                    categoryString);

            categoryList.clear();
            categoryList.addAll(Arrays.asList(categoryString.split(",")));
            jobcontext.setParameters("kpiIds", kpiWithSubKPIs);
            logger.info("KPI IDs={} Set to Job Context Successfully!", kpiWithSubKPIs);

            String timeShiftedKPI = timeShiftWithSubkpi.stream().distinct().collect(Collectors.joining(","));
            jobcontext.setParameters("timeShiftedKPIs", timeShiftedKPI);
            logger.info("TimeShifted KPIs={} Set to Job Context Successfully!", timeShiftedKPI);

            counterIds = String.join(",", counterMap);
            jobcontext.setParameters("counterIds", counterIds);
            logger.info("Counter IDs={} Set to Job Context Successfully!", counterIds);

            jobcontext.setParameters("finalCounterInfo", finalCounterInfo);
            logger.info("Final Counter Info={} Set to Job Context Successfully!", finalCounterInfo);

            Map<String, String> counterAggrMap = getCounterAggrMap(jobcontext.getParameters());
            jobcontext.setParameters("counterAggrMap", new Gson().toJson(counterAggrMap));
            logger.info("Counter Aggr Map={} Set to Job Context Successfully!", counterAggrMap);
        }

        jobcontext.setParameters("quarterlyDateList", String.join(", ", quarterlyDateList));
        jobcontext.setParameters("categoryList", String.join(",", categoryList));
        jobcontext.setParameters("counterWithAttribute", new Gson().toJson(counterWithvalues));

        logger.info("Category List={}, Counter With Attribute={}, Quarterly Date List={}", categoryList,
                counterWithvalues, quarterlyDateList);

        String emstype = getEmsTypeOnDomainVendor(domain, vendor);
        logger.info("EMS_TYPE={}, Domain={}, Vendor={}", emstype, domain, vendor);

        String COUNTER_PARQUET = jobcontext.getParameter("COUNTER_PARQUET");
        String META_PARQUET = jobcontext.getParameter("META_PARQUET");
        logger.info("Counter Parquet={}, Meta Parquet={}", COUNTER_PARQUET, META_PARQUET);

        // Counter Parquet=s3a://bntvpm/PERFORMANCE/ALLCOUNTER/
        // Meta Parquet=s3a://bntvpm/TRINO_ALLCOUNTER/NE_META/

        if (StringUtils.isNotEmpty(jobcontext.getParameter("vendor"))
                && jobcontext.getParameter("vendor").equalsIgnoreCase("ALL")) {
            getCategoryWiseParquetPath(jobcontext, categoryList, COUNTER_PARQUET, META_PARQUET);
        } else {
            logger.info("Processing Specific Vendor Case={}", vendor);

            String basePath = COUNTER_PARQUET + "domain=" + domain + "/vendor=" + vendor + "/emstype=" + emstype
                    + "/technology=" + technology;

            String metaColumnPath = META_PARQUET + "d=" + domain + "/v=" + vendor + "/emstype=" + emstype + "/t="
                    + technology + "/";

            jobcontext.setParameters("basePath", basePath);
            jobcontext.setParameters("metaColumnPath", metaColumnPath);

            logger.info("Base Path={}, Meta Column Path={}", basePath, metaColumnPath);
            // Base
            // Path=s3a://bntvpm/PERFORMANCE/ALLCOUNTER/domain=TRANSPORT/vendor=CISCO/emstype=NA/technology=COMMON
            // Meta Column
            // Path=s3a://bntvpm/TRINO_ALLCOUNTER/NE_META/d=TRANSPORT/v=CISCO/emstype=NA/t=COMMON/
        }

        jobcontext.setParameters("counterMap", new Gson().toJson(counterMap));
        logger.info("Counter Map={} Set to Job Context Successfully!", counterMap);

        if (!StringUtils.isEmpty(filterLevel)) {
            jobcontext.setParameters("filterLevel", filterLevel);
        } else {

            String filterLevelFinal = getFilter(reportLevel, technology, vendor, nodetype, nodeVal);
            jobcontext.setParameters("filterLevel", filterLevelFinal);
            logger.info("Filter Level={} Set to Job Context Successfully!", filterLevelFinal);
        }

        String kpiIds = jobcontext.getParameter("kpiIds");
        logger.info("KPI IDs To Process={}", kpiIds);
        // 1088,1086,1083

        if (kpiFormulaFinalMap == null && StringUtils.isNotEmpty(kpiIds)) {
            getkpiFormulaMap(kpiIds);
        }
        jobcontext.setParameters("kpiFormulaFinalMap", new Gson().toJson(kpiFormulaFinalMap));

        Set<String> timeShiftKeysSet = timeShiftWiseDateMap.entrySet().stream().map(e -> e.getKey()).distinct()
                .collect(Collectors.toSet());

        logger.info("Time Shift Keys Set: {}", timeShiftKeysSet);

        String timeShiftKeys = timeShiftKeysSet.stream().filter(e -> !e.equals("0")).collect(Collectors.joining(","));

        logger.info("Time Shift Keys: {}", timeShiftKeys);

        String finalTimeShiftKeys = !StringUtils.isEmpty(timeShiftKeys) ? "true" : "false";
        jobcontext.setParameters("finalTimeShiftKeys", finalTimeShiftKeys);

        logger.info("Final Time Shift Keys: {}", finalTimeShiftKeys);

        getPMCounterVariableAggrQuery(jobcontext, frequency, timeShiftWiseDateMap);

        Map<String, Map<String, String>> kpiFormulaMap = new Gson()
                .fromJson(jobcontext.getParameter("kpiFormulaFinalMap"), Map.class);

        logger.info("Final Time Shift Wise Date Map={}", finaltimeShiftWiseDateMap);
        logger.info("KPI Formula Map={}", kpiFormulaMap);

        if (timeShiftKeys.equalsIgnoreCase("true")) {

            logger.info("Processing Time Shift Query...");
            String timeShiftQuery = getTimeShiftQuery(kpiFormulaMap, normalKPICounter, finaltimeShiftWiseDateMap,
                    frequency);
            jobcontext.setParameters("timeShiftQuery", timeShiftQuery);
        } else {
            logger.info("Time Shift Keys Set to False, Skipping Time Shift Query!");
        }

        logger.info("=====Successfully Updated Paramaters In Job Context for Report=\"{}\"======", reportName);
    }

    public static void main1(String[] args) {
        String normalKPI = "1041,12308";
        String formula = "{\"7375)).TimeShift(336##((SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C34#13100\":\"SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7187##(((KPI#2118))-((KPI#7186)))\":{},\" 2102)).TimeShift(336##((KPI#2101)).TimeShift(336)+((KPI#2100)).TimeShift(336)\":{},\" 7378##((((((((KPI#2102)).TimeShift(168)))+((((KPI#2102)).TimeShift(336))))+((((KPI#2102)).TimeShift(504))))+((((KPI#2102)).TimeShift(672))))/(4))\":{},\" 7376##(((KPI#7375))-((((((((KPI#7375)).TimeShift(168)))+((((KPI#7375)).TimeShift(336))))+((((KPI#7375)).TimeShift(504))))+((((KPI#7375)).TimeShift(672))))/(4)))\":{},\" 7071)).TimeShift(336##((SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C24#20409\":\"SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7071)).TimeShift(168##((SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C24#20409\":\"SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7346##((IF(((((KPI#2102)-(((((((KPI#2102)).TimeShift(168))+(((KPI#2102)).TimeShift(336)))+(((KPI#2102)).TimeShift(504)))+(((KPI#2102)).TimeShift(672)))/4)))>\":\" 50),\"1,\"0)))\":{},\" 7375##((SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C34#13100\":\"SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7078##((TRAFFIC.TCH_REJ_REQ_DUE_LACK_FR.Sum.Sum))\":{\"C116#20413\":\"TRAFFIC.TCH_REJ_REQ_DUE_LACK_FR.Sum.Sum},\" 7186##((((((((KPI#2118)).TimeShift(168)))+((((KPI#2118)).TimeShift(336))))+((((KPI#2118)).TimeShift(504))))+((((KPI#2118)).TimeShift(672))))/(4))\":{},\" 7071##((SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C24#20409\":\"SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7375)).TimeShift(672##((SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C34#13100\":\"SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum},\" 2102)).TimeShift(168##((KPI#2101)).TimeShift(168)+((KPI#2100)).TimeShift(168)\":{},\" 2118##(((KPI#2116))+((KPI#2117)))\":{},\" 7406##((KPI#7071))-((KPI#7404))\":{},\" 7404##(((((KPI#7071)).TimeShift(168)))+((((KPI#7071)).TimeShift(336)))+((((KPI#7071)).TimeShift(504)))+((((KPI#7071)).TimeShift(672))))/(4)\":{},\" 2102)).TimeShift(672##((KPI#2101)).TimeShift(672)+((KPI#2100)).TimeShift(672)\":{},\" 7375)).TimeShift(168##((SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C34#13100\":\"SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7071)).TimeShift(672##((SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C24#20409\":\"SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7375)).TimeShift(504##((SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C34#13100\":\"SERVICE.TCH_NEW_CALL_ASSIGN.Sum.Sum},\" 7071)).TimeShift(504##((SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum))\":{\"C24#20409\":\"SERVICE.SDCCH_NEW_CALL_ASSIGN.Sum.Sum},\" 2102)).TimeShift(504##((KPI#2101)).TimeShift(504)+((KPI#2100)).TimeShift(504)\":{},\" 7379##(((KPI#2102))-((((((((KPI#2102)).TimeShift(168)))+((((KPI#2102)).TimeShift(336))))+((((KPI#2102)).TimeShift(504))))+((((KPI#2102)).TimeShift(672))))/(4)))\":{},\" 2102##((KPI#2101))+((KPI#2100))\":{}}";
        Map<String, Map<String, String>> kpiFormulaFinalMap = new Gson().fromJson(formula, Map.class);
        // String timeShiftDate =
        // "{\"0\":[\"22020100\",\"22020101\"],\"1\":[\"22013123\",\"22020100\"],\"2\":[\"22013122\",\"22013123\"]}";
        String timeShiftDate = "{\"0\":[\"220201\",\"220202\"],\"1\":[\"220131\",\"220201\"],\"2\":[\"220130\",\"220131\"]}";
        Map<String, List<String>> timeShiftDateMap = new Gson().fromJson(timeShiftDate, Map.class);
        String timeShiftQuery = getTimeShiftQuery(kpiFormulaFinalMap, normalKPI, timeShiftDateMap, "PERDAY");
        System.out.println("timeShiftQuery : " + timeShiftQuery);
    }

    public static void main2(String[] args) {
        String formula = "{\"1267,G0090##((IF((((RESAVAIL.BCCH_UPTIME.Sum.Sum))> 0),CellAvailableTime.Granularity.Sum.Sum,0)))\":{\"C94#12787\":\"RESAVAIL.BCCH_UPTIME.Sum.Sum\", \"C1#20473\":\"CellAvailableTime.Granularity.Sum.Sum\"}, \"1268,G0091##((IF((((RESAVAIL.AVE_AVAIL_TCH_SUM.Sum.Sum)+(RESAVAIL.AVE_GPRS_CHANNELS_SUM.Sum.Sum))= 0),CellAvailableTime.Granularity.Sum.Sum,RESAVAIL.BCCH_DOWNTIME.Sum.Sum)))\":{\"C62#12788\":\"RESAVAIL.AVE_GPRS_CHANNELS_SUM.Sum.Sum\", \"C60#12789\":\"RESAVAIL.AVE_AVAIL_TCH_SUM.Sum.Sum\", \"C93#12790\":\"RESAVAIL.BCCH_DOWNTIME.Sum.Sum\", \"C1#20473\":\"CellAvailableTime.Granularity.Sum.Sum\"}, \"9977,G0681##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0) || IF(EQUALS(#M22#,\\\"P\\\"),1,0),((KPI#2689)), 0)))\":{}, \"9973,G0682##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0) || IF(EQUALS(#M22#,\\\"P\\\"),1,0),((RAN_2G_Beacon.S12_2G_AVAIL_NUM.Sum.Sum)), 0)))\":{\"C4#13337\":\"RAN_2G_Beacon.S12_2G_AVAIL_NUM.Sum.Sum\"}, \"7597,G1800##((IF(IF(EQUALS(#M22#,\\\"P\\\"),1,0),1, 0)))\":{\"C4#18218\":\"TRAFGPRS4.MUTIL146.Sum.Sum\"}, \"3585,G1799##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0),1, 0)))\":{\"C1#16736\":\"TRAFGPRS4.MUTIL148.Sum.Sum\"}, \"7599,G1800##((IF(IF(EQUALS(#M22#,\\\"P\\\"),1,0),1,0)))\":{\"C2#13335\":\"RAN_2G_Beacon.S12_2G_ASR_NUM.Sum.Sum\"}, \"9971,G0682##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0) || IF(EQUALS(#M22#,\\\"P\\\"),1,0),((KPI#2690)), 0)))\":{}, \"6438##((IF((((RESAVAIL.BCCH_UPTIME.Sum.Sum))>= 0),CellAvailableTime.Granularity.Sum.Sum,0)))\":{\"C94#12787\":\"RESAVAIL.BCCH_UPTIME.Sum.Sum\", \"C1#20473\":\"CellAvailableTime.Granularity.Sum.Sum\"}, \"3580,G1799##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0),1, 0)))\":{\"C14#24577\":\"RESACC.AVE_RACH_BUSY.Sum.Sum\"}, \"3579,G1800##((IF(IF(EQUALS(#M22#,\\\"P\\\"),1,0),1, 0)))\":{\"C1#18324\":\"RESACC.PAGING_MSG_SENT.Sum.Sum\"}, \"1258,G0090##((CellAvailableTime.Granularity.Sum.Sum))*((IF((((DOWNTIME.BDWNACC.Sum.Sum))>= 0),1,0)))\":{\"C1#22211\":\"CellAvailableTime.Granularity.Sum.Sum\", \"C1#12235\":\"DOWNTIME.BDWNACC.Sum.Sum\"}, \"1259,G0091##(10*(DOWNTIME.BDWNACC.Sum.Sum))\":{\"C1#12235\":\"DOWNTIME.BDWNACC.Sum.Sum\"}, \"1582,G0091##(RAN_2G_Beacon.S12_2G_AVAIL_NUM.Sum.Sum)\":{\"C4#13337\":\"RAN_2G_Beacon.S12_2G_AVAIL_NUM.Sum.Sum\"}, \"1581,G0090##(RAN_2G_Beacon.S12_2G_AVAIL_DENOM.Sum.Sum)\":{\"C3#13336\":\"RAN_2G_Beacon.S12_2G_AVAIL_DENOM.Sum.Sum\"}, \"2689##((CellAvailableTime.Granularity.Sum.Sum))*((IF((((DOWNTIME.BDWNACC.Sum.Sum))>= 0),1,0)))\":{\"C1#22211\":\"CellAvailableTime.Granularity.Sum.Sum\", \"C1#12235\":\"DOWNTIME.BDWNACC.Sum.Sum\"}, \"2690##((10)*(DOWNTIME.BDWNACC.Sum.Sum))\":{\"C1#12235\":\"DOWNTIME.BDWNACC.Sum.Sum\"}, \"9979,G0681##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0) || IF(EQUALS(#M22#,\\\"P\\\"),1,0),((RAN_2G_Beacon.S12_2G_AVAIL_DENOM.Sum.Sum)), 0)))\":{\"C3#13336\":\"RAN_2G_Beacon.S12_2G_AVAIL_DENOM.Sum.Sum\"}, \"9969,G0682##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0) || IF(EQUALS(#M22#,\\\"P\\\"),1,0),((IF(((((RESAVAIL.AVE_AVAIL_TCH_SUM.Sum.Sum)+(RESAVAIL.AVE_GPRS_CHANNELS_SUM.Sum.Sum)))= 0),((CellAvailableTime.Granularity.Sum.Sum)),((RESAVAIL.BCCH_DOWNTIME.Sum.Sum))))), 0)))\":{\"C62#12788\":\"RESAVAIL.AVE_GPRS_CHANNELS_SUM.Sum.Sum\", \"C60#12789\":\"RESAVAIL.AVE_AVAIL_TCH_SUM.Sum.Sum\", \"C93#12790\":\"RESAVAIL.BCCH_DOWNTIME.Sum.Sum\", \"C1#20473\":\"CellAvailableTime.Granularity.Sum.Sum\"}, \"9975,G0681##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0) || IF(EQUALS(#M22#,\\\"P\\\"),1,0),((KPI#6438)), 0)))\":{}, \"7598,G1799##((IF(IF(EQUALS(#M22#,\\\"A\\\"),1,0),1,0)))\":{\"C2#13335\":\"RAN_2G_Beacon.S12_2G_ASR_NUM.Sum.Sum\"}}";
        Map<String, Map<String, String>> kpiFormulaFinalMap = new Gson().fromJson(formula, Map.class);
        String genericKPIFormula = "{\"G1801\":\"(KPI#G1800)+(KPI#G1799)\",\"G1797\":\"(IF(IF(EQUALS(#M33#,\\\"VFinTF\\\"),1,0) || IF(EQUALS(#M33#,\\\"TFinTF\\\"),1,0) || IF(EQUALS(#M33#,\\\"TFinVF\\\"),1,0),1,0))\",\"G0092\":\"(100)*((((KPI#G0090))-((KPI#G0091)))/((KPI#G0090)))\"}";
        Map<String, String> generickpiFormulaFinalMap = new Gson().fromJson(genericKPIFormula, Map.class);
        Set<String> finalkpiIdList = new HashSet<String>(Arrays.asList("G0092,G0682,G0681,G1797,G1801".split(",")));
        System.out.println("finalkpiIdList : " + finalkpiIdList);
        // extractMagnetKPIMap(kpiFormulaFinalMap, generickpiFormulaFinalMap,
        // finalkpiIdList);

    }

    private static void extractMagnetKPIMap(Map<String, Map<String, String>> kpiFormulaFinalMap,
            Map<String, String> generickpiFormulaFinalMap, Set<String> finalkpiIdList, JobContext jobcontext) {
        Map<String, Map<String, String>> magnetKPIMap = new HashMap<>();
        Map<String, Map<String, String>> finalkpiFormulaMap = new HashMap<>();
        Map<String, String> finalGenericFormulaMap = new HashMap<>();
        Map<String, String> magnetGenericKPIMap = new HashMap<>();
        Set<String> kpiIdsMagnet = new HashSet<>();
        Set<String> magnetKPIIds = new HashSet<>();
        for (Entry<String, Map<String, String>> kpiFormula : kpiFormulaFinalMap.entrySet()) {
            String formulaeString = kpiFormula.getKey();
            if (formulaeString.contains("#$") || formulaeString.contains("(EQUALS(#")
                    || formulaeString.contains("(NOT_EQUALS(#)") || formulaeString.contains("LEFT(")
                    || formulaeString.contains("RIGHT(")) {
                if (formulaeString.contains("KPI#")) {
                    final Pattern operatorPattern = Pattern.compile("KPI#(G?[0-9]{0,4})");
                    final Matcher operatorMatcher = operatorPattern.matcher(formulaeString);
                    while (operatorMatcher.find()) {
                        String matchkey = operatorMatcher.group(1);
                        kpiIdsMagnet.add(matchkey);
                    }
                }
                magnetKPIMap.put(formulaeString, kpiFormula.getValue());
                String formulaId = StringUtils.substringBeforeLast(formulaeString, "##");
                if (formulaId.contains(",")) {
                    for (String kpiId1 : formulaId.split(",")) {
                        magnetKPIIds.add(kpiId1);
                    }
                } else {
                    magnetKPIIds.add(formulaId);
                }
            } else {
                finalkpiFormulaMap.put(formulaeString, kpiFormula.getValue());
            }
        }
        for (Entry<String, Map<String, String>> kpiFormula : kpiFormulaFinalMap.entrySet()) {
            String formulaId = StringUtils.substringBeforeLast(kpiFormula.getKey(), "##");
            for (String kpiId : kpiIdsMagnet) {
                if (formulaId.contains(kpiId)) {
                    magnetKPIMap.put(kpiFormula.getKey(), kpiFormula.getValue());
                }
            }

        }
        for (Entry<String, String> genericFormula : generickpiFormulaFinalMap.entrySet()) {
            String formulaString = genericFormula.getValue();
            if (formulaString.contains("#$") || formulaString.contains("(EQUALS(#")
                    || formulaString.contains("(NOT_EQUALS(#)") || formulaString.contains("LEFT(")
                    || formulaString.contains("RIGHT(")) {
                if (formulaString.contains("KPI#")) {
                    final Pattern operatorPattern = Pattern.compile("KPI#(G?[0-9]{0,4})");
                    final Matcher operatorMatcher = operatorPattern.matcher(formulaString);
                    while (operatorMatcher.find()) {
                        String matchkey = operatorMatcher.group(1);
                        magnetKPIIds.add(matchkey);
                    }
                }
                magnetGenericKPIMap.put(genericFormula.getKey(), formulaString);
            } else {
                finalGenericFormulaMap.put(genericFormula.getKey(), formulaString);
            }

        }
        for (Entry<String, String> generickpiFormula : generickpiFormulaFinalMap.entrySet()) {
            String formulaId = generickpiFormula.getValue();
            for (String kpiId : magnetKPIIds) {
                if (formulaId.contains(kpiId)) {
                    magnetGenericKPIMap.put(generickpiFormula.getKey(), generickpiFormula.getValue());
                    finalGenericFormulaMap.remove(generickpiFormula.getKey());
                }
            }

        }
        magnetKPIIds.addAll(magnetGenericKPIMap.keySet());
        finalkpiIdList.retainAll(magnetKPIIds);
        jobcontext.setParameters("finalkpiIdList", String.join(",", finalkpiIdList));
        jobcontext.setParameters("finalMagnetKPIMap", new Gson().toJson(magnetKPIMap));
        jobcontext.setParameters("finalMagnetGenericKPIMap", new Gson().toJson(magnetGenericKPIMap));
        jobcontext.setParameters("genericKPIWiseFormulaDesc", new Gson().toJson(finalGenericFormulaMap));
        jobcontext.setParameters("kpiFormulaFinalMap", new Gson().toJson(finalkpiFormulaMap));
    }

    private static String getTimeShiftQuery(Map<String, Map<String, String>> kpiFormulaFinalMap, String normalKPI,
            Map<String, List<String>> timeShiftWiseDateMap, String frequency) {
        logger.info(
                "kpiFormulaFinalMap : {} and normalKPICounter : {} and timeShiftWiseDateMap : {} and frequency : {} ",
                kpiFormulaFinalMap, normalKPI, timeShiftWiseDateMap, frequency);
        int amount;
        String format = "yyyyMMddHH";
        String finalShiftQuery = Symbol.EMPTY_STRING;
        StringBuilder selectTimeKPI = new StringBuilder();
        StringBuilder selectTimeQuery = new StringBuilder();
        StringBuilder condition = new StringBuilder(" where ");
        String normalKPIVal = "ts.`" + normalKPI.replace(",", "`,ts.`") + "`";
        StringBuilder timeQuery = new StringBuilder();
        Set<String> timeShiftKPI = kpiFormulaFinalMap.entrySet().stream().filter(e -> e.getValue().isEmpty())
                .map(e -> e.getKey()).collect(Collectors.toSet());
        kpiFormulaFinalMap.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty() && e.getKey().contains(").TimeShift(")).map(e -> e.getKey())
                .collect(Collectors.toMap(null, null));

        if (!timeShiftKPI.isEmpty()) {
            normalKPIVal = normalKPIVal + ",";
        }

        Map<String, List<String>> timeKPIValMap = new HashMap<>();
        for (String timeKPI : timeShiftKPI) {
            if (timeKPI.contains("TimeShift(")) {
                List<String> kpiIdList = new ArrayList<>();
                Integer timeVal = Integer
                        .parseInt(StringUtils.substringBetween(timeKPI, "TimeShift(", Symbol.PARENTHESIS_CLOSE_STRING));
                String kpiId = StringUtils.substringBeforeLast(timeKPI, "##");
                kpiIdList.add(kpiId);
                List<String> list = timeKPIValMap.get(String.valueOf(timeVal));
                if (!timeKPIValMap.isEmpty() && list != null) {
                    list.add(kpiId);
                    timeKPIValMap.put(String.valueOf(timeVal), list);
                } else {
                    timeKPIValMap.put(String.valueOf(timeVal), kpiIdList);
                }
            }
        }
        List<List<String>> ts0List = timeShiftWiseDateMap.entrySet().stream().filter(e -> e.getKey().equals("0"))
                .map(e -> e.getValue()).collect(Collectors.toList());
        String ts0Val = String.join(",", ts0List.get(0));
        if (frequency.equals("PERDAY") || frequency.equals("PERWEEK") || frequency.equals("PERMONTH")) {
            ts0Val = ts0Val.replace(",", "00,") + "00";
        }
        for (Entry<String, List<String>> timeShiftkpi : timeKPIValMap.entrySet()) {
            Integer timeVal = Integer.parseInt(timeShiftkpi.getKey());
            List<List<String>> tsList = timeShiftWiseDateMap.entrySet().stream()
                    .filter(e -> e.getKey().equals(String.valueOf(timeVal))).map(e -> e.getValue())
                    .collect(Collectors.toList());
            String tsVal = String.join(",", tsList.get(0));
            String timeShiftKPIIds = String.join(",", timeShiftkpi.getValue());
            String timeShiftVal = "ts" + timeShiftkpi.getKey() + ".`"
                    + timeShiftKPIIds.replace(",", "`,ts" + timeShiftkpi.getKey() + ".`") + "`";
            String key = Symbol.EMPTY_STRING;
            if (frequency.equals("15 Min")) {
                format = "yyyyMMddHHmm";
                amount = (15 * timeVal);
                key = amount + " minutes ";
            } else if (frequency.equals("PERHOUR")) {
                amount = (timeVal);
                key = amount + " hours ";
            } else if (frequency.equals("PERDAY")) {
                tsVal = tsVal.replace(",", "00,") + "00";
                amount = (24 * timeVal);
                key = amount + " hours ";
            } else if (frequency.equals("PERWEEK")) {
                tsVal = tsVal.replace(",", "00,") + "00";
                amount = (24 * 7 * timeVal);
                key = amount + " hours ";
            }
            selectTimeKPI.append(timeShiftVal + ",");
            condition.append(" ts.DATE = ts" + timeVal + ".DATE and ");
            selectTimeQuery.append(" (select DATE_FORMAT(CAST(UNIX_TIMESTAMP(DATE, '" + format
                    + "') as TIMESTAMP) + INTERVAL " + key + " ,'" + format + "') as DATE ,`"
                    + timeShiftKPIIds.replace(",", "`,`") + "`  from PerformanceKPIFinalData where DATE in ('20"
                    + tsVal.replace(",", "','20") + "')) as ts" + timeVal + ",");
        }
        timeQuery.append("select DATE_FORMAT(CAST(UNIX_TIMESTAMP(ts.DATE, '" + format
                + "') as TIMESTAMP),'dd/MM/yyyy') as DATE,ts.TIME,ts.DL1,ts.DL2,ts.DL3,ts.DL4,ts.T,ts.H1,ts.ENB,ts.CEL,ts.NAM,ts.BND,ts.NS,ts.M33,"
                + normalKPIVal + StringUtils.substringBeforeLast(selectTimeKPI.toString(), ",")
                + " from ( select DATE,TIME,DL1,DL2,DL3,DL4,T,H1,ENB,CEL,NAM,BND,NS,M33,`"
                + normalKPI.replace(",", "`,`") + "` from PerformanceKPIFinalData where DATE in ('20"
                + ts0Val.replace(",", "','20") + "')) as ts , "
                + StringUtils.substringBeforeLast(selectTimeQuery.toString(), ",")
                + StringUtils.substringBeforeLast(condition.toString(), " and"));
        finalShiftQuery = timeQuery.toString();
        return finalShiftQuery;
    }

    private void getkpiFormulaMap(String kpiIds) {

        logger.info("Getting KPI Formula Map With KPI IDs={}", kpiIds);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            Map<String, String> contextMap = new HashMap<>();
            contextMap = jobcontext.getParameters();
            String colFilter = contextMap.get("metaColumn");
            logger.info("Column Filter={}", colFilter);

            String genericKPIs = contextMap.get("genericKPIs");
            logger.info("Generic KPIs={}", genericKPIs);

            String genericKpiMappingByGenericKpis = contextMap.get("genericKpiMappingByGenericKpiList");
            logger.info("Generic KPI Mapping By Generic KPIs={}", genericKpiMappingByGenericKpis);

            Map<String, List<String>> genericKPIWithKPI = new Gson().fromJson(genericKpiMappingByGenericKpis,
                    Map.class);
            logger.info("Generic KPI With KPI={}", genericKPIWithKPI);

            String timeShiftedKPIs = contextMap.get("timeShiftedKPIs");
            logger.info("Time Shifted KPIs={}", timeShiftedKPIs);

            String sqlQuery = "SELECT CONCAT(kf.KPI_CODE, '##', kf.KPI_FORMULA_DESC) AS formula, CAST(COALESCE(CONCAT('C', kc.SEQUENCE_NO, '#', pmc.PM_COUNTER_VARIABLE_ID_PK),'null') AS BINARY), pmc.UNIQUE_STRING AS UNIQUESTRING FROM KPI_FORMULA kf "
                    + "LEFT JOIN (FORMULA_COUNTER_MAPPING fcm JOIN PM_COUNTER_VARIABLE pmc) ON fcm.KPI_FORMULA_ID_FK = kf.KPI_FORMULA_ID_PK AND fcm.PM_COUNTER_VARIABLE_ID_FK = pmc.PM_COUNTER_VARIABLE_ID_PK  "
                    + "LEFT JOIN GENERIC_KPI_MAPPING gkm on (kf.KPI_FORMULA_ID_PK = gkm.KPI_FORMULA_ID_FK) "
                    + "LEFT JOIN PM_GENERIC_KPI gk on (gkm.PM_GENERIC_KPI_ID_FK = gk.PM_GENERIC_KPI_ID_PK) LEFT JOIN KPI_COUNTER kc ON kc.KPI_COUNTER_ID_PK = pmc.KPI_COUNTER_ID_FK WHERE kf.domain = '"
                    + contextMap.get("domain") + "' AND kf.DELETED = 0 ";

            if (StringUtils.isNotEmpty(contextMap.get("vendor")) && !contextMap.get("vendor").equalsIgnoreCase("ALL")) {
                sqlQuery = sqlQuery + "AND kf.VENDOR IN ('" + contextMap.get("vendor") + "') ";
            }
            if (StringUtils.isNotEmpty(kpiIds)) {
                sqlQuery = sqlQuery + "and kf.KPI_CODE in (" + kpiIds + ") ";
            }
            logger.info("KPI Formula Query: {}", sqlQuery);

            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(sqlQuery);
            resultSet = preparedStatement.executeQuery();
            kpiFormulaFinalMap = new THashMap<>();
            while (resultSet.next()) {
                String genericKPICode = Symbol.EMPTY_STRING;
                if (genericKPIWithKPI != null
                        && genericKPIWithKPI
                                .containsKey(StringUtils.substringBeforeLast(resultSet.getString(1), "##"))) {
                    genericKPICode = Symbol.COMMA_STRING + String.join(Symbol.COMMA_STRING,
                            genericKPIWithKPI.get(StringUtils.substringBeforeLast(resultSet.getString(1), "##")));
                }
                String key = StringUtils.substringBeforeLast(resultSet.getString(1), "##") + genericKPICode + "##"
                        + StringUtils.substringAfterLast(resultSet.getString(1), "##");
                if (!kpiFormulaFinalMap.isEmpty() && kpiFormulaFinalMap.containsKey(key)) {
                    Map<String, String> counterUniqueStringMap = kpiFormulaFinalMap.get(key);
                    counterUniqueStringMap.put(String.valueOf(resultSet.getString(2)), resultSet.getString(3));
                    kpiFormulaFinalMap.put(key, counterUniqueStringMap);
                } else {
                    Map<String, String> counterUniqueStringMap = new HashMap<>();
                    counterUniqueStringMap.put(String.valueOf(resultSet.getString(2)), resultSet.getString(3));
                    kpiFormulaFinalMap.put(key, counterUniqueStringMap);
                }
            }
            Map<String, Map<String, String>> tempKpiFormulaMap = new HashMap<>(kpiFormulaFinalMap);
            if (StringUtils.isNotEmpty(timeShiftedKPIs)) {
                for (Entry<String, Map<String, String>> kpiFormula : tempKpiFormulaMap.entrySet()) {
                    List<String> timeShiftedKPIList = Arrays.asList(timeShiftedKPIs.split(","));
                    String kpiCode = StringUtils.substringBeforeLast(kpiFormula.getKey(), "##");
                    String kpiDesc = StringUtils.substringAfterLast(kpiFormula.getKey(), "##");
                    getPossibleTimeShiftKPIWiseMap1(kpiFormulaFinalMap, timeShiftedKPIList, kpiFormula, kpiCode,
                            kpiDesc);
                }
            }
            logger.info("KPI Formula Map: {}", kpiFormulaFinalMap);
        } catch (Exception e) {
            logger.error("Exception While Fetching KPI Formula Map, Message={}, Error={}", e.getMessage(), e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    logger.error("Exception While Closing Statement in KPI Map, Message={}, Error={}", e.getMessage(),
                            e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Exception While Closing Connection, Message={}, Error={}", e.getMessage(), e);
                }
            }
        }
    }

    private void getPossibleTimeShiftKPIWiseMap1(Map<String, Map<String, String>> kpiWiseCounterMap,
            List<String> commonSubKpiListForTimeShift, Entry<String, Map<String, String>> kpiWiseCounter,
            String kpiCode, String kpiDesc) {
        for (String subkpiList : commonSubKpiListForTimeShift) {
            int count = 0;
            if (subkpiList.contains("TimeShift") && subkpiList.contains(kpiCode)) {
                String timeShift = subkpiList.split("\\.")[1];
                String temp = kpiDesc;
                String[] splitDesc = temp.split("KPI#");
                for (String string : splitDesc) {
                    count++;
                    if (string.contains("))") && count > 1) {
                        String before = StringUtils.substringBefore(string, "))");
                        String latest = before + "))." + timeShift;
                        temp = StringUtils.replace(temp, before + ")", latest);
                    }
                }
                kpiWiseCounterMap.put(subkpiList + "##" + temp, kpiWiseCounter.getValue());
            }
        }
    }

    private Map<String, List<String>> getTimeShiftMapForCounters(List<String> quarterList, String frequency,
            Map<String, List<String>> finaltimeShiftWiseDateMap) {
        Map<String, List<String>> map = new HashMap<>();
        List<String> reducedDateList = getReducedDateList(frequency, quarterList);
        List<String> sortedDateList = reducedDateList.stream().sorted().collect(Collectors.toList());
        map.put("0", sortedDateList);
        finaltimeShiftWiseDateMap.put("0", sortedDateList);
        return map;
    }

    private static Map<String, List<String>> getTimeShifWiseQuarterList(String frequency, Set<String> timeShiftedKPIs,
            List<String> quarterlyDateList, Map<String, List<String>> finaltimeShiftWiseDateMap) {

        logger.info(
                "Getting TimeShift Wise Quarter List With Frequency={} and TimeShifted KPIs={}, Quarterly Date List={}, Final TimeShift Wise Date Map={}",
                frequency, timeShiftedKPIs, quarterlyDateList, finaltimeShiftWiseDateMap);
        // Getting TimeShift Wise Quarter List With Frequency=15 Min and TimeShifted
        // KPIs=[], Quarterly Date List=[2025-01-01, 2025-04-01, 2025-07-01,
        // 2025-10-01], Final TimeShift Wise Date Map={0=[2025-01-01, 2025-04-01,
        // 2025-07-01, 2025-10-01]}

        Map<String, List<String>> timeShiftWiseDateMap = new HashMap<>();
        List<String> finalReducedDateList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(timeShiftedKPIs)) {
            for (String timeShiftKpi : timeShiftedKPIs) {
                List<String> timeShitedQuarterlyList = new ArrayList<>();
                try {
                    Integer timeShift = Integer.parseInt(StringUtils.substringAfter(timeShiftKpi, "TimeShift("));
                    if (frequency.equalsIgnoreCase("15 Min")) {
                        getQuarterlyTimeShift(timeShitedQuarterlyList, quarterlyDateList, timeShift);
                    } else if (frequency.equalsIgnoreCase("PERHOUR")) {
                        getHourlyTimeShift(timeShitedQuarterlyList, quarterlyDateList, timeShift);
                    } else {
                        getDailyTimeShift(timeShitedQuarterlyList, quarterlyDateList, timeShift, frequency);
                    }
                    List<String> reducedDateList = getReducedDateList(frequency, timeShitedQuarterlyList);
                    finalReducedDateList = reducedDateList.stream().sorted().collect(Collectors.toList());
                    timeShiftWiseDateMap.put(String.valueOf(timeShift), finalReducedDateList);
                    finaltimeShiftWiseDateMap.put(String.valueOf(timeShift), timeShitedQuarterlyList);
                } catch (Exception e) {
                    List<String> reducedDateList = getReducedDateList(frequency, quarterlyDateList);
                    finalReducedDateList = reducedDateList.stream().sorted().collect(Collectors.toList());
                    finaltimeShiftWiseDateMap.put("0", quarterlyDateList);
                    timeShiftWiseDateMap.put("0", finalReducedDateList);
                }
            }
        } else {
            List<String> reducedDateList = getReducedDateList(frequency, quarterlyDateList);
            finalReducedDateList = reducedDateList.stream().sorted().collect(Collectors.toList());
            finaltimeShiftWiseDateMap.put("0", quarterlyDateList);
            timeShiftWiseDateMap.put("0", finalReducedDateList);

        }
        return timeShiftWiseDateMap;
    }

    private static void getQuarterlyTimeShift(List<String> timeShiftedQuarterList, List<String> dates,
            Integer timeShift) throws ParseException {
        for (String date : dates) {
            Date dateformat = new SimpleDateFormat("yyMMddHHmm").parse((StringUtils.substringBefore(date, "*")));
            SimpleDateFormat formatter = new SimpleDateFormat("yyMMddHHmm");
            Calendar c = Calendar.getInstance();
            c.setTime(dateformat);
            int amount = -(15 * timeShift);
            c.add(Calendar.MINUTE, amount);
            String timeshiftedDate = formatter.format(c.getTime());
            timeShiftedQuarterList.add(timeshiftedDate);// + "*"
        }
    }

    private static void getHourlyTimeShift(List<String> timeShiftedQuarterList, List<String> dates, Integer timeShift)
            throws ParseException {
        for (String date : dates) {
            Date dateformat = new SimpleDateFormat("yyMMddHHmm").parse((StringUtils.substringBefore(date, "*") + "00"));
            SimpleDateFormat formatter = new SimpleDateFormat("yyMMddHHmm");
            Calendar c = Calendar.getInstance();
            c.setTime(dateformat);
            int amount = -(timeShift);
            c.add(Calendar.HOUR, amount);
            String timeshiftedDate = formatter.format(c.getTime());
            String timeShiftedDay = StringUtils.substring(timeshiftedDate, 0, 8);// + "*"
            timeShiftedQuarterList.add(timeShiftedDay);
        }
    }

    private static void getDailyTimeShift(List<String> timeShiftedQuarterList, List<String> dates, Integer timeShift,
            String frequency) throws ParseException {
        for (String date : dates) {
            Date dateformat = new SimpleDateFormat("yyMMddHHmm")
                    .parse((StringUtils.substringBefore(date, "*") + "0000"));
            SimpleDateFormat formatter = new SimpleDateFormat("yyMMddHHmm");
            Calendar c = Calendar.getInstance();
            c.setTime(dateformat);
            int amount = -(timeShift);
            if (frequency.equalsIgnoreCase("PERDAY")) {
                c.add(Calendar.DATE, amount);
            } else if (frequency.equalsIgnoreCase("PERWEEK")) {
                c.add(Calendar.WEEK_OF_YEAR, amount);
            } else if (frequency.equalsIgnoreCase("PERMONTH")) {
                c.add(Calendar.MONTH, amount);
            }
            String timeshiftedDate = formatter.format(c.getTime());
            String timeShiftedDay = StringUtils.substring(timeshiftedDate, 0, 6);// + "*"
            timeShiftedQuarterList.add(timeShiftedDay);
        }
    }

    private String getEmsTypeOnDomainVendor(String domain, String vendor) {
        String emsType = null;
        if (domain.equalsIgnoreCase("RAN") && vendor.equalsIgnoreCase("NOKIA")) {
            emsType = "NOKIA_RAN";
        }
        if (domain.equalsIgnoreCase("RAN") && vendor.equalsIgnoreCase("VODAFONE")) {
            emsType = "BEACON_RAN";
        }
        if (domain.equalsIgnoreCase("RAN") && vendor.equalsIgnoreCase("ERICSSON")) {
            emsType = "ERICSSON_RAN";
        }
        if (domain.equalsIgnoreCase("CORE") && vendor.equalsIgnoreCase("ERICSSON")) {
            emsType = "5G_Packet_Core";
        }
        if (domain.equalsIgnoreCase("TRANSPORT") && vendor.equalsIgnoreCase("CISCO")) {
            emsType = "NA";
        }
        if (domain.equalsIgnoreCase("TRANSPORT") && vendor.equalsIgnoreCase("NOKIA")) {
            emsType = "NA";
        }
        if (domain.equalsIgnoreCase("TRANSPORT") && vendor.equalsIgnoreCase("JUNIPER")) {
            emsType = "NA";
        }
        if (domain.equalsIgnoreCase("TRANSPORT") && vendor.equalsIgnoreCase("ADVA")) {
            emsType = "NA";
        }

        return emsType;

    }

    private Map<String, List<Map<String, String>>> getCounterListMapByCategory(JSONObject configJson,
            Map<String, Map<String, String>> counterNameWiseInfoMap, Map<String, String> counterAggrMap)
            throws JSONException {
        List<Map<String, String>> finalCounterInfoMapList = new ArrayList<Map<String, String>>();
        Map<String, List<Map<String, String>>> catgoryInfoMap = getMOCounterListMap(configJson,
                finalCounterInfoMapList);
        logger.info("catgoryInfoMap MOCountListMap : {} ", catgoryInfoMap);
        String kpiString = getValueFromJson(configJson, PMConstants.KPI_JSONKEY);
        try {
            org.json.JSONArray kpiJson = new JSONArray(kpiString);
            for (Integer i = 0; i < kpiJson.length(); i++) {
                JSONObject jsonObject = kpiJson.getJSONObject(i);
                if (jsonObject.getString("type").equalsIgnoreCase("Counter")) {
                    String categoryName = jsonObject.getString("categoryName");
                    String categoryId = jsonObject.getString("categoryId");
                    categoryName = categoryName + "@" + categoryId;
                    List<Map<String, String>> counterInfoMapList = new ArrayList<Map<String, String>>();
                    if (catgoryInfoMap.containsKey(categoryName)) {
                        counterInfoMapList = catgoryInfoMap.get(categoryName);

                    }
                    Map<String, String> counterInfoMap = new HashMap<String, String>();
                    String counterHeaderName = jsonObject.getString("headerName");
                    String pmcountervariableid_pk = jsonObject.getString("kpicode");
                    String nodeAggregation = jsonObject.getString("nodeAggregation");
                    String timeAggregation = jsonObject.getString("timeAggregation");
                    String sequenceNo = jsonObject.getString("sequenceNo");
                    counterInfoMap.put("counterHeaderName", counterHeaderName);
                    counterInfoMap.put("pmcountervariableid_pk", pmcountervariableid_pk);
                    counterInfoMap.put("categoryName", categoryName);
                    counterInfoMap.put("nodeAggregation", nodeAggregation);
                    counterInfoMap.put("timeAggregation", timeAggregation);
                    counterInfoMap.put("nodeAggr", nodeAggregation);
                    counterInfoMap.put("timeAggr", timeAggregation);
                    counterInfoMap.put("sequenceNo", sequenceNo);
                    counterInfoMap.put("isKPICounter", "1");
                    counterInfoMapList.add(counterInfoMap);
                    catgoryInfoMap.put(categoryName, counterInfoMapList);
                    finalCounterInfoMapList.add(counterInfoMap);
                    if (!counterNameWiseInfoMap.containsKey("C" + sequenceNo + "#" + pmcountervariableid_pk)) {
                        counterNameWiseInfoMap.put("C" + sequenceNo + "#" + pmcountervariableid_pk, counterInfoMap);
                    }

                    if (!counterAggrMap.containsKey("C" + sequenceNo + "#" + pmcountervariableid_pk)) {
                        counterAggrMap.put("C" + sequenceNo + "#" + pmcountervariableid_pk,
                                nodeAggregation + Symbol.HASH_STRING + timeAggregation);
                    }

                } else {
                    logger.info("finalCounterInfoMapList : {} ", finalCounterInfoMapList);
                    String kpiName = getValueFromJson(jsonObject, "kpiName");
                    String kpiCode = kpiName.split(Symbol.HYPHEN_STRING)[0];
                    getMOkpiInfo(catgoryInfoMap, counterAggrMap, counterNameWiseInfoMap, finalCounterInfoMapList,
                            kpiCode);

                }
            }

        } catch (Exception e) {
            logger.error("Exception in getting Hbase Read Column from KPIJson :{}", ExceptionUtils.getStackTrace(e));
        }
        return catgoryInfoMap;
    }

    private void getMOkpiInfo(Map<String, List<Map<String, String>>> catgoryInfoMap, Map<String, String> counterAggrMap,
            Map<String, Map<String, String>> counterNameWiseInfoMap, List<Map<String, String>> counterInfoMap,
            String kpiCode) {
        String counterIds = null;
        jobcontext.setParameters("MoKPI", kpiCode);
        Map<String, Map<String, String>> kpiCounterInfoMap = getCounterInfoMap(jobcontext);
        logger.info("kpiCounterInfoMap : {}  and counterInfoMap : {} ", kpiCounterInfoMap, counterInfoMap);
        for (Entry<String, Map<String, String>> kpiCounterInfo : kpiCounterInfoMap.entrySet()) {
            if (!counterNameWiseInfoMap.containsKey(kpiCounterInfo.getKey())) {
                counterNameWiseInfoMap.put(kpiCounterInfo.getKey(), kpiCounterInfo.getValue());
            }

        }
        counterIds = kpiCounterInfoMap.entrySet().stream()
                .map(e -> e.getValue().get("sequenceNo") + "#" + e.getValue().get("pmcountervariableid_pk")).distinct()
                .collect(Collectors.joining(","));
        jobcontext.setParameters("counterIds", counterIds);
        HashMap<String, String> parameters = jobcontext.getParameters();
        Map<String, String> kpicounterAggrMap = getCounterAggrMap(parameters);
        for (Entry<String, String> kpiCounterAggrInfo : kpicounterAggrMap.entrySet()) {
            if (!counterAggrMap.containsKey(kpiCounterAggrInfo.getKey())) {
                counterAggrMap.put(kpiCounterAggrInfo.getKey(), kpiCounterAggrInfo.getValue());
            }
        }
        for (Map<String, String> value : kpiCounterInfoMap.values()) {
            Map<String, String> infoMap = new HashMap<>();
            infoMap.put("sequenceNo", value.get("sequenceNo"));
            infoMap.put("categoryName", value.get("categoryName"));
            infoMap.put("counterHeaderName", value.get("counterHeaderName"));
            infoMap.put("pmcountervariableid_pk", value.get("pmcountervariableid_pk"));
            if (value.get("subcategoryHeader1") != null && !value.get("subcategoryHeader1").equalsIgnoreCase("null")
                    && value.get("subcategoryValue1") != null
                    && !value.get("subcategoryValue1").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader1", value.get("subcategoryHeader1"));
                infoMap.put("subcategoryValue1", value.get("subcategoryValue1"));
            }
            if (value.get("subcategoryHeader2") != null && !value.get("subcategoryHeader2").equalsIgnoreCase("null")
                    && value.get("subcategoryValue2") != null
                    && !value.get("subcategoryValue2").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader2", value.get("subcategoryHeader2"));
                infoMap.put("subcategoryValue2", value.get("subcategoryValue2"));
            }
            if (value.get("subcategoryHeader3") != null && !value.get("subcategoryHeader3").equalsIgnoreCase("null")
                    && value.get("subcategoryValue3") != null
                    && !value.get("subcategoryValue3").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader3", value.get("subcategoryHeader3"));
                infoMap.put("subcategoryValue3", value.get("subcategoryValue3"));
            }
            if (value.get("subcategoryHeader4") != null && !value.get("subcategoryHeader4").equalsIgnoreCase("null")
                    && value.get("subcategoryValue4") != null
                    && !value.get("subcategoryValue4").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader4", value.get("subcategoryHeader4"));
                infoMap.put("subcategoryValue4", value.get("subcategoryValue4"));
            }
            counterInfoMap.add(infoMap);
            catgoryInfoMap.put(value.get("categoryName"), counterInfoMap);

        }

    }

    private Map<String, String> getCounterAggrMap(Map<String, String> contextMap) {

        Connection connection = null;
        Map<String, String> counterMap = null;
        PreparedStatement preparedStatement = null;
        try {
            String counterIds = contextMap.get("counterIds");
            logger.info("Counter IDs={}", counterIds);
            logger.info("Domain={}", contextMap.get("domain"));
            logger.info("Vendor={}", contextMap.get("vendor"));

            String sqlQuery = "SELECT CONCAT('C', kc.SEQUENCE_NO, '#', pc.PM_COUNTER_VARIABLE_ID_PK) AS Id, " +
                    "CONCAT(pc.NODE_AGGREGATION, '#', pc.TIME_AGGREGATION) AS aggrDetails " +
                    "FROM PM_COUNTER_VARIABLE pc " +
                    "JOIN PM_CATEGORY c ON pc.PM_CATEGORY_ID_FK = c.PM_CATEGORY_ID_PK " +
                    "JOIN PM_NODE_VENDOR pn ON c.PM_NODE_VENDOR_ID_FK = pn.PM_NODE_VENDOR_ID_PK " +
                    "JOIN KPI_COUNTER kc ON kc.KPI_COUNTER_ID_PK = pc.KPI_COUNTER_ID_FK " +
                    "WHERE pn.DOMAIN = '" + contextMap.get("domain") + "' " +
                    "AND pc.NODE_AGGREGATION IS NOT NULL AND pc.NODE_AGGREGATION != '' " +
                    "AND pc.TIME_AGGREGATION IS NOT NULL AND pc.TIME_AGGREGATION != '' ";

            if (!com.enttribe.commons.lang.StringUtils.isEmpty(counterIds)) {
                sqlQuery += "AND pc.PM_COUNTER_VARIABLE_ID_PK IN (" + counterIds + ") ";
            }

            if (StringUtils.isNotEmpty(contextMap.get("vendor")) &&
                    !"ALL".equalsIgnoreCase(contextMap.get("vendor"))) {
                sqlQuery += "AND pn.VENDOR IN ('" + contextMap.get("vendor") + "') ";
            }

            logger.info("MySQL Query For Counter Aggr Map: {}", sqlQuery);

            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(sqlQuery);
            ResultSet resultSet = preparedStatement.executeQuery();
            counterMap = new HashMap<>();
            while (resultSet.next()) {
                counterMap.put(String.valueOf(resultSet.getString(1)), resultSet.getString(2));
            }
        } catch (Exception e) {
            logger.error("Exception While Getting Counter Aggr Map, Message={}, Error={}", e.getMessage(), e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    logger.error("Error in Closing Statement in NodeAggregator - {}", e.getMessage());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Error in Closing connection in NodeAggregator- {}", e.getMessage());
                }
            }
        }
        return counterMap;
    }

    private Map<String, List<Map<String, String>>> getMOCounterListMap(JSONObject configJson,
            List<Map<String, String>> finalCounterInfoMapList) throws JSONException {
        Map<String, List<Map<String, String>>> catgoryInfoMap = new HashMap<String, List<Map<String, String>>>();
        try {
            StringBuilder moColumns = new StringBuilder();
            String moHierarchy = getValueFromJson(configJson, "moHierarchy");
            org.json.JSONArray moHierarchyJson = new JSONArray(moHierarchy);
            for (Integer i = 0; i < moHierarchyJson.length(); i++) {
                JSONObject jsonObject = moHierarchyJson.getJSONObject(i);
                String subCatValue = jsonObject.getString("subCatValue");
                if (subCatValue.contains("INDIVIDUAL")) {
                    String categoryName = jsonObject.getString("categoryName");
                    String categoryId = jsonObject.getString("categoryId");
                    categoryName = categoryName + "@" + categoryId;
                    List<Map<String, String>> counterInfoMapList = new ArrayList<Map<String, String>>();
                    if (catgoryInfoMap.containsKey(categoryName)) {
                        counterInfoMapList = catgoryInfoMap.get(categoryName);
                    }
                    Map<String, String> counterInfoMap = new HashMap<String, String>();
                    String counterHeaderName = jsonObject.getString("subCatHeader");
                    moColumns.append(counterHeaderName).append(Symbol.COMMA_STRING);
                    String sequenceNo = jsonObject.getString("sequenceNo");
                    String pmcountervariableid_pk = jsonObject.getString("id");
                    counterInfoMap.put("counterHeaderName", counterHeaderName);
                    counterInfoMap.put("categoryName", categoryName);
                    counterInfoMap.put("sequenceNo", sequenceNo);
                    counterInfoMap.put("isKPICounter", "0");
                    counterInfoMap.put("pmcountervariableid_pk", pmcountervariableid_pk);
                    counterInfoMapList.add(counterInfoMap);
                    catgoryInfoMap.put(categoryName, counterInfoMapList);
                    finalCounterInfoMapList.add(counterInfoMap);
                }
            }
            logger.info("catgoryInfoMap of moHierarchy : {} ", catgoryInfoMap);
            jobcontext.setParameters("moColumns",
                    StringUtils.substringBeforeLast(moColumns.toString(), Symbol.COMMA_STRING));
        } catch (Exception e) {
            logger.error("Exception in getting MO Hierarchy from KPIJson :{}", ExceptionUtils.getStackTrace(e));
        }
        return catgoryInfoMap;
    }

    private static Map<String, List<Map<String, String>>> getCatgoryInfoMap(
            Map<String, Map<String, String>> counterInfoMap) {
        Map<String, List<Map<String, String>>> categoryInfoMap = new HashMap<>();
        for (Map<String, String> value : counterInfoMap.values()) {
            List<Map<String, String>> infoMapList = categoryInfoMap.get(value.get("categoryName"));
            if (infoMapList == null) {
                infoMapList = new ArrayList<>();
            }
            Map<String, String> infoMap = new HashMap<>();
            infoMap.put("sequenceNo", value.get("sequenceNo"));
            infoMap.put("counterHeaderName", value.get("counterHeaderName"));
            infoMap.put("pmcountervariableid_pk", value.get("pmcountervariableid_pk"));
            infoMap.put("uniqueString", value.get("uniqueString"));
            infoMap.put("nodeAggr", value.get("nodeAggr"));
            infoMap.put("timeAggr", value.get("timeAggr"));
            if (value.get("subcategoryHeader1") != null && !value.get("subcategoryHeader1").equalsIgnoreCase("null")
                    && value.get("subcategoryValue1") != null
                    && !value.get("subcategoryValue1").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader1", value.get("subcategoryHeader1"));
                infoMap.put("subcategoryValue1", value.get("subcategoryValue1"));
            }
            if (value.get("subcategoryHeader2") != null && !value.get("subcategoryHeader2").equalsIgnoreCase("null")
                    && value.get("subcategoryValue2") != null
                    && !value.get("subcategoryValue2").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader2", value.get("subcategoryHeader2"));
                infoMap.put("subcategoryValue2", value.get("subcategoryValue2"));
            }
            if (value.get("subcategoryHeader3") != null && !value.get("subcategoryHeader3").equalsIgnoreCase("null")
                    && value.get("subcategoryValue3") != null
                    && !value.get("subcategoryValue3").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader3", value.get("subcategoryHeader3"));
                infoMap.put("subcategoryValue3", value.get("subcategoryValue3"));
            }
            if (value.get("subcategoryHeader4") != null && !value.get("subcategoryHeader4").equalsIgnoreCase("null")
                    && value.get("subcategoryValue4") != null
                    && !value.get("subcategoryValue4").equalsIgnoreCase("null")) {
                infoMap.put("subcategoryHeader4", value.get("subcategoryHeader4"));
                infoMap.put("subcategoryValue4", value.get("subcategoryValue4"));
            }
            infoMapList.add(infoMap);
            categoryInfoMap.put(value.get("categoryName"), infoMapList);
        }
        return categoryInfoMap;
    }

    private Map<String, Map<String, String>> getCounterInfoMap(JobContext jobContext) {

        Map<String, Map<String, String>> counterInfoMap = new HashMap<>();
        Connection connection = null;
        Map<String, String> contextMap = jobContext.getParameters();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        String domain = contextMap.get("domain");
        String vendor = contextMap.get("vendor");
        String moKPI = contextMap.get("MoKPI");
        String kpiIds = contextMap.get("kpiIdsWithSubKpi");

        logger.info("Getting Counter Info Map With Domain={}, Vendor={}, MO KPI={}, KPI IDs={}", domain, vendor, moKPI,
                kpiIds);

        String query = "SELECT DISTINCT " +
                "UPPER(REPLACE(cv.COUNTER, ' ', '')) AS counterHeaderName, " +
                "cv.PM_COUNTER_VARIABLE_ID_PK AS pmcountervariableid_pk, " +
                "UPPER(REPLACE(kc.CATEGORY_ALIAS_NAME, ' ', '')) AS categoryName, " +
                "cv.ATTRIBUTE AS attribute, " +
                "cv.SUBCATEGORY1_VALUE AS subcategory1value, " +
                "cv.SUBCATEGORY2_VALUE AS subcategory2value, " +
                "cv.SUBCATEGORY3_VALUE AS subcategory3value, " +
                "cv.SUBCATEGORY4_VALUE AS subcategory4value, " +
                "CONCAT('C', subcat1.SEQUENCE_NO) AS subcatHeader1, " +
                "CONCAT('C', subcat2.SEQUENCE_NO) AS subcatHeader2, " +
                "CONCAT('C', subcat3.SEQUENCE_NO) AS subcatHeader3, " +
                "CONCAT('C', subcat4.SEQUENCE_NO) AS subcatHeader4, " +
                "kc.SEQUENCE_NO, " +
                "UPPER(REPLACE(pc.PM_CATEGORY_ID_PK, ' ', '')) AS categoryId, " +
                "cv.UNIQUE_STRING, " +
                "cv.NODE_AGGREGATION AS nodeAggr, " +
                "cv.TIME_AGGREGATION AS timeAggr " +
                "FROM KPI_FORMULA kpi " +
                "INNER JOIN FORMULA_COUNTER_MAPPING map ON kpi.KPI_FORMULA_ID_PK = map.KPI_FORMULA_ID_FK " +
                "INNER JOIN PM_COUNTER_VARIABLE cv ON cv.PM_COUNTER_VARIABLE_ID_PK = map.PM_COUNTER_VARIABLE_ID_FK " +
                "INNER JOIN KPI_COUNTER kc ON UPPER(kc.KPI_COUNTER_ID_PK) = UPPER(cv.KPI_COUNTER_ID_FK) " +
                "INNER JOIN PM_CATEGORY pc ON pc.PM_CATEGORY_ID_PK = cv.PM_CATEGORY_ID_FK " +
                "LEFT JOIN KPI_COUNTER subcat1 ON subcat1.KPI_COUNTER_ID_PK = cv.KPI_COUNTER1_ID_FK " +
                "LEFT JOIN KPI_COUNTER subcat2 ON subcat2.KPI_COUNTER_ID_PK = cv.KPI_COUNTER2_ID_FK " +
                "LEFT JOIN KPI_COUNTER subcat3 ON subcat3.KPI_COUNTER_ID_PK = cv.KPI_COUNTER3_ID_FK " +
                "LEFT JOIN KPI_COUNTER subcat4 ON subcat4.KPI_COUNTER_ID_PK = cv.KPI_COUNTER4_ID_FK " +
                "WHERE kpi.DOMAIN = '" + domain + "' ";

        if (!StringUtils.isEmpty(moKPI)) {
            query += "AND kpi.KPI_CODE IN (" + moKPI + ") ";
        } else {
            query += "AND kpi.KPI_CODE IN (" + kpiIds + ") ";
        }

        if (vendor != null && !vendor.equalsIgnoreCase("ALL")) {
            query += "AND kpi.VENDOR = '" + vendor + "' ";
        }

        logger.info("Counter Info Map Query={}", query);

        try {
            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            Class.forName(jdbcDriver);

            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String countername = resultSet.getString(1);
                String pmcountervariableid_pk = resultSet.getString(2);
                String categoryName = resultSet.getString(3) + "@" + resultSet.getString(14);
                String subcategory1value = resultSet.getString(5);
                String subcategory2value = resultSet.getString(6);
                String subcategory3value = resultSet.getString(7);
                String subcategory4value = resultSet.getString(8);
                String subcatHeader1 = resultSet.getString(9);
                String subcatHeader2 = resultSet.getString(10);
                String subcatHeader3 = resultSet.getString(11);
                String subcatHeader4 = resultSet.getString(12);
                String sequenceNo = resultSet.getString(13);
                String uniqueString = resultSet.getString(15);
                String nodeAggr = resultSet.getString(16);
                String timeAggr = resultSet.getString(17);

                Map<String, String> infoMap = counterInfoMap.get(countername);
                if (infoMap == null) {
                    infoMap = new HashMap<>();
                }
                infoMap.put("counterHeaderName", countername);
                infoMap.put("pmcountervariableid_pk", pmcountervariableid_pk);
                infoMap.put("categoryName", categoryName);
                infoMap.put("sequenceNo", sequenceNo);
                infoMap.put("uniqueString", uniqueString);
                infoMap.put("nodeAggr", nodeAggr);
                infoMap.put("timeAggr", timeAggr);
                prepareinfoMap(subcategory1value, subcatHeader1, infoMap, "subcategoryHeader1", "subcategoryValue1");
                prepareinfoMap(subcategory2value, subcatHeader2, infoMap, "subcategoryHeader2", "subcategoryValue2");
                prepareinfoMap(subcategory3value, subcatHeader3, infoMap, "subcategoryHeader3", "subcategoryValue3");
                prepareinfoMap(subcategory4value, subcatHeader4, infoMap, "subcategoryHeader4", "subcategoryValue4");
                counterInfoMap.put("C" + sequenceNo + "#" + pmcountervariableid_pk, infoMap);
            }
        } catch (Exception e) {
            logger.error("Exception While Getting Counter Info Map, Message={}, Error={}", e.getMessage(), e);
        } finally {
            close(connection, preparedStatement, resultSet);
        }
        return counterInfoMap;
    }

    private Map<String, List<String>> getGenericKpiMappingByGenericKpiList(JobContext jobContext,
            Set<String> genericKpiList, Set<String> kpiList) {
        Map<String, List<String>> kpiWithGenericKPIMap = new HashMap<>();
        List<String> kpiIds = new ArrayList<>();
        Map<String, String> aggregationLevel = new HashMap<>();
        Connection conn = null;
        Map<String, String> contextMap = jobContext.getParameters();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String domain = contextMap.get("domain");
        String vendor = contextMap.get("vendor");
        String genericKPIs = genericKpiList.stream().distinct().collect(Collectors.joining(","));
        String sqlQuery = "SELECT " +
                "gkpi.CODE AS code, " +
                "kpi.KPI_CODE AS kpicode, " +
                "gkpi.AGGREGATION AS aggregation " +
                "FROM GENERIC_KPI_MAPPING gm " +
                "JOIN PM_GENERIC_KPI gkpi ON gm.PM_GENERIC_KPI_ID_FK = gkpi.PM_GENERIC_KPI_ID_PK " +
                "JOIN KPI_FORMULA kpi ON gm.KPI_FORMULA_ID_FK = kpi.KPI_FORMULA_ID_PK " +
                "WHERE kpi.DOMAIN = '" + domain + "' " +
                "AND gkpi.CODE IN ('" + genericKPIs.replace(",", "','") + "')";

        if (vendor != null && !vendor.equalsIgnoreCase("ALL"))
            sqlQuery = sqlQuery + " AND kpi.vendor='" + vendor + "'";
        logger.info("getGenericKpiMappingByGenericKpiList sqlQuery is :{}", sqlQuery);
        try {
            final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
            Class.forName(JDBC_DRIVER);
            final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
            final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String genericKPICode = rs.getString(1);
                String kpicode = rs.getString(2);
                String aggregation = rs.getString(3);
                kpiIds.add(kpicode);
                List<String> list = null;
                if (kpiWithGenericKPIMap.containsKey(kpicode)) {
                    list = kpiWithGenericKPIMap.get(kpicode);
                } else {
                    list = new ArrayList<>();
                }
                list.add(genericKPICode);
                aggregationLevel.put(genericKPICode, aggregation);
                kpiWithGenericKPIMap.put(kpicode, list);
            }
            jobContext.setParameters("aggregationLevel", new Gson().toJson(aggregationLevel));
            kpiList.addAll(kpiIds.stream().distinct().collect(Collectors.toSet()));
            logger.info("kpiWithGenericKPIMap size :{} ", kpiWithGenericKPIMap.size());
        } catch (Exception e) {
            logger.error("Exception While getting counterInfoMap :{}", e.getMessage());
        } finally {
            close(conn, stmt, rs);
        }
        return kpiWithGenericKPIMap;
    }

    private Set<String> getGenericKpiList(JobContext jobContext, Set<String> genericKpiList,
            Map<String, String> genericKPIWiseFormulaDesc) {
        Map<String, List<String>> kpiWithGenericKPIMap = new HashMap<>();
        Set<String> kpiIds = new HashSet<>();
        Connection conn = null;
        Map<String, String> contextMap = jobContext.getParameters();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String genericKPIs = genericKpiList.stream().distinct().collect(Collectors.joining(","));
        String sqlQuery = "SELECT "
                + "PM_GENERIC_KPI.CODE AS code, "
                + "PM_GENERIC_KPI.AGGREGATION AS aggregation, "
                + "PM_GENERIC_KPI.KPI_FORMULA_DESC AS kpiformuladesc "
                + "FROM PM_GENERIC_KPI PM_GENERIC_KPI "
                + "WHERE PM_GENERIC_KPI.CODE IN ('" + genericKPIs.replace(",", "','") + "')";

        logger.info("getGenericKpiList sqlQuery is :{}", sqlQuery);
        try {
            final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
            Class.forName(JDBC_DRIVER);
            final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
            final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
            while (rs.next()) {
                if (StringUtils.isNotEmpty(rs.getString(2))) {
                    genericKpiList.add(rs.getString(1));
                } else {
                    if (rs.getString(3).contains("KPI#")) {
                        final Pattern operatorPattern = Pattern.compile("KPI#(G?[0-9]{0,4})");
                        final Matcher operatorMatcher = operatorPattern.matcher(rs.getString(3));
                        while (operatorMatcher.find()) {
                            String matchkey = operatorMatcher.group(1);
                            kpiIds.add(matchkey);
                        }
                    }
                    // kpiIds.addAll(
                    // Arrays.stream(rs.getString(3).split("KPI#"))
                    // .filter(e -> e.contains(Symbol.PARENTHESIS_CLOSE_STRING))
                    // .map(e -> StringUtils.substringBefore(
                    // StringUtils.substringBefore(e, Symbol.PARENTHESIS_CLOSE_STRING), ".")
                    // .replace(")", ""))
                    // .distinct().collect(Collectors.toSet()));
                    genericKPIWiseFormulaDesc.put(rs.getString(1), rs.getString(3));
                }
            }
            genericKpiList.addAll(kpiIds);
            logger.info("kpiWithGenericKPIMap size :{} ", kpiWithGenericKPIMap.size());
            if (CollectionUtils.isNotEmpty(kpiIds)) {
                genericKpiList.addAll(getGenericKpiList(jobContext, kpiIds, genericKPIWiseFormulaDesc));
            }
        } catch (Exception e) {
            logger.error("Exception While getting counterInfoMap :{}", e.getMessage());
        } finally {
            close(conn, stmt, rs);
        }
        return genericKpiList;
    }

    private void getCategoryWiseParquetPath(JobContext jobContext, Set<String> categoryList, String counterParquet,
            String metaParquet) {
        Map<String, String> categoryWiseBasePath = new HashMap<>();
        Map<String, String> categoryWiseMetaParquetPath = new HashMap<>();
        Set<String> kpiIds = new HashSet<>();
        Connection conn = null;
        Map<String, String> contextMap = jobContext.getParameters();
        PreparedStatement stmt = null;
        ResultSet rs = null;
        // String categoryNames =
        // categoryList.stream().filter(e->e.split("@")[0]).distinct().collect(Collectors.joining(","));
        String categoryNames = categoryList.stream().map(e -> e.split("@")[0]).distinct()
                .collect(Collectors.joining(","));
        String categoryIds = categoryList.stream().map(e -> e.split("@")[1]).distinct()
                .collect(Collectors.joining(","));
        String formattedCategoryNames = Arrays.stream(categoryNames.split(","))
                .map(name -> "'" + name.trim() + "'")
                .collect(Collectors.joining(","));

        String formattedCategoryIds = Arrays.stream(categoryIds.split(","))
                .map(id -> "'" + id.trim() + "'")
                .collect(Collectors.joining(","));

        String sqlQuery = "SELECT DISTINCT UPPER(kc.CATEGORY_ALIAS_NAME) AS categoryAliasName, " +
                "nv.DOMAIN, nv.VENDOR, nv.TECHNOLOGY " +
                "FROM KPI_COUNTER kc " +
                "JOIN PM_CATEGORY pm ON kc.PM_CATEGORY_ID_FK = pm.PM_CATEGORY_ID_PK " +
                "JOIN PM_NODE_VENDOR nv ON pm.PM_NODE_VENDOR_ID_FK = nv.PM_NODE_VENDOR_ID_PK " +
                "WHERE kc.CATEGORY_ALIAS_NAME IN (" + formattedCategoryNames + ") " +
                "AND pm.PM_CATEGORY_ID_PK IN (" + formattedCategoryIds + ")";

        logger.info("getCategoryWiseParquetPath sqlQuery is : {}", sqlQuery);

        try {
            final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
            Class.forName(JDBC_DRIVER);
            final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
            final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String emsType = getEmsTypeOnDomainVendor(rs.getString(2), rs.getString(3));
                categoryWiseBasePath.put(rs.getString(1).toUpperCase(), counterParquet + "domain=" + rs.getString(2)
                        + "/vendor=" + rs.getString(3) + "/emstype=" + emsType + "/technology=" +
                        rs.getString(4));
                // + "/vendor=*/emstype=*/technology=" + rs.getString(4));
                categoryWiseMetaParquetPath.put(rs.getString(1).toUpperCase(), metaParquet + "d=" + rs.getString(2)
                        + "/v=" + rs.getString(3) + "/emstype=" + emsType + "/t=" + rs.getString(4) + "/");
                // + "/v=*/emstype=*/t=" + rs.getString(4) + "/");
            }
            jobContext.setParameters("categoryWiseBasePath", new Gson().toJson(categoryWiseBasePath));
            jobContext.setParameters("categoryWiseMetaParquetPath", new Gson().toJson(categoryWiseMetaParquetPath));

        } catch (Exception e) {
            logger.error("Exception While getting counterInfoMap :{}", e.getMessage());
        } finally {
            close(conn, stmt, rs);
        }
    }

    private void prepareinfoMap(String subcategory1value, String subcatHeader1, Map<String, String> infoMap,
            String headerColumn, String valueColumn) {
        if (subcategory1value != null && !subcategory1value.equalsIgnoreCase("null")
                && !subcategory1value.contains("INDIVIDUAL") && !subcategory1value.contains("AGGREGATED")) {
            infoMap.put(headerColumn, subcatHeader1);
            if (infoMap.get(valueColumn) != null && !infoMap.get(valueColumn).equalsIgnoreCase("null")) {
                subcategory1value = infoMap.get(valueColumn) + "','" + subcategory1value;
                infoMap.put(valueColumn, subcategory1value);
            } else {
                infoMap.put(valueColumn, subcategory1value);
            }
        }
    }

    public static Set<String> getSubKPICode(String kpiFormulaDesc) {
        Set<String> kpicodeList = new HashSet<>();
        if (kpiFormulaDesc.contains("KPI#")) {
            final Pattern operatorPattern = Pattern.compile("KPI#(G?[0-9]{0,4})");
            final Matcher operatorMatcher = operatorPattern.matcher(kpiFormulaDesc);
            while (operatorMatcher.find()) {
                String matchkey = operatorMatcher.group(1);
                kpicodeList.add(matchkey);
            }
        }
        // if (kpiFormulaDesc.contains("KPI#")) {
        // kpicodeList.addAll(Arrays
        // .stream(kpiFormulaDesc.split("KPI")).filter(
        // e -> e.contains(Symbol.PARENTHESIS_CLOSE_STRING +
        // Symbol.PARENTHESIS_CLOSE_STRING)
        // && e.contains(Symbol.HASH_STRING))
        // .map(e -> StringUtils
        // .substringBefore(StringUtils.substringBefore(e,
        // Symbol.PARENTHESIS_CLOSE_STRING + Symbol.PARENTHESIS_CLOSE_STRING), ".")
        // .replace("#", "").replace(")", ""))
        // .distinct().collect(Collectors.toSet()));
        // }
        return kpicodeList;
    }

    // public static Set<String> getSubKPICodeForTimeShift(String kpiFormulaDesc) {
    // Set<String> kpicodeList = new HashSet<>();
    // if (kpiFormulaDesc.contains("KPI#")) {
    // kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI"))
    // .filter(e -> e.contains("TimeShift") && e.contains(Symbol.HASH_STRING))
    // .map(e -> StringUtils.substringBetween(e, Symbol.HASH_STRING,
    // Symbol.PARENTHESIS_CLOSE_STRING)
    // + PMConstants.CUSTOM_TIME_SHIFT
    // + StringUtils.substringBetween(e, "TimeShift(",
    // Symbol.PARENTHESIS_CLOSE_STRING))
    // .distinct().collect(Collectors.toSet()));
    // kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI"))
    // .filter(e -> !e.contains("TimeShift")
    // && e.contains(Symbol.PARENTHESIS_CLOSE_STRING +
    // Symbol.PARENTHESIS_CLOSE_STRING)
    // && e.contains(Symbol.HASH_STRING))
    // .map(e -> StringUtils
    // .substringBefore(e, Symbol.PARENTHESIS_CLOSE_STRING +
    // Symbol.PARENTHESIS_CLOSE_STRING)
    // .replace("#", ""))
    // .distinct().collect(Collectors.toSet()));
    // }
    // return kpicodeList;
    // }

    public static Set<String> getSubKPICodeForTimeShift(String kpiFormulaDesc) {
        Set<String> kpicodeList = new HashSet<>();
        if (kpiFormulaDesc.contains("KPI#")) {
            kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI"))
                    .filter(e -> e.contains("TimeShift") && e.contains(Symbol.HASH_STRING))
                    .map(e -> StringUtils.substringBetween(e, Symbol.HASH_STRING, Symbol.PARENTHESIS_CLOSE_STRING)
                            + PMConstants.CUSTOM_TIME_SHIFT
                            + StringUtils.substringBetween(e, "TimeShift(", Symbol.PARENTHESIS_CLOSE_STRING))
                    .distinct().collect(Collectors.toSet()));
            kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI"))
                    .filter(e -> e.contains(Symbol.PARENTHESIS_CLOSE_STRING) && e.contains(Symbol.HASH_STRING))
                    .map(e -> StringUtils
                            .substringBefore(StringUtils.substringBefore(e, Symbol.PARENTHESIS_CLOSE_STRING), ".")
                            .replace("#", ""))
                    .distinct().collect(Collectors.toSet()));
        }
        return kpicodeList;
    }

    private void getNormalAndTimeShiftKPIs(Set<String> kpiCode, Set<String> normalKPIs, Set<String> timeShiftKPIs) {

        logger.info("Getting Normal And TimeShift KPIs: KPI Code={}, Normal KPIs={}, TimeShift KPIs={}", kpiCode,
                normalKPIs, timeShiftKPIs);
        // KPI Code=[1088, 1086, 1083], Normal KPIs=[], TimeShift KPIs=[]

        Map<String, String> contextMap = jobcontext.getParameters();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        String kpiCodes = Joiner.on(",").join(kpiCode);
        String query = "SELECT KPI_FORMULA_DESC, KPI_CODE FROM KPI_FORMULA WHERE KPI_CODE IN ('"
                + kpiCodes.replace(",", "','")
                + "')";

        logger.info("MySQL Query To Get KPI Formula Desc: {}", query);

        try {
            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            logger.info("JDBC Driver={}, JDBC URL={}, JDBC Username={}, JDBC Password={}", jdbcDriver, jdbcUrl,
                    jdbcUsername, jdbcPassword);

            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                if (!StringUtils.isEmpty(resultSet.getString(1)) && resultSet.getString(1).contains("TimeShift")) {
                    timeShiftKPIs.add(resultSet.getString(2));
                } else {
                    normalKPIs.add(resultSet.getString(2));
                }
            }

        } catch (Exception e) {
            logger.error("Exception While Getting Normal KPI and TimeShift KPI, Message={}, Error={}", e.getMessage(),
                    e);
        } finally {
            close(connection, preparedStatement, resultSet);
        }

        logger.info("Normal KPIs={}, TimeShift KPIs={}", normalKPIs, timeShiftKPIs);
        // Normal KPIs=[1088, 1086, 1083], TimeShift KPIs=[]
    }

    private Set<String> getNormalSubkpiWithKPI(Set<String> kpiCode, Set<String> kpiWithSubKPI,
            Set<String> timeShiftKPI) {

        Map<String, String> contextMap = jobcontext.getParameters();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        String kpiCodes = Joiner.on(",").join(kpiCode);
        Set<String> subkpi = new HashSet<>();

        String query = "SELECT KPI_FORMULA_DESC, KPI_CODE FROM KPI_FORMULA WHERE KPI_CODE IN ('"
                + kpiCodes.replace(",", "','")
                + "')";

        logger.info("MySQL Query To Get Sub KPI Formula Desc With KPI: {}", query);
        try {
            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                if (!StringUtils.isEmpty(resultSet.getString(1)) && !resultSet.getString(1).equalsIgnoreCase("null")
                        && !resultSet.getString(1).contains("TimeShift")) {
                    subkpi = getSubKPICode(resultSet.getString(1));
                    kpiWithSubKPI.addAll(subkpi);
                } else {
                    timeShiftKPI.add(resultSet.getString(2));
                }
            }
            if (!CollectionUtils.isEmpty(subkpi)) {
                kpiWithSubKPI = getNormalSubkpiWithKPI(subkpi, kpiWithSubKPI, timeShiftKPI);
            }
            kpiWithSubKPI.addAll(Arrays.asList(kpiCodes.split(",")));
        } catch (Exception e) {
            logger.error("Exception While Getting Normal KPI with Sub KPI, Message={}, Error={}", e.getMessage(), e);
        } finally {
            close(connection, preparedStatement, resultSet);
        }
        return kpiWithSubKPI;
    }

    private Set<String> getTimeShiftSubkpiWithKPI(Set<String> timeShiftKPI, Set<String> kpiWithSubKPI,
            Map<String, Set<String>> map) {

        Map<String, String> contextMap = jobcontext.getParameters();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        Set<String> subkpi = new HashSet<>();
        String timeShiftKPIs = Joiner.on(",").join(timeShiftKPI);

        String query = "SELECT KPI_FORMULA_DESC, KPI_CODE FROM KPI_FORMULA WHERE KPI_CODE IN ('"
                + timeShiftKPIs.replace(",", "','") + "')";

        logger.info("MySQL Query To Get TimeShift KPI With Sub KPI: {}", query);
        try {
            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                if (!StringUtils.isEmpty(resultSet.getString(1)) && !resultSet.getString(1).equalsIgnoreCase("null")) {
                    subkpi = getSubKPICodeForTimeShift(resultSet.getString(1));
                    kpiWithSubKPI.addAll(subkpi);
                    getPossibleSubKpiListForTimeShift1(map, kpiWithSubKPI, resultSet.getString(2), subkpi);
                }
            }
            if (!subkpi.isEmpty()) {
                getTimeShiftMap(map, subkpi);
            }
            if (!CollectionUtils.isEmpty(subkpi)) {
                kpiWithSubKPI = getTimeShiftSubkpiWithKPI(subkpi, kpiWithSubKPI, map);
            }
            kpiWithSubKPI.addAll(timeShiftKPI);
        } catch (Exception e) {
            logger.error("Exception While Getting TimeShift KPI with Sub KPI, Message={}, Error={}", e.getMessage(), e);
        } finally {
            close(connection, preparedStatement, resultSet);
        }
        return kpiWithSubKPI;
    }

    private void getPossibleSubKpiListForTimeShift1(Map<String, Set<String>> map, Set<String> subKpiListForTimeShift,
            String kpiCode, Set<String> temp) {
        if (map.containsKey(kpiCode)) {
            Set<String> possibleTimeShift = map.get(kpiCode);
            for (String subKpi : temp) {
                for (String possibleValue : possibleTimeShift) {
                    if (!possibleValue.equalsIgnoreCase("0")) {
                        subKpiListForTimeShift.add(subKpi + "))." + possibleValue);
                    }
                }
            }
        }
    }

    private void getTimeShiftMap(Map<String, Set<String>> map, Set<String> subKpiListForTimeShift) {
        for (String subkpiList : subKpiListForTimeShift) {
            String[] subKpi = subkpiList.split("\\)\\).");
            try {
                if (map.containsKey(subKpi[0])) {
                    Set<String> list = map.get(subKpi[0]);
                    list.add(subKpi[1]);
                    map.put(subKpi[0], list);
                } else {
                    Set<String> ls = new HashSet<>();
                    ls.add(subKpi[1]);
                    map.put(subKpi[0], ls);
                }
            } catch (Exception e1) {
                if (map.containsKey(subKpi[0])) {
                    Set<String> ls = map.get(subKpi[0]);
                    ls.add("0");
                    map.put(subKpi[0], ls);
                } else {
                    Set<String> ls = new HashSet<>();
                    ls.add("0");
                    map.put(subKpi[0], ls);
                }
            }
        }
    }

    private static List<String> getQuarterlyDateList(String frequency, String startDate, String endDate,
            String specificDate, String duration, String dayno) throws ParseException {

        logger.info(
                "Getting Quarterly Date List With Input: frequency={}, startDate={}, endDate={}, specificDate={}, duration={}, dayno={}",
                frequency, startDate, endDate, specificDate, duration, dayno);

        // frequency=15 Min, startDate=Jan 21,2025 0:00, endDate=Jan 21,2025 23:45,
        // specificDate=null, duration=1

        Calendar calendar = new GregorianCalendar();
        List<String> dateList = new ArrayList<>();
        DateFormat dateFormat = new SimpleDateFormat("yyMMddHHmm");
        if (frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERDAY)) {
            dateFormat = new SimpleDateFormat("yyMMdd");
        } else if (frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERHOUR)) {
            dateFormat = new SimpleDateFormat("yyMMddHH");
        }
        if (!StringUtils.isEmpty(specificDate)) {
            String[] split = specificDate.split(",");
            for (String str : split) {
                Date date = new Date(Long.parseLong(str));
                calendar.setTime(date);
                String format = dateFormat.format(calendar.getTime());
                dateList.add(format);// + "*"
            }
        } else {
            Date fromDate = null;
            Date toDate = null;
            if (duration.equalsIgnoreCase("dates")) {
                fromDate = new SimpleDateFormat("MMM dd,yyyy HH:mm").parse(startDate);
                toDate = new SimpleDateFormat("MMM dd,yyyy HH:mm").parse(endDate);

                logger.info("Formatted Dates: fromDate={}, toDate={}", fromDate, toDate);
            } else if (duration.equalsIgnoreCase(PMConstants.DURATION_DAILY)) {
                toDate = DateUtils.addMilliseconds(DateUtils.ceiling(DateUtils.addDays(new Date(), -1), Calendar.DATE),
                        -1);
                fromDate = DateUtils.truncate(DateUtils.addDays(new Date(), -Integer.valueOf(dayno)), Calendar.DATE);
            } else if (duration.equalsIgnoreCase(PMConstants.DURATION_HOURLY)) {
                if (dayno != null && !dayno.equalsIgnoreCase("")) {
                    toDate = DateUtils.addHours(new Date(), -1);
                    fromDate = DateUtils.addHours(new Date(), -(Integer.valueOf(dayno)));
                    logger.info("dayno in if part for fromdate  : {} and todate : {} and dayno : {}", fromDate, toDate,
                            dayno);
                } else {
                    toDate = DateUtils.addHours(new Date(), -1);
                    fromDate = DateUtils.addHours(new Date(), -24);
                    logger.info("dayno for toDate  : {} and fromDate : {} and dayno : {}", toDate, fromDate, dayno);
                }
            } else if (duration.equalsIgnoreCase(PMConstants.DURATION_WEEKLY)
                    || duration.equalsIgnoreCase(PMConstants.FREQUENCY_PERWEEK)) {
                Date[] previousWeekStartEndDate = getPreviousWeekStartEndDate(dayno);
                fromDate = previousWeekStartEndDate[0];
                toDate = previousWeekStartEndDate[1];
            } else if (duration.equalsIgnoreCase(PMConstants.DURATION_MONTHLY)
                    || duration.equalsIgnoreCase(PMConstants.FREQUENCY_PERMONTH)) {
                Date[] preWeekStartEndDate = getPreviousMonthStartEndDate(dayno);
                fromDate = preWeekStartEndDate[0];
                toDate = preWeekStartEndDate[1];
            }
            int increment = 0;
            if (frequency.equalsIgnoreCase(PMConstants.FREQUENCY_QUARTERLY)
                    || frequency.equalsIgnoreCase("QUARTERLY")) {
                dateFormat = new SimpleDateFormat("yyMMddHHmm");
                // toDate = DateUtils.addMinutes(toDate, 45);
                increment = 15;
            } else if (frequency.equalsIgnoreCase(PMConstants.DURATION_HOURLY)
                    || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERHOUR)) {
                dateFormat = new SimpleDateFormat("yyMMddHH");
                increment = 15 * 4;
            } else if (frequency.equalsIgnoreCase(PMConstants.DURATION_DAILY)
                    || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERDAY)
                    || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_BBH)
                    || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_NBH)
                    || frequency.equalsIgnoreCase(PMConstants.DURATION_WEEKLY)
                    || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERWEEK)
                    || frequency.equalsIgnoreCase(PMConstants.DURATION_MONTHLY)
                    || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERMONTH)) {
                dateFormat = new SimpleDateFormat("yyMMdd");
                increment = 15 * 4 * 24;
            }

            logger.info("Formatted fromDate: {}, toDate: {}, increment: {}", fromDate, toDate, increment);
            // Formatted fromDate: 2025-01-21T00:00:00.000+0000, toDate:
            // 2025-01-22T00:30:00.000+0000, increment: 15
            while (fromDate.before(toDate) || fromDate.equals(toDate)) {
                String format = dateFormat.format(fromDate);
                dateList.add(format);// + "*"
                Date addMinutes = DateUtils.addMinutes(fromDate, increment);
                fromDate = addMinutes;
                // logger.info("fromDate : {} ", fromDate);
            }
            // logger.info("dateList : {} ", dateList);
        }
        return dateList.stream().sorted().collect(Collectors.toList());
        // List<String> reducedDateList = getReducedDateList(frequency, dateList);
        // return reducedDateList.stream().sorted().collect(Collectors.toList());
    }

    public static Date[] getPreviousWeekStartEndDate(String dayno) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        Date maxTime = DateUtils.truncate(lastDayOfLastWeek(calendar), Calendar.DATE);
        maxTime = setTimeOfDay(maxTime, 23, 45, 0).toDate();
        Date minTime = DateUtils.truncate(firstDayOfLastWeek(calendar, dayno), Calendar.DATE);
        return new Date[] { minTime, maxTime };
    }

    public static Date[] getPreviousMonthStartEndDate(String dayno) {
        Calendar calStart = Calendar.getInstance();
        Calendar calStart1 = Calendar.getInstance();
        calStart.add(Calendar.MONTH, -Integer.valueOf(dayno));
        calStart.set(Calendar.DATE, 1);
        calStart.set(Calendar.HOUR_OF_DAY, 0);
        calStart.set(Calendar.MINUTE, 0);
        calStart.set(Calendar.SECOND, 0);
        calStart.set(Calendar.MILLISECOND, 0);
        Date firstDateOfPreviousMonth = calStart.getTime();
        calStart1.set(Calendar.DATE, 1);
        calStart1.add(Calendar.DAY_OF_MONTH, -1);
        calStart1.set(Calendar.HOUR_OF_DAY, 0);
        calStart1.set(Calendar.MINUTE, 0);
        calStart1.set(Calendar.SECOND, 0);
        calStart1.set(Calendar.MILLISECOND, 0);
        Date lastDateOfPreviousMonth = calStart1.getTime();
        return new Date[] { firstDateOfPreviousMonth, lastDateOfPreviousMonth };
    }

    public static Date lastDayOfLastWeek(Calendar c) {
        c = (Calendar) c.clone();
        c.add(Calendar.WEEK_OF_YEAR, -1);
        c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        return c.getTime();
    }

    public static LocalDateTime setTimeOfDay(Date date, Integer hour, Integer minute, Integer second) {
        LocalDateTime local = new LocalDateTime(date);
        if (hour != null && minute != null && second != null) {
            local = local.withHourOfDay(hour).withMinuteOfHour(minute).withSecondOfMinute(second);
        } else if (hour != null && minute != null) {
            local = local.withHourOfDay(hour).withMinuteOfHour(minute);
        } else if (hour != null && second != null) {
            local = local.withHourOfDay(hour).withSecondOfMinute(second);
        } else if (hour != null) {
            local = local.withHourOfDay(hour);
        } else if (minute != null && second != null) {
            local = local.withMinuteOfHour(minute).withSecondOfMinute(second);
        } else if (minute != null) {
            local = local.withMinuteOfHour(minute);
        } else if (second != null) {
            local = local.withSecondOfMinute(second);
        }
        return local;
    }

    public static Date firstDayOfLastWeek(Calendar c, String dayno) {
        c = (Calendar) c.clone();
        c.add(Calendar.WEEK_OF_YEAR, -Integer.valueOf(dayno));
        c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return c.getTime();
    }

    private static List<String> getReducedDateList(String frequency, List<String> dateList) {
        System.out.println(
                "Starting getReducedDateList with frequency=" + frequency + ", dateList size=" + dateList.size());
        List<String> list = new ArrayList<>();
        if (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY")) {
            System.out.println("Processing 15 Min or QUARTERLY frequency");

            System.out.println("Input DateList = " + dateList);
            // Group by day (first 6 characters)
            // .collect(Collectors.groupingBy(e -> e.substring(0, e.length() - 4)));
            Map<String, List<String>> dayWiseQuarters = dateList.stream()
                    .collect(Collectors.groupingBy(e -> e.substring(0, 6)));
            System.out.println("Created dayWiseQuarters = " + dayWiseQuarters);

            List<String> restOfDaysQuarter = reducePath(dayWiseQuarters, list, 96);
            System.out.println("After first reducePath, restOfDaysQuarter size=" + restOfDaysQuarter.size());

            // Group by hour (first 8 characters)
            // .collect(Collectors.groupingBy(e -> e.substring(0, e.length() - 3)));
            Map<String, List<String>> hourWiseQuarters = restOfDaysQuarter.stream()
                    .collect(Collectors.groupingBy(e -> e.substring(0, 8)));
            System.out.println("Created hourWiseQuarters map with " + hourWiseQuarters.size() + " entries");

            List<String> restOfHoursQuarter = reducePath(hourWiseQuarters, list, 4);
            System.out.println("After second reducePath, restOfHoursQuarter size=" + restOfHoursQuarter.size());

            list.addAll(restOfHoursQuarter);
            System.out.println("Final list size after adding restOfHoursQuarter=" + list.size());
        }

        System.out.println("Returning final list with size=" + list.size());
        return list;
    }

    // private static List<String> getReducedDateList(String frequency, List<String>
    // dateList) {
    // List<String> list = new ArrayList<>();
    // if (frequency.equalsIgnoreCase(PMConstants.FREQUENCY_QUARTERLY) ||
    // frequency.equalsIgnoreCase("QUARTERLY")) {
    // Map<String, List<String>> dayWiseQuarters = dateList.stream()
    // .collect(Collectors.groupingBy(e -> e.substring(0, e.length() - 5)));
    // List<String> restOfDaysQuarter = reducePath(dayWiseQuarters, list, 96);
    // Map<String, List<String>> hourWiseQuarters = restOfDaysQuarter.stream()
    // .collect(Collectors.groupingBy(e -> e.substring(0, e.length() - 3)));
    // List<String> restOfHoursQuarter = reducePath(hourWiseQuarters, list, 4);
    // list.addAll(restOfHoursQuarter);
    // } else if (frequency.equalsIgnoreCase(PMConstants.DURATION_HOURLY)
    // || frequency.equalsIgnoreCase(PMConstants.FREQUENCY_PERHOUR)) {
    // Map<String, List<String>> hourWiseQuarters = dateList.stream()
    // .collect(Collectors.groupingBy(e -> e.substring(0, e.length() - 3)));
    // List<String> restOfHoursQuarter = reducePath(hourWiseQuarters, list, 24);
    // list.addAll(restOfHoursQuarter);
    // } else {
    // list = dateList;
    // }
    // return list;
    // }

    private void getPMCounterVariableAggrQuery(JobContext jobContext, String frequency,
            Map<String, List<String>> timeShiftWiseDateMap) throws Exception {

        logger.info("Get PM Counter Variable Aggr Query, With Frequency={} and Time Shift Wise Date Map={}", frequency,
                timeShiftWiseDateMap);

        StringBuilder nodeAggrQuery = new StringBuilder();
        String filterQueryFinal = Symbol.EMPTY_STRING;
        StringBuilder counterWithNodeAggr = new StringBuilder();
        StringBuilder counterWithTimeAggr = new StringBuilder();
        String finalTimeAggrQuery = Symbol.EMPTY_STRING;
        StringBuilder filterQuery = new StringBuilder();
        String finalNodeAggrQuery = Symbol.EMPTY_STRING;
        String finaldate = Symbol.EMPTY_STRING;
        Set<String> finalDateSet = new HashSet<>();
        String timeKey = Symbol.EMPTY_STRING;
        StringBuilder mapQuery = new StringBuilder();
        String counterQueryMap = null;
        if (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY")) {
            timeKey = "quarterKey ";
        }
        if (frequency.equalsIgnoreCase("DAILY") || frequency.equalsIgnoreCase("PERDAY")
                || frequency.equalsIgnoreCase("WEEKLY") || frequency.equalsIgnoreCase("PERWEEK")
                || frequency.equalsIgnoreCase("MONTHLY") || frequency.equalsIgnoreCase("PERMONTH")) {
            timeKey = "dateKey ";
        }
        if (frequency.equalsIgnoreCase("HOURLY") || frequency.equalsIgnoreCase("PERHOUR")) {
            timeKey = "hourKey ";
        }
        String catgoryInfoMapString = jobContext.getParameter("catgoryInfoMap");
        Map<String, List<Map<String, String>>> catgoryInfoMap = new Gson().fromJson(catgoryInfoMapString, Map.class);
        String categoryList = jobContext.getParameter("categoryString");

        logger.info("Category List={}", categoryList);
        logger.info("Category Info Map={}", catgoryInfoMap);

        counterWithNodeAggr.append(
                " SELECT quarterKey, dateKey, hourKey, finalKey, categoryname, NAM, moHierachy, first_value(metaData) AS metaData, ");

        logger.info("Time Shift Wise Date Map={}", timeShiftWiseDateMap); // {0=[25012*]}

        for (Map.Entry<String, List<String>> timeShiftWiseDates : timeShiftWiseDateMap.entrySet()) {
            StringBuilder dateString = new StringBuilder();
            String date = Symbol.EMPTY_STRING;
            for (String dates : timeShiftWiseDates.getValue()) {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyMMdd");
                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");

                if (!dates.contains("*")) {

                    logger.info("If Dates={}", dates);
                    logger.info("If Frequency={}", frequency);

                    if (!StringUtils.isEmpty(frequency) && (frequency.equalsIgnoreCase("DAILY")
                            || frequency.equalsIgnoreCase("WEEKLY") || frequency.equalsIgnoreCase("PERWEEK")
                            || frequency.equalsIgnoreCase("PERDAY") || frequency.equalsIgnoreCase("MONTHLY")
                            || frequency.equalsIgnoreCase("PERMONTH"))) {
                        dateString = dateString
                                .append(outputFormat.format(inputFormat.parse(dates.substring(0, 6))) + ",");
                    } else if (!StringUtils.isEmpty(frequency)
                            && (frequency.equalsIgnoreCase("PERHOUR") || frequency.equalsIgnoreCase("HOURLY"))) {
                        inputFormat = new SimpleDateFormat("yyMMddHH");
                        outputFormat = new SimpleDateFormat("yyyyMMddHH");
                        dateString = dateString
                                .append(outputFormat.format(inputFormat.parse(dates.substring(0, 8))) + ",");
                    } else if (!StringUtils.isEmpty(frequency)
                            && (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY"))) {
                        inputFormat = new SimpleDateFormat("yyMMddHHmm");
                        outputFormat = new SimpleDateFormat("yyyyMMddHHmm"); // 202501190700
                        dateString = dateString
                                .append(outputFormat.format(inputFormat.parse(dates.substring(0, 10))) + ",");
                    }
                } else {

                    // logger.info("Else Dates={}", dates);
                    // logger.info("Else Frequency={}", frequency);

                    logger.info("DEBUG inputFormat={}, dates={}, frequency={}", inputFormat, dates, frequency);

                    String finaldateString = outputFormat.format(inputFormat.parse(dates.substring(0, 6)));

                    logger.info("Final Date String={}", finaldateString); // 20250102

                    List<String> finaldateList = Arrays.asList("23", "22", "21", "20", "19", "18", "17", "16", "15",
                            "14", "13", "12", "11", "10", "09", "08", "07", "06", "05", "04", "03", "02", "01", "00");

                    if ((!frequency.equalsIgnoreCase("PERHOUR") && !frequency.equalsIgnoreCase("HOURLY"))
                            && (!frequency.equalsIgnoreCase("15 Min") && !frequency.equalsIgnoreCase("QUARTERLY"))) {
                        dateString = dateString.append(finaldateString + ",");
                    } else {
                        for (String datesList : finaldateList) {
                            if (!StringUtils.isEmpty(frequency) && (frequency.equalsIgnoreCase("PERHOUR")
                                    || frequency.equalsIgnoreCase("HOURLY"))) {
                                dateString = dateString.append(finaldateString + datesList + ",");
                            } else if (!StringUtils.isEmpty(frequency) && (frequency.equalsIgnoreCase("15 Min")
                                    || frequency.equalsIgnoreCase("QUARTERLY"))) {
                                Integer minute = 00;
                                while (minute < 60) {
                                    dateString = dateString
                                            .append(finaldateString + datesList + String.valueOf(minute) + ",");
                                    minute += Integer.valueOf(15);
                                }
                            }
                        }
                    }
                }
                finalDateSet.add(dateString.toString());
            }

            logger.info("Final Date Set={}", finalDateSet);

            date = StringUtils.substringBeforeLast(dateString.toString(), ",");
            logger.info("Date={}", date);

            logger.info("Time Shift Wise Dates={}", timeShiftWiseDates);
            String timeShift = timeShiftWiseDates.getKey();
            logger.info("Time Shift Before={}", timeShift);

            timeShift = timeShift.equals("0") ? "" : "ts" + timeShift;
            logger.info("Time Shift After={}", timeShift);

            for (String category : categoryList.split(",")) {
                logger.info("Getting Category={}", category);

                String categoryName = category.split("@")[0];
                List<Map<String, String>> catInfoList = catgoryInfoMap.get(category);
                if (catInfoList == null) {
                    logger.info("catInfoList is null For Category={}", category);
                    continue;
                }

                for (Map<String, String> counterInfoMap : catInfoList) {
                    String counterKey = "C" + counterInfoMap.get("sequenceNo") + "#"
                            + counterInfoMap.get("pmcountervariableid_pk") + timeShift;
                    String counterId = counterKey.split("#")[1];
                    String nodeAggrVal = counterInfoMap.get("nodeAggr");
                    String timeAggrVal = counterInfoMap.get("timeAggr");

                    logger.info("Counter Key={}", counterKey);
                    logger.info("Counter Id={}", counterId);
                    logger.info("Node Aggr Val={}", nodeAggrVal);
                    logger.info("Time Aggr Val={}", timeAggrVal);

                    if (!StringUtils.isEmpty(nodeAggrVal) && !StringUtils.isEmpty(timeAggrVal)) {
                        mapQuery = mapQuery.append("'" + counterKey + "', `" + counterKey + "`,'" + counterId + "', `"
                                + counterKey + "`, ");
                    }

                    if (!StringUtils.isEmpty(nodeAggrVal)) {
                        if (nodeAggrVal.equalsIgnoreCase("AVG")) {

                            counterWithNodeAggr = counterWithNodeAggr.append(nodeAggrVal + "(CASE WHEN categoryname = '"
                                    + categoryName + "' AND " + timeKey + " IN ('" + date.replace(",", "','")
                                    + "') THEN CAST(`" + counterKey + "` AS DOUBLE)  ELSE NULL END) AS `" + counterKey
                                    + "`," + "sum" + "(CASE WHEN categoryname = '" + categoryName + "'  AND " + timeKey
                                    + " IN ('" + date.replace(",", "','") + "') THEN CAST(`" + counterKey
                                    + "` AS DOUBLE) ELSE NULL END) AS `S" + counterKey
                                    + "`, COUNT(CASE WHEN categoryname = '" + categoryName + "' AND " + timeKey
                                    + " IN ('" + date.replace(",", "','") + "') THEN CAST(`" + counterKey
                                    + "` AS DOUBLE) ELSE NULL END) AS `C" + counterKey + "` ,");

                            nodeAggrQuery = nodeAggrQuery.append("sum(`S" + counterKey + "`)/sum(`C" + counterKey
                                    + "`) AS `" + counterKey + "`, sum(`S" + counterKey + "`) AS `S" + counterKey
                                    + "`, sum(`C" + counterKey + "`) AS `C" + counterKey + "`, ");

                            filterQuery = filterQuery.append("(`S" + counterKey + "`) AS `S" + counterKey + "`,(`C"
                                    + counterKey + "`) AS `C" + counterKey + "`,");

                        } else if (nodeAggrVal.equalsIgnoreCase("COUNT")) {

                            counterWithNodeAggr.append(nodeAggrVal + "(CASE WHEN categoryname = '" + categoryName
                                    + "' AND " + timeKey + " in ('" + date.replace(",", "','") + "') THEN cast(`"
                                    + counterKey + "` as DOUBLE) ELSE NULL END) as `" + counterKey + "`,");

                            nodeAggrQuery = nodeAggrQuery
                                    .append("sum" + "(`" + counterKey + "`) as `" + counterKey + "`,");

                            filterQuery = filterQuery.append("(`" + counterKey + "`) as `" + counterKey + "`,");

                        } else {

                            counterWithNodeAggr.append(nodeAggrVal + "(CASE WHEN categoryname = '" + categoryName
                                    + "' AND " + timeKey + " in ('" + date.replace(",", "','") + "') THEN cast(`"
                                    + counterKey + "`" + " as DOUBLE) ELSE NULL END) as `" + counterKey + "`,");

                            nodeAggrQuery = nodeAggrQuery
                                    .append(nodeAggrVal + "(`" + counterKey + "`) as `" + counterKey + "`,");

                            filterQuery = filterQuery.append("(`" + counterKey + "`) as `" + counterKey + "`,");
                        }
                    }
                    if (!StringUtils.isEmpty(timeAggrVal)) {
                        counterWithTimeAggr.append(timeAggrVal + "(`" + counterKey + "`) AS `" + counterKey + "`,");
                    }
                }
            }
        }

        logger.info("Counter With Node Aggr={}", counterWithNodeAggr);
        logger.info("Counter With Time Aggr={}", counterWithTimeAggr);
        logger.info("Node Aggr Query={}", nodeAggrQuery);
        logger.info("Filter Query={}", filterQuery);
        logger.info("Map Query={}", mapQuery);

        finaldate = String.join(",", finalDateSet);

        jobContext.setParameters("finalDateStringKey", finaldate);
        jobContext.setParameters("finalFrequencyKey", timeKey);

        logger.info("Final Date={}", finaldate);
        logger.info("Time Key={}", timeKey);

        counterQueryMap = StringUtils.substringBeforeLast(mapQuery.toString(), ",");
        counterQueryMap = "Map(" + counterQueryMap + ") AS rawcounters";

        String counterNodeAggr = StringUtils.substringBeforeLast(counterWithNodeAggr.toString(), ",");
        String counterTimeAggr = StringUtils.substringBeforeLast(counterWithTimeAggr.toString(), ",");

        logger.info("Counter Node Aggr={}", counterNodeAggr);
        logger.info("Counter Time Aggr={}", counterTimeAggr);

        String finalNodeAggr = "SELECT finalKey, first_value(metaData) AS metaData, "
                + StringUtils.substringBeforeLast(nodeAggrQuery.toString(), ",")
                + " FROM FinalCounterData GROUP BY finalKey ";

        filterQueryFinal = StringUtils.substringBeforeLast(filterQuery.toString(), ",");

        finalNodeAggrQuery = counterNodeAggr
                + " FROM CounterData GROUP BY quarterKey, finalKey, dateKey, hourKey, NAM, categoryname, moHierachy";

        finalTimeAggrQuery = "SELECT finalKey, first_value(metaData) AS metaData, " + counterTimeAggr
                + " FROM finalNodeAggrData GROUP BY finalKey ORDER BY finalKey";

        logger.info("Final Node Aggr Query={}", finalNodeAggrQuery);
        logger.info("Final Time Aggr Query={}", finalTimeAggrQuery);
        logger.info("Counter Query Map={}", counterQueryMap);
        logger.info("Filter Query Final={}", filterQueryFinal);
        logger.info("Final Node Aggr={}", finalNodeAggr);

        jobContext.setParameters("COUNTER_MAP_QUERY", counterQueryMap);
        jobContext.setParameters("filterQueryFinal", filterQueryFinal);
        jobContext.setParameters("COUNTER_NODE_AGGR_QUERY", finalNodeAggr);
        jobContext.setParameters("COUNTER_TIME_AGGR_QUERY", finalTimeAggrQuery);
        jobContext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", finalNodeAggrQuery);
    }

    private static List<String> reducePath(Map<String, List<String>> collect, List<String> list, int count) {
        System.out.println("Starting reducePath with count=" + count);
        List<String> tempList = new ArrayList<>();
        try {
            for (Map.Entry<String, List<String>> s : collect.entrySet()) {
                String key = s.getKey();
                List<String> value = s.getValue();
                System.out.println("Processing key=" + key + ", value size=" + value.size());

                if (value.size() == count) {
                    // System.out.println("Adding key=" + key + " with * to list");
                    // list.add(key + "*");
                    System.out.println("Adding all values for complete group with key=" + key);
                    list.addAll(value);
                } else {
                    System.out.println("Adding " + value.size() + " values to tempList");
                    tempList.addAll(value);
                }
            }
            System.out.println("Completed reducePath, returning tempList with " + tempList.size() + " elements");
        } catch (Exception e) {
            System.out.println("Error in reducePath: " + e.getMessage());
            e.printStackTrace();
        }
        return tempList;
    }

    // private static List<String> reducePath(Map<String, List<String>> collect,
    // List<String> list, int count) {
    // List<String> tempList = new ArrayList<>();
    // for (Map.Entry<String, List<String>> s : collect.entrySet()) {
    // String key = s.getKey();
    // List<String> value = s.getValue();
    // if (value.size() == count) {
    // list.add(key + "*");
    // } else {
    // tempList.addAll(value);
    // }
    // }
    // return tempList;
    // }

    private void setCategoryListAndCounterMap(String domain, String vendor, String kpiIds, String counterIds,
            Set<String> counterMap, Set<String> categoryList, Map<String, Map<String, String>> counterWithvalues) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Map<String, String> contextMap = new HashMap<>();
        contextMap = jobcontext.getParameters();
        String query = "SELECT " +
                "UPPER(pc.CATEGORY_NAME) AS categoryName, " +
                "cv.PM_COUNTER_VARIABLE_ID_PK AS pmcountervariableid_pk, " +
                "UPPER(REPLACE(cv.COUNTER, ' ', '')) AS counter, " +
                "cv.ATTRIBUTE AS attribute, " +
                "cv.SUBCATEGORY1_VALUE AS subcategory1value, " +
                "cv.SUBCATEGORY2_VALUE AS subcategory2value, " +
                "cv.SUBCATEGORY3_VALUE AS subcategory3value, " +
                "cv.SUBCATEGORY4_VALUE AS subcategory4value " +
                "FROM KPI_FORMULA kpi " +
                "INNER JOIN FORMULA_COUNTER_MAPPING map ON kpi.KPI_FORMULA_ID_PK = map.KPI_FORMULA_ID_FK " +
                "INNER JOIN PM_COUNTER_VARIABLE cv ON cv.PM_COUNTER_VARIABLE_ID_PK = map.PM_COUNTER_VARIABLE_ID_FK " +
                "INNER JOIN PM_CATEGORY pc ON pc.PM_CATEGORY_ID_PK = cv.PM_CATEGORY_ID_FK " +
                "WHERE kpi.DOMAIN = '" + domain + "' " +
                "AND kpi.KPI_CODE IN (" + kpiIds + ")";

        if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
            query += " AND kpi.VENDOR = '" + vendor + "'";
        }
        if (!com.enttribe.commons.lang.StringUtils.isEmpty(counterIds)) {
            query += " AND map.PM_COUNTER_VARIABLE_ID_FK IN (" + counterIds + ")";
        }

        logger.info("MySQL Query To Get Category List And Counter Map={}", query);

        try {
            final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
            final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, String> metaDataValue = new HashMap<>();
                categoryList.add(resultSet.getString(1));
                counterMap.add(resultSet.getString(2));
                metaDataValue.put(resultSetMetaData.getColumnName(1), resultSet.getString(1));// category
                metaDataValue.put(resultSetMetaData.getColumnName(4), resultSet.getString(4));// attribute
                metaDataValue.put(resultSetMetaData.getColumnName(5), resultSet.getString(5));// subcat1
                metaDataValue.put(resultSetMetaData.getColumnName(6), resultSet.getString(6));// 2
                metaDataValue.put(resultSetMetaData.getColumnName(7), resultSet.getString(7));// 3
                metaDataValue.put(resultSetMetaData.getColumnName(8), resultSet.getString(8));// 4
                counterWithvalues.put(resultSet.getString(3), metaDataValue);

            }
        } catch (Exception e) {
            logger.error("Exception While Getting Category Wise Counters, Message={}, Error={}", e.getMessage(), e);
        } finally {
            close(connection, preparedStatement, resultSet);
        }
    }

    private String getFilter(String reportLevel, String technology, String vendor, String nodetype, String nodeVal) {

        logger.info("Filter: Level={}, Vendor={}, Technology={}, Node Type={}, Node Value={}", reportLevel, vendor,
                technology, nodetype, nodeVal);

        if (!reportLevel.equalsIgnoreCase("CUSTOM")) {
            if (reportLevel.contains("Geography") || reportLevel.contains("GC")) {
                if (reportLevel.toUpperCase().contains("L4")) {
                    return "L4";
                }
                if (reportLevel.toUpperCase().contains("L3")) {
                    return "L3";
                }
                if (reportLevel.toUpperCase().contains("L2")) {
                    return "L2";
                }
                if (reportLevel.toUpperCase().contains("L1")) {
                    return "L1";
                }
                if (reportLevel.toUpperCase().contains("L0")) {
                    return "L0";
                }
                if (reportLevel.toUpperCase().contains("GC")) {
                    return "GC";
                }

            } else if ((technology.equalsIgnoreCase("2G") || technology.equalsIgnoreCase("3G"))
                    && !technology.equalsIgnoreCase("ALL")) {
                if (reportLevel.equalsIgnoreCase("CSR")) {
                    return "H2_NEID";
                }
                if (reportLevel.equalsIgnoreCase("BSC") || reportLevel.equalsIgnoreCase("RNC")) {
                    return "H1_NEID";
                }
                if (reportLevel.equalsIgnoreCase("JVID") || reportLevel.equalsIgnoreCase("RBS")
                        || reportLevel.equalsIgnoreCase("BTS") || reportLevel.equalsIgnoreCase("WBTS")) {
                    return "ENB_NEID";
                }
                if (reportLevel.equalsIgnoreCase("CELL") || reportLevel.equalsIgnoreCase("WCELL")) {
                    return "NEID";
                }

            } else if ((technology.equalsIgnoreCase("LTE") || technology.equalsIgnoreCase("5G"))
                    && !technology.equalsIgnoreCase("ALL")) {
                if (reportLevel.equalsIgnoreCase("CSR")) {
                    return "H1_NEID";
                }
                if (reportLevel.equalsIgnoreCase("GNB_RU") || reportLevel.equalsIgnoreCase("JVID")
                        || reportLevel.equalsIgnoreCase("EnodeB")) {
                    return "ENB_NEID";
                }
                if (reportLevel.equalsIgnoreCase("CELL") || reportLevel.equalsIgnoreCase("GNB_Cell")
                        || reportLevel.equalsIgnoreCase("GNB_CU_CP") || reportLevel.equalsIgnoreCase("GNB_CU_UP")
                        || reportLevel.equalsIgnoreCase("GNB_DU")) {
                    return "NEID";
                } else {
                    return "NEID";
                }

            } else if (technology.equalsIgnoreCase("ALL")) {
                if (reportLevel.equalsIgnoreCase("CELL") || reportLevel.equalsIgnoreCase("GNB_Cell")
                        || reportLevel.equalsIgnoreCase("GNB_CU_CP") || reportLevel.equalsIgnoreCase("GNB_CU_UP")
                        || reportLevel.equalsIgnoreCase("GNB_DU") || reportLevel.equalsIgnoreCase("WCELL")) {
                    return "NEID";
                }
            } else if (reportLevel.toUpperCase().contains("ROUTER") || reportLevel.toUpperCase().contains("SWITCH")
                    || reportLevel.toUpperCase().contains("DWDM")) {
                return "NODE";
            }
        } else if (reportLevel.equalsIgnoreCase("CUSTOM")) {
            if (technology.equalsIgnoreCase("LTE") || technology.equalsIgnoreCase("5G")) {
                if (nodetype.equalsIgnoreCase("CSR")) {
                    return "H1_NEID";
                }
                if (nodetype.equalsIgnoreCase("GNB_RU") || nodetype.equalsIgnoreCase("JVID")
                        || nodetype.equalsIgnoreCase("EnodeB")) {
                    return "ENB_NEID";
                }
                if (nodetype.equalsIgnoreCase("CELL") || nodetype.equalsIgnoreCase("GNB_Cell")
                        || nodetype.equalsIgnoreCase("GNB_CU_CP") || nodetype.equalsIgnoreCase("GNB_CU_UP")
                        || nodetype.equalsIgnoreCase("GNB_DU")) {
                    return "NEID";
                }
            } else if (technology.equalsIgnoreCase("2G") || technology.equalsIgnoreCase("3G")) {
                if (nodetype.equalsIgnoreCase("CSR")) {
                    return "H2_NEID";
                }
                if (nodetype.equalsIgnoreCase("BSC") || nodetype.equalsIgnoreCase("RNC")) {
                    return "H1_NEID";
                }
                if (nodetype.equalsIgnoreCase("JVID") || nodetype.equalsIgnoreCase("RBS")
                        || nodetype.equalsIgnoreCase("BTS") || nodetype.equalsIgnoreCase("WBTS")) {
                    return "ENB_NEID";
                }
                if (nodetype.equalsIgnoreCase("CELL") || nodetype.equalsIgnoreCase("WCELL")) {
                    return "NEID";
                }

            }

        }

        return null;
    }

    public static String customNetype = null;

    private String getNetypeForCustomCells(String domain, String vendor, String technology) {
        try {
            String query = "SELECT GROUP_CONCAT(DISTINCT ne.NE_TYPE) FROM NETWORK_ELEMENT ne ";
            String condition = "WHERE ne.deleted = 0 ";
            // String condition = "WHERE ne.deleted = 0 AND ne.PARENT_NE_ID_FK IS NULL";
            if (domain != null) {
                condition = condition + " AND ne.domain ='" + domain + "'";
            }
            if (vendor != null) {
                condition = condition + " AND ne.vendor ='" + vendor + "'";
            }
            if (technology != null) {
                condition = condition + " AND ne.technology ='" + technology + "'";
            }
            query = query + condition;
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;
            Map<String, String> contextMap = jobcontext.getParameters();
            try {
                final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
                final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
                final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
                final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

                logger.info("jdbcDriver={}, jdbcUrl={}, jdbcUsername={}, jdbcPassword={}", jdbcDriver, jdbcUrl,
                        jdbcUsername, jdbcPassword);
                logger.info("Query: {}", query);
                Class.forName(jdbcDriver);
                connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
                preparedStatement = connection.prepareStatement(query);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    customNetype = resultSet.getString(1);
                }
            } catch (Exception e) {
                logger.error("Exception In Getting Netype From NETWORK_ELEMENT, Message: {}, Error: {}", e.getMessage(),
                        e);
            } finally {
                close(connection, preparedStatement, resultSet);
            }
            return customNetype;
        } catch (Exception e) {
            logger.error("Exception In Getting Netype For Custom Cells, Message: {}, Error: {}", e.getMessage(), e);
        }
        return customNetype;
    }

    private void close(Connection conn, PreparedStatement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("Error While  Closing ResultSet {}", e.getMessage());
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("Error While  Closing Statement {}", e.getMessage());
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Error While  connection {}", e.getMessage());
            }
        }
    }

    // Need to remove
    private void setKPIsAndCounterList(String kpiString, Set<String> kpiList, Set<String> counterList,
            Set<String> genericKpiList, Set<String> finalkpiIdList) {
        try {
            org.json.JSONArray kpiJson = new JSONArray(kpiString);
            for (Integer i = 0; i < kpiJson.length(); i++) {
                JSONObject jsonObject = kpiJson.getJSONObject(i);
                String kpiName = jsonObject.getString("kpiName");
                String kpiCode = kpiName.split(Symbol.HYPHEN_STRING)[0];
                if (jsonObject.getString("type").equalsIgnoreCase("KPI")) {
                    if (jsonObject.getString("kpicode").contains("G")) {
                        genericKpiList.add(kpiCode);
                    } else {
                        kpiList.add(kpiCode);
                    }
                    finalkpiIdList.add(kpiCode);
                } else if (jsonObject.getString("type").equalsIgnoreCase("Counter")) {
                    counterList.add(kpiCode);
                }
            }
        } catch (Exception e) {
            logger.error("Exception in Getting Counter and KPI from Json: {}", ExceptionUtils.getStackTrace(e));
        }

    }

    // PM_NODE_BY_NETYPE
    private String getSystemConfigurationValueByName(String configKey) {

        String configValue = null;

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            String sqlQuery = "SELECT CONFIG_VALUE FROM BASE_CONFIGURATION WHERE CONFIG_KEY = '" + configKey + "'";

            Class.forName(SPARK_PLATFORM_JDBC_DRIVER);
            connection = DriverManager.getConnection(SPARK_PLATFORM_JDBC_URL, SPARK_PLATFORM_JDBC_USERNAME,
                    SPARK_PLATFORM_JDBC_PASSWORD);
            preparedStatement = connection.prepareStatement(sqlQuery);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                configValue = resultSet.getString(1);
            }
        } catch (Exception e) {
            configValue = "{}";
        } finally {
            close(connection, preparedStatement, resultSet);
        }
        return configValue;
    }

    // Temporary remove after shubham put aggregaton value in Configuration
    private String getMoNameByMoType(String moType) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String returnValue = null;
        try {
            Map<String, String> contextMap = new HashMap<>();
            contextMap = jobcontext.getParameters();
            String sql = "select MO_NAME from MO_META_DATA where MO_TYPE ='" + moType + "'";
            final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
            final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                returnValue = rs.getString(1);
            }
            logger.error("MoMetaData value : {} for Key :{}", returnValue, moType);
        } catch (Exception e) {
            logger.error("Exception While getting MoMeta Data :{}", e.getMessage());
        } finally {
            close(conn, stmt, rs);
        }
        return returnValue;
    }

    public static final String CORE_DOMAINS = "CORE_DOMAINS";
    public static final String LEVEL_VNF = "VNF";
    public static final String LEVEL_VNFC = "VNFC";
    public static final String LEVEL_BAREMETAL = "BAREMETAL";

    private String getDynamicQueryForReportConfig(String reportConfiguration, String domain, String vendor,
            String reportLevel, String neType, String systemConfig, String panGeographyName, String reportType,
            String reportwidgetid_pk, String technology) {

        logger.info(
                "Getting Dynamic Query For Report Config, Report Domain={}, Vendor={}, Report Level={}, NE Type={}, Report Type={}, Report Widget ID={}, Technology={}",
                domain, vendor, reportLevel, neType, reportType,
                reportwidgetid_pk, technology);

        String dynamicQuery = null;

        try {
            JSONObject configJson = new JSONObject(reportConfiguration);
            String magnetFields = configJson.has("magnetFields")
                    ? getStringValueFromJsonArray(configJson.getJSONArray("magnetFields"))
                    : null;

            logger.info("Magnet Fields={}", magnetFields);

            List<String> otherGeographyIdList = getListFromJsonArray(getJsonArray(configJson, "otherGeography"));
            String otherGeographyIds = (!otherGeographyIdList.isEmpty()
                    && !otherGeographyIdList.contains(PMConstants.BLANK))
                            ? String.join(PMConstants.COMMA + "P", otherGeographyIdList)
                            : null;
            otherGeographyIds = "P" + otherGeographyIds;

            logger.info("Report Type={} | Other Geography IDs={}", reportType, otherGeographyIds);

            if (reportType != null && !otherGeographyIdList.isEmpty()
                    && !otherGeographyIdList.contains(PMConstants.BLANK) && otherGeographyIds != null) {

                jobcontext.setParameters("childNode", null);
                dynamicQuery = getDynamicQueryForCustomPolygonReportConfiguration(reportConfiguration, configJson,
                        domain, vendor, reportwidgetid_pk, panGeographyName, otherGeographyIdList, otherGeographyIds,
                        neType, technology, magnetFields);
            } else {
                logger.info("Skipping Custom Polygon Report Configuration!");
            }
        } catch (Exception e) {
            logger.error("Exception While Getting Query For Report, Message={}, Error={}", e.getMessage(), e);
        }
        return dynamicQuery;
    }

    private String getDynamicQueryForCustomPolygonReportConfiguration(String reportConfiguration, JSONObject configJson,
            String domain, String vendor, String reportWidgetIdPk, String panGeographyName,
            List<String> otherGeographyIdList, String otherGeographyIds, String neType, String technology,
            String magnetField) throws JSONException, JsonParseException, JsonMappingException, IOException {
        String reportCode = getValueFromJson(configJson, "reportCode");
        String node = getValueFromJson(configJson, "node");
        String dynamicQuery = "";
        String aggregationValue = "";
        JSONObject json = new JSONObject(reportConfiguration);
        List<String> cellsList = getListFromJsonArray(getJsonArray(json, PMConstants.CELLS_JSONKEY));
        logger.info("othergeographylist {} ,otherGeographyIds {}", otherGeographyIdList, otherGeographyIds);
        if (node.contains("INDIVIDUAL")) {
            if (!cellsList.isEmpty() && !cellsList.get(0).equalsIgnoreCase("")) {
                dynamicQuery = getQueryForCustomCells(reportConfiguration, domain, vendor, technology, magnetField);
            }

            else {
                if (magnetField != null && !magnetField.isEmpty()) {
                    dynamicQuery = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne JOIN CustomNEDetail cs ON cs.networkelementid_fk=ne.networkelementid_pk JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk";
                } else {
                    dynamicQuery = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk  JOIN PRIMARY_GEO_L3 gl3  ON ne.geographyl3id_fk = gl3.geographyl3id_pk";
                }
                String condition = " WHERE ne.deleted = 0 ";

                if (domain != null) {
                    condition = condition + " AND ne.domain ='" + domain + "'";
                }
                if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND  ne.vendor ='" + vendor + "'";
                }
                if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                    condition = condition + " AND ne.neType in (" + neType + ")";
                }
                if (technology != null && !technology.equalsIgnoreCase(PMConstants.All)) {
                    technology = !technology.equalsIgnoreCase("LTE") ? "T" + technology : technology;
                    condition = condition + " AND ne.technology ='" + technology + "'";
                }
                if (magnetField != null && !magnetField.isEmpty()) {
                    magnetField = Arrays.asList(magnetField.split(",")).toString().trim();
                    Map<String, Object> map = new HashMap<>();
                    ObjectMapper mapper = new ObjectMapper();
                    List<Map<String, Object>> readValue = mapper.readValue(magnetField,
                            new TypeReference<List<Map<String, Object>>>() {
                            });
                    readValue.forEach(a -> {
                        map.put((String) a.get("columnName"), a.get("value"));
                    });
                    logger.info("map : {}", map.toString());
                    for (Entry<String, Object> entry : map.entrySet()) {
                        String column = entry.getKey();
                        String value = StringUtils.replaceIgnoreCase(StringUtils.replaceIgnoreCase(
                                StringUtils.replaceIgnoreCase(entry.getValue().toString().trim(), "[", "'"), "]", "'"),
                                ", ", "','").trim();
                        logger.info("column : {} ,value : {} ", column, value);
                        condition = condition + " AND cs." + column + " in (" + value + ")";
                    }
                }
                if (!otherGeographyIdList.isEmpty() && otherGeographyIds != null) {
                    condition = condition
                            + " AND ne.networkelementid_pk in (select networkelementid_fk from NEGeographyMapping where OtherGeographyid_fk in (select othergeographyid_pk from OtherGeography og where og.code in('"
                            + otherGeographyIds.replaceAll(PMConstants.COMMA, "','") + "')))";
                }
                dynamicQuery = dynamicQuery + condition;

            }
        } else {
            jobcontext.setParameters("metaColumn",
                    "DATE,TIME,D,V,Level,NAME#Date,Time,Domain,Vendor,Aggregation Type,Polygon Name,");

            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                // dynamicQuery = "select concat((select hb.alphanumericcode from HbaseRowPrefix
                // hb where hb.domain='"
                // + domain + "' and hb.vendor='" + vendor
                // + "' and hb.l1 is null and hb.l2 is null and hb.l3 is
                // null),'#',concat(ot.code),'#','"
                // + panGeographyName + "') from OtherGeography ot";
                // if (!otherGeographyIdList.isEmpty() && otherGeographyIds != null) {
                // dynamicQuery = dynamicQuery + " where ot.code in ( '"
                // + otherGeographyIds.replaceAll(PMConstants.COMMA, "','") + "')";
                // }
            } else {
                dynamicQuery = "select  concat((select hb.alphanumericcode from HbaseRowPrefix hb where hb.domain='"
                        + domain + "' and hb.vendor is null"
                        + " and hb.l1 is null and hb.l2 is null and hb.l3 is null),'#',concat(ot.code),'#','"
                        + panGeographyName + "') from OtherGeography ot";
                if (!otherGeographyIdList.isEmpty() && otherGeographyIds != null) {
                    dynamicQuery = dynamicQuery + " where ot.code in ( '"
                            + otherGeographyIds.replaceAll(PMConstants.COMMA, "','") + "')";
                }
            }

        }
        logger.info("final dynamic query for poly {}", dynamicQuery);
        return dynamicQuery;
    }

    private String getDynamicQueryForCustomReportConfiguration(JSONObject configJson, String domain, String vendor,
            String reportWidgetIdPk, String panGeographyName) throws JSONException {
        String reportCode = getValueFromJson(configJson, "reportCode");
        String aggregationValue = "";
        String query = "";
        aggregationValue = getMoNameByMoType(reportCode); // getValueFromJson(configJson, "customAggregationValue");
        if (vendor.equalsIgnoreCase("ALL")) {
            query = "select concat(hb.alphanumericCode,'#' ,'" + reportCode + "','#','" + aggregationValue + "','#','"
                    + panGeographyName + "') from ReportWidget rw left Join HbaseRowPrefix hb  "
                    + "on (rw.domain = hb.domain) where hb.l1 is null "
                    + "and hb.l2 is null and hb.l3 is null and hb.othergeographyId_fk is null and reportwidgetid_pk = '"
                    + reportWidgetIdPk + "'";
        } else {
            query = "select concat(hb.alphanumericCode,'#' ,'" + reportCode + "','#','" + aggregationValue + "','#','"
                    + panGeographyName + "') from ReportWidget rw left Join HbaseRowPrefix hb  "
                    + "on (rw.domain = hb.domain and rw.vendor = hb.vendor) where hb.l1 is null "
                    + "and hb.l2 is null and hb.l3 is null and hb.othergeographyId_fk is null and reportwidgetid_pk = '"
                    + reportWidgetIdPk + "'";

        }
        return query;
    }

    private String getFinalQueryForOtherGeoMapping(String reportConfiguration, String domain, String vendor,
            String reportLevel, String neType, String panGeoGraphyName, String corePodLevel, String systemConfig) {
        String dynamicQuery;
        neType = "" + neType.replaceAll(",", "','") + "";
        List<String> dateCenters = getValueListFromJsonByKey(systemConfig, PMConstants.DATA_CENTER_LIST);
        logger.info("Date center list for Core Domain :{} ", dateCenters);
        if (reportLevel.equalsIgnoreCase(PMConstants.GEOGRAPHYL0_LEVEL)) {
            dynamicQuery = getQueryForOtherGeographyL0Level(panGeoGraphyName, domain, vendor, neType);
        } else if (reportLevel.equalsIgnoreCase(PMConstants.GEOGRAPHYL1_LEVEL)) {
            dynamicQuery = getQueryForOtherGeographyLevel(reportConfiguration, panGeoGraphyName, domain, vendor, neType,
                    dateCenters);
        } else if (reportLevel.equalsIgnoreCase(PMConstants.CUSTOM)) {
            dynamicQuery = getQueryForCustomCellsForCore(reportConfiguration, domain, vendor, panGeoGraphyName);
        } else {
            dynamicQuery = getQueryForOtherGeographyNodeLevel(reportConfiguration, panGeoGraphyName, domain, vendor,
                    neType, corePodLevel, dateCenters, reportLevel);
        }
        return dynamicQuery;
    }

    private String getQueryForCustomCellsForCore(String reportConfiguration, String domain, String vendor,
            String panGeoGraphyName) {
        try {
            String query = "SELECT concat(hb.alphanumericcode,'#',ne.neid,'#','" + panGeoGraphyName
                    + "') as CRC from NETWORK_ELEMENT ne ,HbaseRowPrefix hb where ne.domain = hb.domain and ne.vendor = hb.vendor and hb.l1 is null and hb.othergeographyid_fk is null ";
            String condition = " and ne.deleted = 0 ";
            JSONObject json = new JSONObject(reportConfiguration);
            List<String> cellsList = getListFromJsonArray(getJsonArray(json, PMConstants.CELLS_JSONKEY));
            String cells = (!cellsList.isEmpty() && !cellsList.get(0).equalsIgnoreCase(""))
                    ? String.join(PMConstants.COMMA, cellsList)
                    : null;
            if (domain != null) {
                condition = condition + " AND ne.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND ne.vendor ='" + vendor + "'";
            }
            if (!cellsList.isEmpty() && cells != null) {
                condition = condition + " AND ne.nename in ('" + cells.replaceAll(PMConstants.COMMA, "','") + "')";
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception while getting dynamic query for custom Upload cells :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForOtherGeographyL0Level(String panGeoGraphyName, String domain, String vendor,
            String neType) {
        try {
            String sqlQuery = "select concat(hb.alphanumericcode,'#','" + panGeoGraphyName.toUpperCase() + "_" + neType
                    + "','#','" + panGeoGraphyName.toUpperCase() + "') From HbaseRowPrefix hb";
            String condition = " where hb.l1 is null and hb.othergeographyid_fk is null ";
            if (domain != null) {
                condition = condition + " and hb.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " and hb.vendor ='" + vendor + "'";
            } else {
                condition = condition + " and hb.vendor is null ";
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception in getting GeographyL0 Level query :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForOtherGeographyLevel(String reportConfiguration, String panGeoGraphyName, String domain,
            String vendor, String neType, List<String> dateCenters) {
        try {
            JSONObject json = new JSONObject(reportConfiguration);
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            List<String> geoL1List = getListFromJsonArray(l1List);
            String dataCenter = getSingleQuoteString(dateCenters);
            if (geoL1List.get(0).contains(PMConstants.INDIVIDUAL)) {
                String georaphylevel = getNodeName(geoL1List.get(0));
                dataCenter = dateCenters.contains(georaphylevel) ? "'" + georaphylevel + "'" : dataCenter;
            }
            logger.error("Fetch data center for other geo type : {}", dataCenter);
            String sqlQuery = "SELECT distinct concat(hb.alphanumericcode,'#',o.name,'_',n.netype,'#','"
                    + panGeoGraphyName.toUpperCase()
                    + "') as CRC from NEGeographyMapping ne , NETWORK_ELEMENT n,OtherGeography o,HbaseRowPrefix hb where  n.networkElementid_pk = ne.networkElementid_fk and o.othergeographyid_pk=ne.othergeographyid_fk "
                    + " and upper(n.domain)= upper(hb.domain) and upper(n.vendor)=upper(hb.vendor) and o.type in ("
                    + dataCenter
                    + ") and hb.l1 is null and hb.l2 is null and hb.l3 is null and hb.othergeographyid_fk is null ";
            String condition = " and hb.l1 is null and hb.l2 is null and hb.l3 is null and hb.othergeographyid_fk is null ";
            String l1 = getValueFromJsonArray(l1List);

            if (domain != null) {
                condition = condition + " AND n.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND n.vendor ='" + vendor + "'";
            }
            if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                condition = condition + " AND n.neType in ('" + neType + "')";
            }
            if (l1List.length() > 0 && l1 != null) {
                condition = condition + " AND o.othergeographyid_pk IN (" + l1 + ")";
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception while getting OtherGeography leve1 dynamic Query :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForOtherGeographyNodeLevel(String configuation, String panGeoGraphyName, String domain,
            String vendor, String neType, String level, List<String> dateCenters, String reportLevel) {
        try {
            JSONObject json = new JSONObject(configuation);
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);

            List<String> geoL1List = getListFromJsonArray(l1List);
            String dataCenter = getSingleQuoteString(dateCenters);
            if (geoL1List.get(0).contains(PMConstants.INDIVIDUAL)) {
                String georaphylevel = getNodeName(geoL1List.get(0));
                dataCenter = dateCenters.contains(georaphylevel) ? "'" + georaphylevel + "'" : dataCenter;
            }
            logger.error("Fetch data center for node leve type : {}", dataCenter);
            neType = "" + neType.replaceAll("'", "") + "";
            List<String> neTypeList = Arrays.asList(neType.split(","));
            logger.info("Fetch neTypeList center for neTypeList : {} and reportlevel : {}", neTypeList, reportLevel);

            if (neTypeList.contains(reportLevel)) {
                neType = reportLevel;
                logger.info("final neType : {} and reportlevel : {}", neType, reportLevel);

            }
            String query = null;
            if (l1List.length() > 0 && l1 != null) {
                query = "SELECT distinct concat(hb.alphanumericcode,'#',n.neid,'#','" + panGeoGraphyName
                        + "') as CRC from NEGeographyMapping ne , NETWORK_ELEMENT n,OtherGeography o,HbaseRowPrefix hb where  n.networkElementid_pk = ne.networkElementid_fk and o.othergeographyid_pk=ne.othergeographyid_fk "
                        + " and upper(n.domain)= upper(hb.domain) and upper(n.vendor)=upper(hb.vendor) and o.type in ("
                        + dataCenter
                        + ") and hb.l1 is null and hb.l2 is null and hb.l3 is null and hb.othergeographyid_fk is null "
                        + " and o.othergeographyid_pk IN (" + l1 + ")";
            } else {
                // query = "SELECT
                // concat(hb.alphanumericcode,'#',n.neid,'#','"+panGeoGraphyName+"') as CRC from
                // NETWORK_ELEMENT n ,HbaseRowPrefix hb where n.domain = hb.domain and n.vendor
                // =
                // hb.vendor and hb.l1 is null and hb.othergeographyid_fk is null ";
                query = "SELECT distinct concat(hb.alphanumericcode,'#',n.neid,'#','" + panGeoGraphyName
                        + "') as CRC from NEGeographyMapping ne , NETWORK_ELEMENT n,OtherGeography o,HbaseRowPrefix hb where  n.networkElementid_pk = ne.networkElementid_fk and o.othergeographyid_pk=ne.othergeographyid_fk "
                        + " and upper(n.domain)= upper(hb.domain) and upper(n.vendor)=upper(hb.vendor) and o.type in ("
                        + dataCenter
                        + ") and hb.l1 is null and hb.l2 is null and hb.l3 is null and hb.othergeographyid_fk is null ";
            }

            String condition = Symbol.EMPTY_STRING;

            if (domain != null) {
                condition = condition + " and n.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " and n.vendor ='" + vendor + "'";
            }
            if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                condition = condition + " and n.neType in ('" + neType + "')";
            }
            if (LEVEL_VNF.equalsIgnoreCase(level) || LEVEL_VNFC.equalsIgnoreCase(level)
                    || LEVEL_BAREMETAL.equalsIgnoreCase(level)) {
                condition = condition + " and n.category = '" + level + "'";
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception in parse Json OtherGeography node level query :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    @SuppressWarnings("resource")
    private Map<String, String> getVendorandAppenderforIndKpi(String domain) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        ResultSet rs1 = null;
        Map<String, String> list = new HashMap<>();
        try {
            Map<String, String> contextMap = new HashMap<>();
            String vendor = null;
            String technology = null;
            String rowKeyAppender = null;
            contextMap = jobcontext.getParameters();
            String IndkpiIds = contextMap.get("IndkpiIds");
            IndkpiIds = IndkpiIds.replaceAll(",", "','");
            String sql = "select distinct vendor,technology from KPIFormula where kpicode in ('" + IndkpiIds
                    + "') and vendor is not null and technology is not null";
            final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
            final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
            final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
            final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                if (vendor != null && technology != null) {
                    vendor = !vendor.contains(rs.getString(1)) ? vendor + Symbol.COMMA_STRING + rs.getString(1)
                            : vendor;
                    technology = !technology.contains(rs.getString(2))
                            ? technology + Symbol.COMMA_STRING + rs.getString(2)
                            : technology;
                } else {
                    vendor = rs.getString(1);
                    technology = rs.getString(2);
                }
                // logger.info("vendor : {} , technology : {} ",vendor,technology );
            }
            technology = technology.replaceAll(",", "','");
            vendor = vendor.replaceAll(",", "','");
            String query = "select distinct CONCAT(IF((rowkeytechnology IS NOT NULL),'_',''),COALESCE(rowkeytechnology, ''),IF((networkType IS NOT NULL),'_',''),COALESCE(networkType, '')) as rowKeyAppender "
                    + " From PMNodeVendor where domain = '" + domain + "'  and technology in('" + technology + "')";
            stmt = conn.prepareStatement(query);
            rs1 = stmt.executeQuery();
            while (rs1.next()) {
                if (rowKeyAppender != null) {
                    rowKeyAppender = rowKeyAppender + Symbol.COMMA_STRING + rs1.getString(1);
                } else {
                    rowKeyAppender = rs1.getString(1);
                }
            }
            list.put("Vendor", vendor);
            list.put("Appender", rowKeyAppender);
            logger.info("Indvidual Appender List => sql : {} query : {} list : {}  ", sql, query, list);
        } catch (Exception e) {
            logger.error("Exception While getting vendor and  RowKeyAppender :{}", e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Error While Getting vendor and  RowKeyAppender Value in Closing ResultSet - {}",
                            e.getMessage());
                }
            }
            if (rs1 != null) {
                try {
                    rs1.close();
                } catch (SQLException e) {
                    logger.error("Error While Getting  vendor and  RowKeyAppender Value in Closing ResultSet - {}",
                            e.getMessage());
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error(
                            "Error While Getting vendor and  RowKeyAppender Value in Closing Statement in parse - {}",
                            e.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Error While Getting RowKeyAppender Value in Closing connection in parse- {}",
                            e.getMessage());
                }
            }
        }
        return list;
    }

    private String getQueryForNodeLevelWithMagnetFields(String reportConfiguration, String domain, String vendor,
            String reportLevel, String neType, String siteCategory, String neStatus, String systemConfig,
            String magnetField, String technology) {
        try {
            String query = "";
            if (siteCategory != null && !siteCategory.isEmpty() && !siteCategory.equalsIgnoreCase("ALL")) {
                String networkMapping = reportLevel.equalsIgnoreCase(PMConstants.ENODEB_NODE) ? "ne.networkelementid_pk"
                        : "ne.networkelementid_fk";
                query = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne JOIN NEDetail nd on " + networkMapping
                        + "=nd.networkelementid_fk JOIN CustomNEDetail cs ON cs.networkelementid_fk=ne.networkelementid_pk JOIN PRIMARY_GEO_L2 gl2 "
                        + "ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor"
                        + " AND ne.geographyl3id_fk = hb.geographyl3id_fk ";
            } else {
                query = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne JOIN CustomNEDetail cs ON "
                        + "cs.networkelementid_fk=ne.networkelementid_pk JOIN PRIMARY_GEO_L2 gl2 ON  ne.geographyl2id_fk = gl2.geographyl2id_pk "
                        + "JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk ";
            }
            JSONObject json = new JSONObject(reportConfiguration);
            String cellOwner = getStringValueFromJsonArray(getJsonArray(json, "cellOwner"));
            cellOwner = StringUtils.replaceIgnoreCase(cellOwner, ",", "','");
            Map<String, Object> map = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> readValue = mapper.readValue(magnetField,
                    new TypeReference<List<Map<String, Object>>>() {
                    });
            readValue.forEach(a -> {
                map.put((String) a.get("columnName"), a.get("value"));
            });
            logger.info("map : {}", map.toString());
            JSONArray groupcenterList = getJsonArray(json, PMConstants.GROUP_CENTER_JSONKEY);
            String gc = getValueFromJsonArray(groupcenterList);
            List<String> parentNodes = getValueListFromJsonByKey(systemConfig, "PARENT_NODE");
            List<String> baseNodes = getValueListFromJsonByKey(systemConfig, "BASE_NODE");
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            JSONArray l3List = getJsonArray(json, PMConstants.GEOGRAPHY_L3_JSONKEY);
            JSONArray l4List = getJsonArray(json, PMConstants.GEOGRAPHY_L4_JSONKEY);
            String bandType = getValueFromJson(json, PMConstants.BANDTYPE_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            String l3 = getValueFromJsonArray(l3List);
            String l4 = getValueFromJsonArray(l4List);
            String zoneGeography = getStringValueFromJsonArray(l1List);

            if (groupcenterList != null && groupcenterList.length() > 0 && gc != null) {
                if (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel)) {
                    query = query + " JOIN NELocation nl on nl.nelocationid_pk=ne.nelocationid_fk ";
                } else {
                    String node = PMConstants.VCU;
                    if (reportLevel.equalsIgnoreCase(PMConstants.ODSC)) {
                        node = PMConstants.ODSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.IDSC)) {
                        node = PMConstants.IDSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.FEMTO)) {
                        node = PMConstants.FEMTO_VCU;
                    }
                    String parentneType = getNETypeList(domain, vendor, node, systemConfig, technology);
                    query = query
                            + " INNER JOIN (SELECT pne.networkelementid_pk FROM NETWORK_ELEMENT pne JOIN NELocation nl ON nl.nelocationid_pk = pne.nelocationid_fk AND nl.nelType = 'GC' AND pne.neType = "
                            + parentneType + " AND nl.nelocationid_pk IN (" + gc
                            + ")) as l ON (ne.parentneid_fk=l.networkelementid_pk) ";
                }
            }

            if (zoneGeography != null && PMConstants.ALL_ZONE_INDIVIDUAL.equalsIgnoreCase(zoneGeography)) {
                query = query
                        + " JOIN NEGeographyMapping ng on ne.networkelementid_pk = ng.networkelementid_fk JOIN OtherGeography o on o.othergeographyid_pk= ng.othergeographyid_fk ";
            }

            String condition = " WHERE ne.deleted = 0 ";
            if (domain != null) {
                condition = condition + " and ne.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND ne.vendor ='" + vendor + "'";
            }
            if (technology != null && vendor.equalsIgnoreCase(PMConstants.All)
                    && !technology.equalsIgnoreCase(PMConstants.All)) {
                technology = !technology.equalsIgnoreCase("LTE") ? "T" + technology : technology;
                condition = condition + " AND ne.technology ='" + technology + "'";
            }
            if (zoneGeography != null && PMConstants.ALL_ZONE_INDIVIDUAL.equalsIgnoreCase(zoneGeography)) {
                condition = condition + " AND upper(o.type) = upper('ZONE') ";
            }
            if (groupcenterList != null && groupcenterList.length() > 0 && gc != null
                    && (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel))) {
                condition = condition + " and nl.nelType='GC' and nl.nelocationid_pk in (" + gc + ") ";
            }
            if (l1List.length() > 0 && l1 != null) {
                condition = condition + " AND ne.geographyl1id_fk IN (" + l1 + ")";
            }
            if (l2List.length() > 0 && l2 != null) {
                condition = condition + " AND ne.geographyl2id_fk IN (" + l2 + ")";
            }
            if (l3List.length() > 0 && l3 != null) {
                condition = condition + " AND ne.geographyl3id_fk IN (" + l3 + ")";
            }
            if (l4List.length() > 0 && l4 != null) {
                condition = condition + " AND ne.geographyl4id_fk IN (" + l4 + ")";
            }
            if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                condition = condition + " AND ne.neType in (" + neType + ")";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                condition = condition + " AND cs.cellOwner in ('" + cellOwner + "')";
            }
            if (baseNodes != null && !baseNodes.isEmpty() && baseNodes.contains(reportLevel)) {
                if (bandType != null && !bandType.isEmpty() && !bandType.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nefrequency = '" + bandType + "'";
                }
                if (neStatus != null && !neStatus.isEmpty() && !neStatus.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nestatus = '" + neStatus + "'";
                }
            }
            if (siteCategory != null && !siteCategory.isEmpty() && !siteCategory.equalsIgnoreCase("ALL")) {
                condition = condition + " AND nd.category ='" + siteCategory + "'";
            }
            for (Entry<String, Object> entry : map.entrySet()) {
                String column = entry.getKey();
                String value = StringUtils.replaceIgnoreCase(
                        StringUtils.replaceIgnoreCase(
                                StringUtils.replaceIgnoreCase(entry.getValue().toString().trim(), "[", "'"), "]", "'"),
                        ", ", "','").trim();
                logger.info("column : {} ,value : {} ", column, value);
                condition = condition + " AND cs." + column + " in (" + value + ")";
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception in parse Json for cell Level query :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private Map<String, List<String>> getQueryForGeographyLevels(String reportConfiguration, String domain,
            String vendor, String targetArea, List<String> l1List, List<String> l2List, List<String> l3List,
            List<String> l4List, String nodeString) throws Exception {

        logger.info(
                "Getting Query For Geography Levels, Domain={}, Vendor={}, L1 List={}, L2 List={}, L3 List={}, L4 List={}, Node String={}",
                domain, vendor, l1List, l2List, l3List, l4List, nodeString);

        Map<String, List<String>> filterLevelWiseGeoNames = new HashMap<>();

        String dynamicQuery = null;
        String level = null;

        Map<String, String> contextMap = jobcontext.getParameters();
        List<String> geoNames = new ArrayList<>();

        JSONObject json = new JSONObject(reportConfiguration);
        JSONArray l1list = getJsonArray(json, "geography_l1");
        JSONArray l2list = getJsonArray(json, "geography_l2");
        JSONArray l3list = getJsonArray(json, "geography_l3");
        JSONArray l4list = getJsonArray(json, "geography_l4");

        logger.info("L1 List={}, L2 List={}, L3 List={}, L4 List={}", l1list, l2list, l3list, l4list);

        String l1 = getValueFromJsonArray(l1list);
        String l2 = getValueFromJsonArray(l2list);
        String l3 = getValueFromJsonArray(l3list);
        String l4 = getValueFromJsonArray(l4list);

        logger.info("L1={}, L2={}, L3={}, L4={}", l1, l2, l3, l4);

        if (!l1.isEmpty() && l1.trim().toUpperCase().contains("CLUBBED")) {
            logger.info("Processing L0 Level Report!");
            level = "L1";
        } else if (!StringUtils.isEmpty(l1) && !l2List.isEmpty()
                && l2List.get(0).contains(PMConstants.ALL_GEOGRAPHYL2_CLUBBED)) {

            logger.info("Processing L1 Level Report!");
            dynamicQuery = getQueryForGeographyL1Level(reportConfiguration, domain, vendor);
            level = "L1";
        } else if (!StringUtils.isEmpty(l2) && !l3List.isEmpty()
                && l3List.get(0).contains(PMConstants.ALL_GEOGRAPHYL3_CLUBBED)) {

            logger.info("Processing L2 Level Report!");
            dynamicQuery = getQueryForGeographyL2Level(reportConfiguration, domain, vendor);
            level = "L2";
        } else if (!StringUtils.isEmpty(l3) && !l4List.isEmpty()
                && l4List.get(0).contains(PMConstants.ALL_GEOGRAPHYL4_CLUBBED)) {

            logger.info("Processing L3 Level Report!");
            dynamicQuery = getQueryForGeographyL3Level(reportConfiguration, domain, vendor);
            level = "L3";
        } else if (!StringUtils.isEmpty(l4) && !l1List.get(0).contains("Custom")
                && nodeString.contains(PMConstants.CLUBBED_CAPS)) {

            logger.info("Processing L4 Level Report!");
            dynamicQuery = getQueryForGeographyL4Level(reportConfiguration, domain, vendor);
            level = "L4";
        } else if (l1List.get(0).equalsIgnoreCase(PMConstants.GROUP_CENTER)) {

            logger.info("Processing Group Center Level Report!");
            dynamicQuery = getQueryForGC(reportConfiguration, domain, vendor);
        } else if (l1List.get(0).equalsIgnoreCase(PMConstants.ALL_ZONE_INDIVIDUAL)) {

            logger.info("Processing Zone Level Report!");
            dynamicQuery = getQueryForZone(domain, vendor, targetArea);
        }

        logger.info("Dynamic Query To Get The Geo Names={}", dynamicQuery);

        if (StringUtils.isNotEmpty(dynamicQuery)) {
            ResultSet resultSet = createConnection(dynamicQuery, contextMap);
            while (resultSet.next()) {
                geoNames.add(resultSet.getString(1));
            }
            filterLevelWiseGeoNames.put(level, geoNames);
        }
        return filterLevelWiseGeoNames;
    }

    private String getQueryForZone(String domain, String vendor, String targetArea) {
        try {
            logger.info("going to get Query for Zone Level Report");
            String sqlQuery = "SELECT distinct o.name from OtherGeography o,HbaseRowPrefix hb where ";
            String condition = " hb.l1 is null and hb.l2 is null and hb.l3 is null and hb.othergeographyid_fk is null and upper(o.type)=upper('ZONE') ";
            if (domain != null) {
                condition = condition + " AND hb.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND hb.vendor in ('" + vendor + "')";
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception while getting Zone level dynamic Query :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForGC(String reportConfiguration, String domain, String vendor) {
        try {
            logger.info("going to get Query for Group center Level Report");
            String sqlQuery = "SELECT nel.nelId FROM NELocation nel JOIN PRIMARY_GEO_L3 gl3 ON nel.geographyl3id_fk = gl3.geographyl3id_pk JOIN PRIMARY_GEO_L2 gl2 ON gl3.geographyl2id_fk = gl2.geographyl2id_pk JOIN PRIMARY_GEO_L1 gl1 ON gl2.geographyl1id_fk = gl1.geographyl1id_pk JOIN HbaseRowPrefix hb ON gl3.name = hb.l3 ";
            String condition = " Where nel.nelType='GC' and ";
            JSONObject json = new JSONObject(reportConfiguration);
            JSONArray groupcenterList = getJsonArray(json, PMConstants.GROUP_CENTER_JSONKEY);
            String gc = getValueFromJsonArray(groupcenterList);
            if (domain != null) {
                condition = condition + " hb.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND hb.vendor in ('" + vendor + "')";
            }
            if (groupcenterList.length() > 0 && gc != null) {
                condition = condition + " AND nel.nelocationid_pk IN (" + gc + ")";
            }

            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception while getting group center level dynamic Query :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForGeographyL4Level(String reportConfiguration, String domain, String vendor) {
        try {
            logger.info("going to get Query for PRIMARY_GEO_L4 Level Report");
            String sqlQuery = "";
            sqlQuery = "SELECT UPPER(gl4.GEO_NAME) AS CRC FROM PRIMARY_GEO_L4 gl4 JOIN PRIMARY_GEO_L3 gl3 ON gl4.PRIMARY_GEO_L3_ID_FK = gl3.ID JOIN PRIMARY_GEO_L2 gl2 ON gl3.PRIMARY_GEO_L2_ID_FK = gl2.ID JOIN PRIMARY_GEO_L1 gl1 ON gl2.PRIMARY_GEO_L1_ID_FK = gl1.ID  ";
            String condition = " ";
            JSONObject json = new JSONObject(reportConfiguration);
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            JSONArray l3List = getJsonArray(json, PMConstants.GEOGRAPHY_L3_JSONKEY);
            JSONArray l4List = getJsonArray(json, PMConstants.GEOGRAPHY_L4_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            String l3 = getValueFromJsonArray(l3List);
            String l4 = getValueFromJsonArray(l4List);
            if (l1List.length() > 0 && l1 != null) {
                l1 = l1.replaceAll(PMConstants.COMMA, "','");
                condition = condition + " Where gl1.GEO_NAME IN ('" + l1 + "')";
            }
            if (l2List.length() > 0 && l2 != null) {
                l2 = l2.replaceAll(PMConstants.COMMA, "','");
                condition = condition + " AND gl2.GEO_NAME IN ('" + l2 + "')";
            }
            if (l3List.length() > 0 && l3 != null) {
                l3 = l3.replaceAll(PMConstants.COMMA, "','");
                condition = condition + " AND gl3.GEO_NAME IN ('" + l3 + "')";
            }
            if (l4List.length() > 0 && l4 != null) {
                l4 = l4.replaceAll(PMConstants.COMMA, "','");

                l4 = l4.toUpperCase();
                if (l4.contains("CLUBBED") || l4.contains("INDIVIDUAL")) {
                    // Nothing to do
                } else {
                    condition = condition + " AND gl4.GEO_NAME IN ('" + l4 + "')";
                }
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception while getting PRIMARY_GEO_L4 level dynamic Query :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForGeographyL3Level(String reportConfiguration, String domain, String vendor) {
        try {
            logger.info("going to get Query for PRIMARY_GEO_L3 Level Report");
            String sqlQuery = "SELECT UPPER(gl3.GEO_NAME) AS CRC FROM PRIMARY_GEO_L3 gl3 JOIN PRIMARY_GEO_L2 gl2 ON gl3.PRIMARY_GEO_L2_ID_FK = gl2.ID JOIN PRIMARY_GEO_L1 gl1 ON gl2.PRIMARY_GEO_L1_ID_FK = gl1.ID  ";
            String condition = "";
            JSONObject json = new JSONObject(reportConfiguration);
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            JSONArray l3List = getJsonArray(json, PMConstants.GEOGRAPHY_L3_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            String l3 = getValueFromJsonArray(l3List);
            if (l1List.length() > 0 && l1 != null) {
                l1 = l1.replaceAll(PMConstants.COMMA, "','");
                condition = condition + " Where gl1.GEO_NAME IN ('" + l1 + "')";
            }
            if (l2List.length() > 0 && l2 != null) {
                l2 = l2.replaceAll(PMConstants.COMMA, "','");
                condition = condition + " AND gl2.GEO_NAME IN ('" + l2 + "')";
            }
            if (l3List.length() > 0 && l3 != null) {

                l3 = l3.toUpperCase();
                if (l3.contains("CLUBBED") || l3.contains("INDIVIDUAL")) {
                    // Nothing to do
                } else {
                    l3 = l3.replaceAll(PMConstants.COMMA, "','");
                    condition = condition + " AND gl3.GEO_NAME IN ('" + l3 + "')";
                }
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception while getting PRIMARY_GEO_L3 level dynamic Query :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForGeographyL2Level(String reportConfiguration, String domain, String vendor) {
        try {
            logger.info("going to get Query for PRIMARY_GEO_L2 Level Report");
            String sqlQuery = "SELECT UPPER(gl2.GEO_NAME) AS CRC FROM PRIMARY_GEO_L2 gl2 JOIN PRIMARY_GEO_L1 gl1 ON gl2.PRIMARY_GEO_L1_ID_FK = gl1.ID  ";
            String condition = "";
            JSONObject json = new JSONObject(reportConfiguration);
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            if (l1List.length() > 0 && l1 != null) {
                l1 = l1.replaceAll(PMConstants.COMMA, "','");
                condition = condition + "Where gl1.GEO_NAME IN ('" + l1 + "')";
            }
            if (l2List.length() > 0 && l2 != null) {
                l2 = l2.replaceAll(PMConstants.COMMA, "','");
                condition = condition + " AND gl2.GEO_NAME IN ('" + l2 + "')";
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception while getting PRIMARY_GEO_L2 level dynamic Query :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForGeographyL0Level(String targetArea, String domain, String vendor) {
        try {
            logger.info("going to get GeographyL0 Query");
            String sqlQuery = "select '" + targetArea.toUpperCase() + "' as CRC From dual";
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception in getting GeographyL0 Level query :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForGeographyL1Level(String reportConfiguration, String domain, String vendor) {
        try {
            logger.info("===== Going to Get Query For PRIMARY_GEO_L1 Level Report =====");

            String sqlQuery = "SELECT UPPER(gl1.GEO_NAME) AS CRC FROM PRIMARY_GEO_L1 gl1 ";
            String condition = "";
            JSONObject json = new JSONObject(reportConfiguration);
            JSONArray l1List = getJsonArray(json, "geography_l1");
            String l1 = getValueFromJsonArray(l1List);
            if (l1List.length() > 0 && l1 != null) {
                l1 = l1.replaceAll(PMConstants.COMMA, "','");
                condition = condition + "WHERE gl1.GEO_NAME IN ('" + l1 + "')";
            }
            sqlQuery = sqlQuery + condition;
            return sqlQuery;
        } catch (Exception e) {
            logger.error("Exception While Getting PRIMARY_GEO_L1 Level Query, Message={}, Error={}", e.getMessage(), e);
        }
        return null;
    }

    private String getQueryForChildNodeLevel(String configuation, String domain, String vendor, String reportLevel,
            String neType, String neStatus, String systemConfig, String childNode, String technology) {
        try {
            String query = "SELECT concat(hb.alphanumericcode,'#',concat(ne.neid,'#',mo.moname),'#',gl2.name) as CRC FROM NETWORK_ELEMENT ne JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk ";
            JSONObject json = new JSONObject(configuation);
            String cellOwner = getStringValueFromJsonArray(getJsonArray(json, "cellOwner"));
            cellOwner = StringUtils.replaceIgnoreCase(cellOwner, ",", "','");
            JSONArray groupcenterList = getJsonArray(json, PMConstants.GROUP_CENTER_JSONKEY);
            String gc = getValueFromJsonArray(groupcenterList);
            List<String> parentNodes = getValueListFromJsonByKey(systemConfig, "PARENT_NODE");
            List<String> baseNodes = getValueListFromJsonByKey(systemConfig, "BASE_NODE");
            if (groupcenterList.length() > 0 && gc != null) {
                if (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel)) {
                    query = query + " JOIN NELocation nl on nl.nelocationid_pk=ne.nelocationid_fk ";
                } else {
                    String node = PMConstants.VCU;
                    if (reportLevel.equalsIgnoreCase(PMConstants.ODSC)) {
                        node = PMConstants.ODSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.IDSC)) {
                        node = PMConstants.IDSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.FEMTO)) {
                        node = PMConstants.FEMTO_VCU;
                    }
                    String parentneType = getNETypeList(domain, vendor, node, systemConfig, technology);
                    query = query
                            + " INNER JOIN (SELECT pne.networkelementid_pk FROM NETWORK_ELEMENT pne JOIN NELocation nl ON nl.nelocationid_pk = pne.nelocationid_fk AND nl.nelType = 'GC' AND pne.neType = "
                            + parentneType + " AND nl.nelocationid_pk IN (" + gc
                            + ")) as l ON (ne.parentneid_fk=l.networkelementid_pk) ";
                }
            }
            if (childNode != null) {
                query = query + " JOIN MoMetaData mo on ne.networkelementid_pk = mo.networkelementid_fk ";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                query = query + "  JOIN CustomNEDetail cs ON cs.networkelementid_fk=ne.networkelementid_pk ";
            }
            String condition = " WHERE ne.deleted = 0 ";
            condition = condition + " and mo.motype = '" + childNode + "' ";
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            JSONArray l3List = getJsonArray(json, PMConstants.GEOGRAPHY_L3_JSONKEY);
            JSONArray l4List = getJsonArray(json, PMConstants.GEOGRAPHY_L4_JSONKEY);
            String bandType = getValueFromJson(json, PMConstants.BANDTYPE_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            String l3 = getValueFromJsonArray(l3List);
            String l4 = getValueFromJsonArray(l4List);

            if (domain != null) {
                condition = condition + " and ne.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND ne.vendor ='" + vendor + "'";
            }
            if (technology != null && vendor.equalsIgnoreCase(PMConstants.All)
                    && !technology.equalsIgnoreCase(PMConstants.All)) {
                technology = !technology.equalsIgnoreCase("LTE") ? "T" + technology : technology;
                condition = condition + " AND ne.technology ='" + technology + "'";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                condition = condition + " AND cs.cellOwner in ('" + cellOwner + "')";

            }
            if (groupcenterList.length() > 0 && gc != null
                    && (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel))) {
                condition = condition + " and nl.nelType='GC' and nl.nelocationid_pk in (" + gc + ") ";
            }
            if (l1List.length() > 0 && l1 != null) {
                condition = condition + " AND ne.geographyl1id_fk IN (" + l1 + ")";
            }
            if (l2List.length() > 0 && l2 != null) {
                condition = condition + " AND ne.geographyl2id_fk IN (" + l2 + ")";
            }
            if (l3List.length() > 0 && l3 != null) {
                condition = condition + " AND ne.geographyl3id_fk IN (" + l3 + ")";
            }
            if (l4List.length() > 0 && l4 != null) {
                condition = condition + " AND ne.geographyl4id_fk IN (" + l4 + ")";
            }

            if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                condition = condition + " AND ne.neType in (" + neType + ")";
            }
            if (baseNodes != null && !baseNodes.isEmpty() && baseNodes.contains(reportLevel)) {
                if (bandType != null && !bandType.isEmpty() && !bandType.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nefrequency = '" + bandType + "'";
                }
                if (neStatus != null && !neStatus.isEmpty() && !neStatus.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nestatus = '" + neStatus + "'";
                }
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception in parse Json for child Node Level Query  :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    public static String getLevelForReport(List<String> l1List, List<String> l2List, List<String> l3List,
            List<String> l4List, List<String> cells, String node) {

        if (node.toUpperCase().contains("INDIVIDUAL")) {
            return getNodeName(node);
        }

        String level;
        if (l1List.get(0).equalsIgnoreCase(PMConstants.CUSTOM) && cells != null && !cells.isEmpty()) {
            level = PMConstants.CUSTOM;
        } else if (l1List.get(0).equalsIgnoreCase(PMConstants.GROUP_CENTER)) {
            if (node.contains(PMConstants.CLUBBED_CAPS)) {
                level = PMConstants.GROUP_CENTER;
            } else {
                level = getNodeName(node);
            }
        } else if (l1List.get(0).equalsIgnoreCase(PMConstants.ALL_ZONE_INDIVIDUAL)) {
            if (node.contains(PMConstants.CLUBBED_CAPS)) {
                level = PMConstants.GEOGRAPHY_LEVEL_ZONE;
            } else {
                level = getNodeName(node);
            }
        } else if (!l1List.get(0).contains(PMConstants.ALL_GEOGRAPHYL1_CLUBBED)
                && !l1List.get(0).equalsIgnoreCase(PMConstants.GROUP_CENTER)) {
            if (!l2List.get(0).contains(PMConstants.ALL_GEOGRAPHYL2_CLUBBED)) {
                if (!l3List.get(0).contains(PMConstants.ALL_GEOGRAPHYL3_CLUBBED)) {
                    if (!l4List.get(0).contains(PMConstants.ALL_GEOGRAPHYL4_CLUBBED)) {
                        if (node.contains(PMConstants.CLUBBED_CAPS)) {
                            level = PMConstants.GEOGRAPHYL4_LEVEL;
                        } else {
                            level = getNodeName(node);
                        }
                    } else {
                        level = PMConstants.GEOGRAPHYL3_LEVEL;
                    }
                } else {
                    level = PMConstants.GEOGRAPHYL2_LEVEL;
                }
            } else {
                level = PMConstants.GEOGRAPHYL1_LEVEL;
            }
        } else {
            level = PMConstants.GEOGRAPHYL0_LEVEL;
        }
        return level;
    }

    private String getQueryForCustomCells(String reportConfiguration, String domain, String vendor, String technology,
            String magnetField) {
        try {
            String query = "";
            if (magnetField != null && !magnetField.isEmpty()) {
                query = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne JOIN CustomNEDetail cs ON cs.networkelementid_fk=ne.networkelementid_pk  JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk ";
            } else {
                query = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk ";

            }
            String condition = " WHERE ne.deleted = 0 ";
            JSONObject json = new JSONObject(reportConfiguration);
            List<String> cellsList = getListFromJsonArray(getJsonArray(json, PMConstants.CELLS_JSONKEY));
            String cells = (!cellsList.isEmpty() && !cellsList.get(0).equalsIgnoreCase(""))
                    ? String.join(PMConstants.COMMA, cellsList)
                    : null;
            if (domain != null) {
                condition = condition + " AND ne.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND  ne.vendor ='" + vendor + "'";
            }
            if (!cellsList.isEmpty() && cells != null) {
                condition = condition + " AND ne.nename in ('" + cells.replaceAll(PMConstants.COMMA, "','") + "')";
            }
            if (technology != null && !technology.equalsIgnoreCase(PMConstants.All)) {
                technology = !technology.equalsIgnoreCase("LTE") ? "T" + technology : technology;
                condition = condition + " AND ne.technology ='" + technology + "'";
            }
            if (magnetField != null && !magnetField.isEmpty()) {
                magnetField = Arrays.asList(magnetField.split(",")).toString().trim();
                Map<String, Object> map = new HashMap<>();
                ObjectMapper mapper = new ObjectMapper();
                List<Map<String, Object>> readValue = mapper.readValue(magnetField,
                        new TypeReference<List<Map<String, Object>>>() {
                        });
                readValue.forEach(a -> {
                    map.put((String) a.get("columnName"), a.get("value"));
                });
                logger.info("map : {}", map.toString());
                for (Entry<String, Object> entry : map.entrySet()) {
                    String column = entry.getKey();
                    String value = StringUtils.replaceIgnoreCase(StringUtils.replaceIgnoreCase(
                            StringUtils.replaceIgnoreCase(entry.getValue().toString().trim(), "[", "'"), "]", "'"),
                            ", ", "','").trim();
                    logger.info("column : {} ,value : {} ", column, value);
                    condition = condition + " AND cs." + column + " in (" + value + ")";
                }
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception while getting dynamic query for custom Upload cells :{}",
                    ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForNodeLevel(String configuation, String domain, String vendor, String reportLevel,
            String neType, String neStatus, String systemConfig, String technology) {
        try {
            List<String> linkNetypes = getValueListFromJsonByKey(systemConfig, "LINK_TYPES");
            String query;
            if (linkNetypes.contains(reportLevel)) {
                query = "SELECT concat(hb.alphanumericcode,'#',nc.linkid,'#',gl2.name) as CRC FROM NEConnectivity nc JOIN NETWORK_ELEMENT ne on nc.networkelementid_fk=ne.networkelementid_pk JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk ";
            } else {
                query = "SELECT ne.neid as CRC FROM NETWORK_ELEMENT ne ";
            }
            JSONObject json = new JSONObject(configuation);
            String cellOwner = getStringValueFromJsonArray(getJsonArray(json, "cellOwner"));
            cellOwner = StringUtils.replaceIgnoreCase(cellOwner, ",", "','");
            JSONArray groupcenterList = getJsonArray(json, PMConstants.GROUP_CENTER_JSONKEY);
            String gc = getValueFromJsonArray(groupcenterList);
            List<String> parentNodes = getValueListFromJsonByKey(systemConfig, "PARENT_NODE");
            List<String> baseNodes = getValueListFromJsonByKey(systemConfig, "BASE_NODE");
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            JSONArray l3List = getJsonArray(json, PMConstants.GEOGRAPHY_L3_JSONKEY);
            JSONArray l4List = getJsonArray(json, PMConstants.GEOGRAPHY_L4_JSONKEY);
            String bandType = getValueFromJson(json, PMConstants.BANDTYPE_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            String l3 = getValueFromJsonArray(l3List);
            String l4 = getValueFromJsonArray(l4List);
            String zoneGeography = getStringValueFromJsonArray(l1List);

            if (groupcenterList != null && groupcenterList.length() > 0 && gc != null) {
                if (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel)) {
                    query = query + " JOIN NELocation nl on nl.nelocationid_pk=ne.nelocationid_fk ";
                } else {
                    String node = PMConstants.VCU;
                    if (reportLevel.equalsIgnoreCase(PMConstants.ODSC)) {
                        node = PMConstants.ODSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.IDSC)) {
                        node = PMConstants.IDSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.FEMTO)) {
                        node = PMConstants.FEMTO_VCU;
                    }
                    String parentneType = getNETypeList(domain, vendor, node, systemConfig, technology);
                    query = query
                            + " INNER JOIN (SELECT pne.networkelementid_pk FROM NETWORK_ELEMENT pne JOIN NELocation nl ON nl.nelocationid_pk = pne.nelocationid_fk AND nl.nelType = 'GC' AND pne.neType = "
                            + parentneType + " AND nl.nelocationid_pk IN (" + gc
                            + ")) as l ON (ne.parentneid_fk=l.networkelementid_pk) ";
                }
            }

            if (zoneGeography != null && PMConstants.ALL_ZONE_INDIVIDUAL.equalsIgnoreCase(zoneGeography)) {
                query = query
                        + " JOIN NEGeographyMapping ng on ne.networkelementid_pk = ng.networkelementid_fk JOIN OtherGeography o on o.othergeographyid_pk= ng.othergeographyid_fk ";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                query = query + " JOIN CustomNEDetail cs ON cs.networkelementid_fk=ne.networkelementid_pk ";
            }

            String condition = " WHERE ne.deleted = 0 ";

            if (domain != null) {
                condition = condition + " and ne.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND ne.vendor ='" + vendor + "'";
            }
            if (technology != null && vendor.equalsIgnoreCase(PMConstants.All)
                    && !technology.equalsIgnoreCase(PMConstants.All)) {
                technology = !technology.equalsIgnoreCase("LTE") ? "T" + technology : technology;
                condition = condition + " AND ne.technology ='" + technology + "'";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                condition = condition + " AND cs.cellOwner in ('" + cellOwner + "')";

            }
            if (zoneGeography != null && PMConstants.ALL_ZONE_INDIVIDUAL.equalsIgnoreCase(zoneGeography)) {
                condition = condition + " AND upper(o.type) = upper('ZONE') ";
            }
            if (groupcenterList != null && groupcenterList.length() > 0 && gc != null
                    && (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel))) {
                condition = condition + " and nl.nelType='GC' and nl.nelocationid_pk in (" + gc + ") ";
            }
            if (l1List.length() > 0 && l1 != null) {
                condition = condition + " AND ne.geographyl1id_fk IN (" + l1 + ")";
            }
            if (l2List.length() > 0 && l2 != null) {
                condition = condition + " AND ne.geographyl2id_fk IN (" + l2 + ")";
            }
            if (l3List.length() > 0 && l3 != null) {
                condition = condition + " AND ne.geographyl3id_fk IN (" + l3 + ")";
            }
            if (l4List.length() > 0 && l4 != null) {
                condition = condition + " AND ne.geographyl4id_fk IN (" + l4 + ")";
            }
            if (linkNetypes.contains(reportLevel)) {
                condition = condition + " AND nc.linkType ='" + reportLevel + "'";
            }
            if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                condition = condition + " AND ne.neType in (" + neType + ")";
            }
            if (baseNodes != null && !baseNodes.isEmpty() && baseNodes.contains(reportLevel)) {
                if (bandType != null && !bandType.isEmpty() && !bandType.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nefrequency = '" + bandType + "'";
                }
                if (neStatus != null && !neStatus.isEmpty() && !neStatus.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nestatus = '" + neStatus + "'";
                }
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception in parse Json for cell Level query :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    private String getQueryForNodeLevelForSiteCategory(String configuation, String domain, String vendor,
            String reportLevel, String neType, String siteCategory, String neStatus, String systemConfig,
            String technology) {
        try {
            String networkMapping = reportLevel.equalsIgnoreCase(PMConstants.ENODEB_NODE) ? "ne.networkelementid_pk"
                    : "ne.networkelementid_fk";

            String query = "SELECT concat(hb.alphanumericcode,'#',ne.neid,'#',gl2.name) as CRC FROM NETWORK_ELEMENT ne JOIN NEDetail nd on "
                    + networkMapping
                    + "=nd.networkelementid_fk JOIN PRIMARY_GEO_L2 gl2 ON ne.geographyl2id_fk = gl2.geographyl2id_pk JOIN HbaseRowPrefix hb ON ne.domain = hb.domain AND ne.vendor = hb.vendor AND ne.geographyl3id_fk = hb.geographyl3id_fk ";
            JSONObject json = new JSONObject(configuation);
            String cellOwner = getStringValueFromJsonArray(getJsonArray(json, "cellOwner"));
            cellOwner = StringUtils.replaceIgnoreCase(cellOwner, ",", "','");
            JSONArray groupcenterList = getJsonArray(json, PMConstants.GROUP_CENTER_JSONKEY);
            String gc = getValueFromJsonArray(groupcenterList);
            List<String> parentNodes = getValueListFromJsonByKey(systemConfig, "PARENT_NODE");
            List<String> baseNodes = getValueListFromJsonByKey(systemConfig, "BASE_NODE");
            JSONArray l1List = getJsonArray(json, PMConstants.GEOGRAPHY_L1_JSONKEY);
            JSONArray l2List = getJsonArray(json, PMConstants.GEOGRAPHY_L2_JSONKEY);
            JSONArray l3List = getJsonArray(json, PMConstants.GEOGRAPHY_L3_JSONKEY);
            JSONArray l4List = getJsonArray(json, PMConstants.GEOGRAPHY_L4_JSONKEY);
            String bandType = getValueFromJson(json, PMConstants.BANDTYPE_JSONKEY);
            String l1 = getValueFromJsonArray(l1List);
            String l2 = getValueFromJsonArray(l2List);
            String l3 = getValueFromJsonArray(l3List);
            String l4 = getValueFromJsonArray(l4List);
            String zoneGeography = getStringValueFromJsonArray(l1List);

            if (groupcenterList != null && groupcenterList.length() > 0 && gc != null) {
                if (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel)) {
                    query = query + " JOIN NELocation nl on nl.nelocationid_pk=ne.nelocationid_fk ";
                } else {
                    String node = PMConstants.VCU;
                    if (reportLevel.equalsIgnoreCase(PMConstants.ODSC)) {
                        node = PMConstants.ODSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.IDSC)) {
                        node = PMConstants.IDSC_VCU;
                    } else if (reportLevel.equalsIgnoreCase(PMConstants.FEMTO)) {
                        node = PMConstants.FEMTO_VCU;
                    }
                    String parentneType = getNETypeList(domain, vendor, node, systemConfig, technology);
                    query = query
                            + " INNER JOIN (SELECT pne.networkelementid_pk FROM NETWORK_ELEMENT pne JOIN NELocation nl ON nl.nelocationid_pk = pne.nelocationid_fk AND nl.nelType = 'GC' AND pne.neType = "
                            + parentneType + " AND nl.nelocationid_pk IN (" + gc
                            + ")) as l ON (ne.parentneid_fk=l.networkelementid_pk) ";
                }
            }

            if (zoneGeography != null && PMConstants.ALL_ZONE_INDIVIDUAL.equalsIgnoreCase(zoneGeography)) {
                query = query
                        + " JOIN NEGeographyMapping ng on ne.networkelementid_pk = ng.networkelementid_fk JOIN OtherGeography o on o.othergeographyid_pk= ng.othergeographyid_fk ";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                query = query + " JOIN CustomNEDetail cs ON cs.networkelementid_fk=ne.networkelementid_pk ";

            }

            String condition = " WHERE ne.deleted = 0 ";
            if (domain != null) {
                condition = condition + " and ne.domain ='" + domain + "'";
            }
            if (vendor != null && !vendor.equalsIgnoreCase(PMConstants.All)) {
                condition = condition + " AND ne.vendor ='" + vendor + "'";
            }
            if (technology != null && vendor.equalsIgnoreCase(PMConstants.All)
                    && !technology.equalsIgnoreCase(PMConstants.All)) {
                technology = !technology.equalsIgnoreCase("LTE") ? "T" + technology : technology;
                condition = condition + " AND ne.technology ='" + technology + "'";
            }
            if (cellOwner != null && !cellOwner.isEmpty() && baseNodes.contains(reportLevel)) {
                condition = condition + " AND cs.cellOwner in ('" + cellOwner + "')";

            }
            if (zoneGeography != null && PMConstants.ALL_ZONE_INDIVIDUAL.equalsIgnoreCase(zoneGeography)) {
                condition = condition + " AND upper(o.type) = upper('ZONE') ";
            }
            if (groupcenterList != null && groupcenterList.length() > 0 && gc != null
                    && (parentNodes != null && !parentNodes.isEmpty() && parentNodes.contains(reportLevel))) {
                condition = condition + " and nl.nelType='GC' and nl.nelocationid_pk in (" + gc + ") ";
            }
            if (l1List.length() > 0 && l1 != null) {
                condition = condition + " AND ne.geographyl1id_fk IN (" + l1 + ")";
            }
            if (l2List.length() > 0 && l2 != null) {
                condition = condition + " AND ne.geographyl2id_fk IN (" + l2 + ")";
            }
            if (l3List.length() > 0 && l3 != null) {
                condition = condition + " AND ne.geographyl3id_fk IN (" + l3 + ")";
            }
            if (l4List.length() > 0 && l4 != null) {
                condition = condition + " AND ne.geographyl4id_fk IN (" + l4 + ")";
            }
            if (neType != null && !neType.isEmpty() && !neType.equalsIgnoreCase("ALL")) {
                condition = condition + " AND ne.neType in (" + neType + ")";
            }

            if (baseNodes != null && !baseNodes.isEmpty() && baseNodes.contains(reportLevel)) {
                if (bandType != null && !bandType.isEmpty() && !bandType.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nefrequency = '" + bandType + "'";
                }
                if (neStatus != null && !neStatus.isEmpty() && !neStatus.equalsIgnoreCase(PMConstants.All)) {
                    condition = condition + " AND ne.nestatus = '" + neStatus + "'";
                }
            }
            if (siteCategory != null && !siteCategory.isEmpty() && !siteCategory.equalsIgnoreCase("ALL")) {
                condition = condition + " AND nd.category ='" + siteCategory + "'";
            }
            query = query + condition;
            return query;
        } catch (Exception e) {
            logger.error("Exception in parse Json for cell Level query :{}", ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    public JSONArray getJsonArray(JSONObject json, String jsonKey) {
        JSONArray jsonArray = null;
        try {
            if (json.has(jsonKey) && !json.isNull(jsonKey)) {
                jsonArray = json.getJSONArray(jsonKey);
            }
        } catch (JSONException e) {
            logger.error("Exception in getting jsonArray from configJson :{}", ExceptionUtils.getStackTrace(e));
        }
        return jsonArray;
    }

    @Override
    public String getName() {
        return "ExtractOTFReportConfig";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        // fields.add(DataTypes.createStructField("reportName", DataTypes.StringType,
        // true));
        // fields.add(DataTypes.createStructField("frequencies", DataTypes.StringType,
        // true));
        // fields.add(DataTypes.createStructField("queryForGeoDetails",
        // DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("REPORT_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("FREQUENCIES", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("QUERY_FOR_GEO_DETAILS", DataTypes.StringType, true));
        return DataTypes.createStructType(fields);
    }

    /***************************
     * PM ReportUtils Code
     **********************************/

    public static String getValueFromJsonArray(JSONArray listJson) {
        String result = null;
        try {
            if (listJson != null) {
                for (Integer i = 0; i < listJson.length(); i++) {
                    String value = listJson.getString(i);
                    if (value != null && !value.isEmpty()) {// temp changes for geo if (value != null &&
                                                            // !value.isEmpty() && isNumeric(value)) {
                        if (result != null) {
                            result = result + Symbol.COMMA_STRING + value;
                        } else {
                            result = value;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in getting Value from Json Array :{} due to : {}", listJson, e.getMessage());
        }
        return result;
    }

    public static List<String> getListFromJsonArray(JSONArray array) {
        List<String> list = new ArrayList<>();
        if (array != null && array.length() > 0) {
            list = new ArrayList<>();
            for (Integer i = 0; i < array.length(); i++) {
                try {
                    list.add(array.getString(i));
                } catch (JSONException e) {
                    logger.error("Error in getting json array to string list {}", e.getMessage());
                }
            }
        }
        return list;
    }

    public static void main(String[] args) {

        String magnetFields = "{\"magnetFields\":[{\"columnName\":\"L1\",\"value\":[\"L1\"]},{\"columnName\":\"L2\",\"value\":[\"L2\"]},{\"columnName\":\"L3\",\"value\":[\"L3\"]}]}";
        JSONObject jsonObject = new JSONObject(magnetFields);
        logger.info("jsonObject: {}", jsonObject.toString());

        String result = getStringValueFromJsonArrayTest(jsonObject.getJSONArray("magnetFields"));
        logger.info("result: {}", result);
        getMapFromJsonArray(jsonObject.getJSONArray("magnetFields").toString());
        logger.info("map: {}");

    }

    public static String getStringValueFromJsonArrayTest(JSONArray listJson) {
        String result = null;
        try {
            if (listJson != null) {
                System.out.println("Input JSONArray: " + listJson.toString());
                for (int i = 0; i < listJson.length(); i++) {
                    JSONObject obj = listJson.getJSONObject(i); // Get object at index
                    JSONArray valueArray = obj.getJSONArray("value"); // Get 'value' array
                    String value = valueArray.getString(0); // Get first string inside 'value'

                    System.out.println("Processing columnName: " + obj.getString("columnName") + ", value: " + value);

                    if (result != null) {
                        result = result + Symbol.COMMA_STRING + value;
                    } else {
                        result = value;
                    }
                    System.out.println("Intermediate result: " + result);
                }
            } else {
                System.out.println("Input JSONArray is null.");
            }
        } catch (Exception e) {
            System.out.println("Error in getting Value from Json Array: " + listJson + " due to: " + e.getMessage());
            logger.error("Error in getting Value from Json Array :{} due to : {}", listJson, e.getMessage());
        }
        System.out.println("Final result: " + result);
        return result;
    }

    private static void getMapFromJsonArray(String magnetFields) {
        try {
            magnetFields = Arrays.asList(magnetFields.split(",")).toString().trim();
            logger.info("magnetFields: {}", magnetFields);
            Map<String, Object> map = new HashMap<>();
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> readValue = mapper.readValue(magnetFields,
                    new TypeReference<List<Map<String, Object>>>() {
                    });
            readValue.forEach(a -> {
                map.put((String) a.get("columnName"), a.get("value"));
            });
            logger.info("map : {}", map.toString());
        } catch (Exception e) {
            logger.error("Error in getting Value from Json Array :{} due to : {}", magnetFields, e.getMessage());
        }
    }

    public static String getStringValueFromJsonArray(JSONArray listJson) {
        String result = null;
        try {
            if (listJson != null) {
                for (Integer i = 0; i < listJson.length(); i++) {
                    String value = listJson.getString(i);
                    if (result != null) {
                        result = result + Symbol.COMMA_STRING + value;
                    } else {
                        result = value;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in getting Value from Json Array :{} due to : {}", listJson, e.getMessage());
        }
        return result;
    }

    public static String getNodeName(String nodeString) {
        String node = StringUtils
                .substringAfter(StringUtils.substringBeforeLast(nodeString, Symbol.HYPHEN_STRING), PMConstants.SPACE)
                .trim().toUpperCase();
        node = node.trim().replaceAll(PMConstants.SPACE, Symbol.HYPHEN_STRING);
        return node;
    }

    public static String getNodeVal(String nodeString) {
        String node = StringUtils.substringAfterLast(nodeString, Symbol.HYPHEN_STRING).trim().toUpperCase();
        node = node.trim().replaceAll(PMConstants.SPACE, Symbol.HYPHEN_STRING);
        return node;
    }

    public static String getSingleQuoteString(List<String> stringList) {
        String finalString = null;
        try {
            String stringValue = !stringList.isEmpty() ? String.join(PMConstants.COMMA, stringList) : null;
            if (stringValue != null) {
                finalString = "'" + stringValue.replaceAll(PMConstants.COMMA, "','") + "'";
            }
        } catch (Exception e) {
            logger.error("Exception in conver list to single quote String  :{}", ExceptionUtils.getStackTrace(e));
        }
        logger.info("Data center list : {}", finalString);
        return finalString;
    }

    public static boolean isNumeric(String str) {
        try {
            Integer.parseInt(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public static String getValueFromJson(JSONObject configJson, String jsonKey) throws JSONException {
        if (configJson.has(jsonKey) && !configJson.isNull(jsonKey)) {
            return configJson.getString(jsonKey);
        }
        return null;
    }

    public static String getValueFromJsonArray(JSONObject configJson, String jsonKey) throws JSONException {
        if (configJson.has(jsonKey) && !configJson.isNull(jsonKey)) {
            return configJson.getJSONArray(jsonKey).toString();
        }
        return null;
    }

    public List<String> getValueListFromJson(String systemConfig, String jsonKey) {
        List<String> valueList = new ArrayList<>();
        try {
            if (systemConfig != null && !systemConfig.isEmpty()) {
                Map<String, Object> jsonMap = new ObjectMapper().readValue(systemConfig, Map.class);
                String mapValue = jsonMap.get(jsonKey) != null ? (String) jsonMap.get(jsonKey) : null;
                if (mapValue != null) {
                    valueList.addAll(Arrays.asList(mapValue.split(PMConstants.COMMA)));
                }
            }
        } catch (Exception e) {
            logger.error("Error in getting  value list from json {} ", e.getMessage());
        }
        logger.error("Getting value list from json {}", valueList);
        return valueList;
    }

    public String getNeTypeByNode(String systemConfig, String domain, String vendor, String node) {
        String neType = null;
        String neTypeList = null;
        try {
            String nodeType = node.trim().replaceAll(PMConstants.SPACE, "_");
            String neTypeKey = ("REPORTS_NETYPE_" + domain + Symbol.UNDERSCORE + vendor + Symbol.UNDERSCORE + nodeType)
                    .toUpperCase();
            logger.error("neTypeKey for netypelist {}", neTypeKey);
            Map<String, Object> jsonMap = new ObjectMapper().readValue(systemConfig, Map.class);
            neType = jsonMap.get(neTypeKey) != null ? (String) jsonMap.get(neTypeKey) : null;
            if (neType != null) {
                neTypeList = "'" + neType.replaceAll(PMConstants.COMMA, "','") + "'";
            }
        } catch (IOException e) {
            logger.error("Exception in getting netypelist  :{}", ExceptionUtils.getStackTrace(e));
        }
        logger.info("RAN neTypeList : {}", neTypeList);
        return neTypeList;
    }

    /************************ END Code *******************************/

    class PMConstants {

        /******************* BASIC CONSTANTS *******************************/
        public static final String L1 = "L1";
        public static final String L2 = "L2";
        public static final String L3 = "L3";
        public static final String L4 = "L4";
        public static final String BBH = "BBH";
        public static final String NBH = "NBH";
        public static final String ENB = "ENB";
        public static final String CEL = "CEL";
        public static final String BND = "BND";
        public static final String DL_BBH = "BBH1";
        public static final String DL_NBH = "NBH1";
        public static final String CELL = "Cell";
        public static final String SMALL_CELL = "Small Cell";
        public static final String ENODEB = "eNodeB";
        public static final String VCU = "vCU";
        public static final String VDU = "vDU";
        public static final String IDSC = "IDSC";
        public static final String ODSC = "ODSC";
        public static final String FEMTO = "FEMTO";
        public static final String ODSC_VCU = "ODSC_VCU";
        public static final String IDSC_VCU = "IDSC_VCU";
        public static final String FEMTO_VCU = "FEMTO_VCU";
        public static final String RK = "RK";
        public static final String RK1 = "RK1";
        public static final String RAN_DOMAIN = "RAN";

        /******************* PM COLUMN FAMILYIES ****************************/
        // public static final byte[] PMCOLUMN_FAMILY = Bytes.toBytes("K");

        /******************* PM HBASE TABLE *********************************/
        public static final String PM_KPI_DAILY_TABLE = "CombineDailyPM";
        public static final String PM_KPI_HOURLY_TABLE = "CombineHourlyPM";
        public static final String PM_KPI_BBH_TABLE = "CombineBBHPM";
        public static final String PM_KPI_NBH_TABLE = "CombineNBHPM";
        public static final String PM_KPI_WEEKLY_TABLE = "CombineWeeklyPM";
        public static final String PM_KPI_MONTHLY_TABLE = "CombineMonthlyPM";
        public static final String PM_KPI_QUARTERLY_TABLE = "CombineQuarterlyPM";

        /************* DATE FORMAT *******************/
        public static final String YYYYMMDDHH = "yyyyMMddHH";
        public static final String YYYYMMDDHHMM = "yyyyMMddHHmm";
        public static final String YYMMDDHH = "yyMMddHH";
        public static final String YYYYMMDD = "yyyyMMdd";
        public static final String YYMMDD = "yyMMdd";
        public static final String YYYYMMDD_HHMMSS = "yyyy-MM-dd HH:mm:ss";
        public static final String _DDMMYYYY = "_ddMMyyyy";
        public static final String HHMMSS = "HHmmss";
        public static final String YYMMDDHHMM = "yyMMddHHmm";
        public static final String YYMM = "YYMM";
        public static final String DD_MM_YYYY = "dd/MM/yyyy";
        public static final String HH_MM = "HH:mm";
        public static final String YYYYWW = "yyyyww";
        public static final String YYYYMM = "yyyyMM";

        /************** PM Frequecy *********************************************/
        public static final String DURATION_DAILY = "DAILY";
        public static final String DURATION_HOURLY = "HOURLY";
        public static final String DURATION_WEEKLY = "WEEKLY";
        public static final String DURATION_MONTHLY = "MONTHLY";
        public static final String FREQUENCY_BBH = "BBH";
        public static final String FREQUENCY_NBH = "NBH";
        public static final String FREQUENCY_QUARTERLY = "15 Min";
        public static final String FREQUENCY_PERDAY = "PERDAY";
        public static final String FREQUENCY_PERWEEK = "PERWEEK";
        public static final String FREQUENCY_PERHOUR = "PERHOUR";
        public static final String FREQUENCY_PERMONTH = "PERMONTH";
        public static final String FREQUENCY_BUSIESTDAY = "BUSIESTDAY";

        /************************* Other *****************************************/
        public static final String All = "All";
        public static final String METACOLUMNS_JSONKEY = "metaColumns";
        public static final String ALL = "ALL";
        public static final String BLANK = "";
        public static final String SPACE = " ";
        public static final String HYPHEN = "-";
        public static final String COMMA = ",";
        public static final String KPI = "KPI";
        public static final String EXCEPTION_TYPE = "Exception";
        public static final String PERFORMANCE_REPORT = "Performance Report";
        public static final String LINK = "LINK";
        public static final String TIME = "TIME";
        public static final String DATE = "DATE";
        public static final String DL_VOLUME_UTILZATION_KEY = "DL VOLUME";
        public static final String FOURTY_EIGHT_HOUR = "FOURTYEIGHTHOUR";
        public static final String MAXTIME = "maxTime";
        public static final String MINTIME = "minTime";
        public static final String DATES = "dates";
        public static final String ALL_GEOGRAPHY_INDIVIDUAL = "All GEOGRAPHY - Individual";
        public static final String ALL_GEOGRAPHY_CLUBBED = "All GEOGRAPHY - Clubbed";
        public static final String GEOGRAPHY_LEVEL_REPLACE_REGEX = "L[1-4]";
        public static final String CUSTOM = "Custom";
        public static final String DYNAMIC_QUERY = "dynamicQuery";
        public static final String ENODEB_NODE = "eNodeB";
        public static final String FREQUENCEIS = "frequencies";
        public static final String GROUP_CENTER = "GC";
        public static final String ALL_CELL_INDIVIDUAL = "ALL CELL - INDIVIDUAL";
        public static final String ALL_SMALL_CELL_INDIVIDUAL = "ALL SMALL_CELL - INDIVIDUAL";
        public static final String ALL_IDSC_INDIVIDUAL = "ALL IDSC - INDIVIDUAL";
        public static final String ALL_ODSC_INDIVIDUAL = "ALL ODSC - INDIVIDUAL";
        public static final String ALL_FEMTO_INDIVIDUAL = "ALL FEMTO - INDIVIDUAL";
        public static final String ALL_GC_INDIVIDUAL = "All GC - Individual";
        public static final String ALL_ZONE_INDIVIDUAL = "All Zone - Individual";

        public static final String CLUBBED = "Clubbed";
        public static final String INDIVIDUAL = "Individual";
        public static final String CLUBBED_CAPS = "CLUBBED";
        public static final String INDIVIDUAL_CAPS = "INDIVIDUAL";

        public static final String GEOGRAPHYL0_LEVEL = "GeographyL0";
        public static final String GEOGRAPHYL1_LEVEL = "GeographyL1";
        public static final String GEOGRAPHYL2_LEVEL = "GeographyL2";
        public static final String GEOGRAPHYL3_LEVEL = "GeographyL3";
        public static final String GEOGRAPHYL4_LEVEL = "GeographyL4";
        public static final String GEOGRAPHY_LEVEL_ZONE = "ZONE";

        public static final String ALL_GEOGRAPHYL1_CLUBBED = "All GEOGRAPHYL1 - Clubbed";
        public static final String ALL_GEOGRAPHYL1_INDIVIDUAL = "All GEOGRAPHYL1 - Individual";
        public static final String ALL_GEOGRAPHYL2_CLUBBED = "All GEOGRAPHYL2 - Clubbed";
        public static final String ALL_GEOGRAPHYL2_INDIVIDUAL = "All GEOGRAPHYL2 - Individual";
        public static final String ALL_GEOGRAPHYL3_CLUBBED = "All GEOGRAPHYL3 - Clubbed";
        public static final String ALL_GEOGRAPHYL3_INDIVIDUAL = "All GEOGRAPHYL3 - Individual";
        public static final String ALL_GEOGRAPHYL4_CLUBBED = "All GEOGRAPHYL4 - Clubbed";
        public static final String ALL_GEOGRAPHYL4_INDIVIDUAL = "All GEOGRAPHYL4 - Individual";
        public static final String PARENT_NODE = "PARENT_NODE";
        public static final String BASE_NODE = "BASE_NODE";
        public static final String DATA_CENTER_LIST = "dataCenterList";

        /**************************** Index Constant ******************************/

        public static final int INDEX_EIGHT = 8;
        public static final int INDEX_FOUR = 4;
        public static final int INDEX_TEN = 10;
        public static final int INDEX_SIX = 6;
        public static final String FOUR_DECIMAL_FORMAT = "##.####";

        /****************** Report_Configuration JSON Key Constants ***************/

        public static final String NETYPE_JSONKEY = "netype";
        public static final String CELLS_JSONKEY = "cells";
        public static final String CUSTOM_TIME_SHIFT = ")).TimeShift(";
        public static final String NESTATUS_JSONKEY = "neStatus";
        public static final String PERHOURLIST_JSONKEY = "perHourList";
        public static final String GEOGRAPHY_L1_JSONKEY = "geography_l1";
        public static final String GEOGRAPHY_L2_JSONKEY = "geography_l2";
        public static final String GEOGRAPHY_L3_JSONKEY = "geography_l3";
        public static final String GEOGRAPHY_L4_JSONKEY = "geography_l4";
        public static final String DOMAIN_JSONKEY = "domain";
        public static final String UTILIZATION_KEY_JSONKEY = "utilization_key";
        public static final String BANDTYPE_JSONKEY = "bandType";
        public static final String NODE_JSONKEY = "node";
        public static final String DURATION_JSONKEY = "duration";
        public static final String FREQUENCY_JSONKEY = "frequency";
        public static final String FROMDATE_JSONKEY = "fromDate";
        public static final String TODATE_JSONKEY = "toDate";
        public static final String EXCEPTIONREPORT_JSONKEY = "exceptionReport";
        public static final String CHECK_CONSISTENCY_JSONKEY = "check_consistency";
        public static final String EMAIL_JSONKEY = "email";
        public static final String KPI_JSONKEY = "kpi";
        public static final String BLANK_STRING = "";
        public static final String TARGETAREA_JSONKEY = "targetArea";
        public static final String AGGREGATIONTYPE_JSONKEY = "aggregationType";
        public static final String VENDOR_JSONKEY = "vendor";
        public static final String TECHNOLOGY_JSONKEY = "technology";
        public static final String CUSTOMAGGREGATION_JSONKEY = "customAggregation";
        public static final String MONAME_JSONKEY = "moName";
        public static final String GROUP_CENTER_JSONKEY = "group_center";
        public static final String LEVEL_JSONKEY = "level";
        public static final String SITECATEGORY_JSONKEY = "siteCategory";
        public static final String REPORTNAME = "reportName";
        public static final String REPORTGEOGRAPHY = "geography";
        public static final String REPORTLEVEL = "reportLevel";
        public static final String REPORTTYPE = "reportType";
        public static final String METACOLUMN = "metaColumn";
        // public static final String BASE_STRING = "";
        // public static final String MAGNET_PARAM_CONFIG ="";
        public static final String BASE_STRING = "{\"TRANSPORT_WDM_INTERFACE\":\"WDM\",\"TRANSPORT_TOR_INTERFACE\":\"TOR\",\"TRANSPORT_TOR_TEMPERATUREUNIT\":\"TOR\",\"TRANSPORT_ACI_FABRIC_INTERFACE\":\"ACI_FABRIC\",\"TRANSPORT_ACI_FABRIC_TEMPERATUREUNIT\":\"ACI_FABRIC\",\"TRANSPORT_ACI_FABRIC_SYSTEMUNIT\":\"ACI_FABRIC\",\"PM_REPORT_BUILDER_GEO_TRANSPORT_CIENA\":\"L1_L2_L3\",\"PM_REPORT_BUILDER_GEO_TRANSPORT_CISCO\":\"L1_L2_L3\",\"PM_REPORT_BUILDER_GEO_TRANSPORT_NOKIA\":\"L1_L2_L3\",\"PM_REPORT_BUILDER_GEO_TRANSPORT_NETROUNDS\":\"L1_L2_L3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BUILDING_SWITCH\":\"BUILDING_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BUILDING_SWITCH_INTERFACE\":\"BUILDING_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BUILDING_SWITCH_SYSTEMUNIT\":\"BUILDING_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BUILDING_SWITCH_TEMPRATUREUNIT\":\"BUILDING_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BUILDING_SWITCH_MEMORYUNIT\":\"BUILDING_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IGW\":\"IGW\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IGW_INTERFACE\":\"IGW\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IGW_SYSTEMUNIT\":\"IGW\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IGW_TEMPRATUREUNIT\":\"IGW\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IGW_MEMORYUNIT\":\"IGW\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_SWITCH\":\"OOB_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_SWITCH_INTERFACE\":\"OOB_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_SWITCH_SYSTEMUNIT\":\"OOB_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_SWITCH_TEMPRATUREUNIT\":\"OOB_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_SWITCH_MEMORYUNIT\":\"OOB_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG1\":\"AG1\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG1_INTERFACE\":\"AG1\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG1_SYSTEMUNIT\":\"AG1\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG1_TEMPRATUREUNIT\":\"AG1\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG1_MEMORYUNIT\":\"AG1\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_ROUTER\":\"AGG_ROUTER\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_ROUTER_INTERFACE\":\"AGG_ROUTER\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_ROUTER_SYSTEMUNIT\":\"AGG_ROUTER\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_ROUTER_TEMPRATUREUNIT\":\"AGG_ROUTER\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_ROUTER_MEMORYUNIT\":\"AGG_ROUTER\",\"REPORTS_NETYPE_TRANSPORT_CISCO_VoLTE\":\"VoLTE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_VoLTE_INTERFACE\":\"VoLTE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_VoLTE_SYSTEMUNIT\":\"VoLTE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_VoLTE_TEMPRATUREUNIT\":\"VoLTE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_VoLTE_MEMORYUNIT\":\"VoLTE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG2\":\"AG2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG2_INTERFACE\":\"AG2\",\"REPORTS_NETYPE_TRANSPORT_NETROUNDS_NETROUNDS_TA\":\"NETROUNDS_TA\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG2_SYSTEMUNIT\":\"AG2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG2_TEMPRATUREUNIT\":\"AG2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG2_MEMORYUNIT\":\"AG2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IPBB\":\"IPBB\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IPBB_INTERFACE\":\"IPBB\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IPBB_SYSTEMUNIT\":\"IPBB\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IPBB_TEMPRATUREUNIT\":\"IPBB\",\"REPORTS_NETYPE_TRANSPORT_CISCO_IPBB_MEMORYUNIT\":\"IPBB\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L2\":\"OOB_L2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L2_INTERFACE\":\"OOB_L2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L2_SYSTEMUNIT\":\"OOB_L2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L2_TEMPRATUREUNIT\":\"OOB_L2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L2_MEMORYUNIT\":\"OOB_L2\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3\":\"OOB_L3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_INTERFACE\":\"OOB_L3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_SYSTEMUNIT\":\"OOB_L3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_TEMPRATUREUNIT\":\"OOB_L3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_MEMORYUNIT\":\"OOB_L3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG3\":\"AG3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG3_INTERFACE\":\"AG3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG3_SYSTEMUNIT\":\"AG3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG3_TEMPRATUREUNIT\":\"AG3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG3_MEMORYUNIT\":\"AG3\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SC_TOR\":\"SC_TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SC_TOR_INTERFACE\":\"SC_TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SC_TOR_SYSTEMUNIT\":\"SC_TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SC_TOR_TEMPRATUREUNIT\":\"SC_TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SC_TOR_MEMORYUNIT\":\"SC_TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG4\":\"AG4\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG4_INTERFACE\":\"AG4\",\"REPORTS_NETYPE_TRANSPORT_NOKIA_OLT_7360\":\"OLT_7360\",\"REPORTS_NETYPE_TRANSPORT_NOKIA_OLT_7360_PORT\":\"OLT_7360\",\"REPORTS_NETYPE_TRANSPORT_NOKIA_WDM\":\"WDM\",\"REPORTS_NETYPE_TRANSPORT_NOKIA_WDM_PORT\":\"WDM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG4_SYSTEMUNIT\":\"AG4\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG4_TEMPRATUREUNIT\":\"AG4\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AG4_MEMORYUNIT\":\"AG4\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ROAM\":\"ROAM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ROAM_INTERFACE\":\"ROAM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ROAM_SYSTEMUNIT\":\"ROAM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ROAM_TEMPRATUREUNIT\":\"ROAM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ROAM_MEMORYUNIT\":\"ROAM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CSR1KV\":\"CSR1kv\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CSR1KV_INTERFACE\":\"CSR1kv\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CSR1KV_SYSTEMUNIT\":\"CSR1kv\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CSR1KV_TEMPRATUREUNIT\":\"CSR1kv\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CSR1KV_MEMORYUNIT\":\"CSR1kv\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SATELLITERT\":\"SatelliteRT\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SATELLITERT_INTERFACE\":\"SatelliteRT\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SATELLITERT_SYSTEMUNIT\":\"SatelliteRT\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SATELLITERT_TEMPRATUREUNIT\":\"SatelliteRT\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SATELLITERT_MEMORYUNIT\":\"SatelliteRT\",\"REPORTS_NETYPE_TRANSPORT_CISCO_EDGE_TOR_SWITCH\":\"EDGE_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_EDGE_TOR_SWITCH_INTERFACE\":\"EDGE_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_EDGE_TOR_SWITCH_SYSTEMUNIT\":\"EDGE_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_EDGE_TOR_SWITCH_TEMPRATUREUNIT\":\"EDGE_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_EDGE_TOR_SWITCH_MEMORYUNIT\":\"EDGE_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_FH_TOR_SWITCH\":\"FH_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_FH_TOR_SWITCH_INTERFACE\":\"FH_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_FH_TOR_SWITCH_SYSTEMUNIT\":\"FH_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_FH_TOR_SWITCH_TEMPRATUREUNIT\":\"FH_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_FH_TOR_SWITCH_MEMORYUNIT\":\"FH_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_SWITCH\":\"AGG_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_SWITCH_INTERFACE\":\"AGG_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_SWITCH_SYSTEMUNIT\":\"AGG_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_SWITCH_TEMPRATUREUNIT\":\"AGG_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_AGG_SWITCH_MEMORYUNIT\":\"AGG_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_LEAF\":\"TOR_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_LEAF_INTERFACE\":\"TOR_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_LEAF_SYSTEMUNIT\":\"TOR_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_LEAF_TEMPRATUREUNIT\":\"TOR_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_LEAF_MEMORYUNIT\":\"TOR_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_APIC\":\"APIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_APIC_INTERFACE\":\"APIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_APIC_SYSTEMUNIT\":\"APIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_APIC_TEMPRATUREUNIT\":\"APIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_APIC_MEMORYUNIT\":\"APIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BORDER_LEAF\":\"BORDER_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BORDER_LEAF_INTERFACE\":\"BORDER_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BORDER_LEAF_SYSTEMUNIT\":\"BORDER_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BORDER_LEAF_TEMPRATUREUNIT\":\"BORDER_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_BORDER_LEAF_MEMORYUNIT\":\"BORDER_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SPINE\":\"SPINE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SPINE_INTERFACE\":\"SPINE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SPINE_SYSTEMUNIT\":\"SPINE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SPINE_TEMPRATUREUNIT\":\"SPINE\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SPINE_MEMORYUNIT\":\"SPINE\",\"REPORTS_NETYPE_TRANSPORT_CIENA_WDM\":\"WDM\",\"REPORTS_NETYPE_TRANSPORT_CIENA_WDM_INTERFACE\":\"WDM\",\"REPORTS_NETYPE_TRANSPORT_CIENA_WDM_SYSTEMUNIT\":\"WDM\",\"REPORTS_NETYPE_TRANSPORT_CIENA_WDM_TEMPERATUREUNIT\":\"WDM\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR\":\"TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_INTERFACE\":\"TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_SYSTEMUNIT\":\"TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_MEMORYUNIT\":\"TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TOR_TEMPERATUREUNIT\":\"TOR\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ACI_FABRIC\":\"ACI_FABRIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ACI_FABRIC_INTERFACE\":\"ACI_FABRIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ACI_FABRIC_SYSTEMUNIT\":\"ACI_FABRIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ACI_FABRIC_MEMORYUNIT\":\"ACI_FABRIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_ACI_FABRIC_TEMPERATUREUNIT\":\"ACI_FABRIC\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SN_TOR_SWITCH\":\"SN_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SN_TOR_SWITCH_INTERFACE\":\"SN_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SN_TOR_SWITCH_SYSTEMUNIT\":\"SN_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SN_TOR_SWITCH_TEMPRATUREUNIT\":\"SN_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_SN_TOR_SWITCH_MEMORYUNIT\":\"SN_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_NR_TOR_SWITCH\":\"NR_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_NR_TOR_SWITCH_INTERFACE\":\"NR_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_NR_TOR_SWITCH_SYSTEMUNIT\":\"NR_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_NR_TOR_SWITCH_TEMPRATUREUNIT\":\"NR_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_NR_TOR_SWITCH_MEMORYUNIT\":\"NR_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_NX\":\"OOB_L3_NX\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_NX_INTERFACE\":\"OOB_L3_NX\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_NX_SYSTEMUNIT\":\"OOB_L3_NX\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_NX_TEMPRATUREUNIT\":\"OOB_L3_NX\",\"REPORTS_NETYPE_TRANSPORT_CISCO_OOB_L3_NX_MEMORYUNIT\":\"OOB_L3_NX\",\"TRANSPORT_FLOOR_POE_SWITCH_INTERFACE\":\"FLOOR_POE_SWITCH\",\"PM_REPORT_BUILDER_GEO_TRANSPORT_DZS\":\"L1_L2_L3\",\"REPORTS_NETYPE_TRANSPORT_DZS_FLOOR_POE_SWITCH\":\"FLOOR_POE_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_DZS_STU\":\"STU\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TIER2_LEAF\":\"TIER2_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TIER2_LEAF_INTERFACE\":\"TIER2_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TIER2_LEAF_SYSTEMUNIT\":\"TIER2_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TIER2_LEAF_TEMPRATUREUNIT\":\"TIER2_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_TIER2_LEAF_MEMORYUNIT\":\"TIER2_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CM_TOR_SWITCH\":\"CM_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CM_TOR_SWITCH_INTERFACE\":\"CM_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CM_TOR_SWITCH_SYSTEMUNIT\":\"CM_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CM_TOR_SWITCH_TEMPRATUREUNIT\":\"CM_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CM_TOR_SWITCH_MEMORYUNIT\":\"CM_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_PO_TOR_SWITCH\":\"PO_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_PO_TOR_SWITCH_INTERFACE\":\"PO_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_PO_TOR_SWITCH_SYSTEMUNIT\":\"PO_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_PO_TOR_SWITCH_TEMPRATUREUNIT\":\"PO_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_PO_TOR_SWITCH_MEMORYUNIT\":\"PO_TOR_SWITCH\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CDC_BE_LEAF\":\"CDC_BE_LEAF\",\"REPORTS_NETYPE_TRANSPORT_CISCO_CDC_BE_SPINE\":\"CDC_BE_SPINE\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_DAS\":\"DAS_RRH\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_DAS_RIU\":\"DAS_RIU\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_RIUD\":\"RIUD_RRH\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_RIUD_VCU\":\"RIUD_VCU\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_RIUD_RIU\":\"RIUD_RIU\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_RIUD_DU\":\"RIUD_DU\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_RIUD_RIM\":\"RIUD_RIM\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_CELL\":\"MACRO_CELL\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_ENODEB\":\"MACRO,GALLERY_SITE,PICO_SITE,IBS_SITE,SHOOTER_SITE,ODSC_SITE,IDSC_SITE\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_VCU\":\"MACRO_ENB\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_VDU\":\"MACRO_VDU\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_GNB_CELL\":\"MACRO_SUB6_CELL,MACRO_MMW_CELL\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_GNB\":\"MACRO_MMW_CU_CP,MACRO_SUB6_CU_CP\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_GNB_CU_UP\":\"MACRO_SUB6_CU_UP,MACRO_MMW_CU_UP\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_GNB_DU\":\"MACRO_SUB6_VDU,MACRO_MMW_DU\",\"REPORTS_NETYPE_RAN_ALTIOSTAR_GNB_RU\":\"MACRO_SUB6_RU\",\"REPORTS_NETYPE_RAN_SERCOMM_VCU\":\"IDSC_VCU\",\"REPORTS_NETYPE_RAN_SERCOMM_FEMTO\":\"FEMTO_CELL\",\"REPORTS_NETYPE_RAN_SERCOMM_FEMTO_VCU\":\"FEMTO_VCU\",\"REPORTS_NETYPE_RAN_AIRSPAN_ODSC\":\"ODSC_CELL\",\"REPORTS_NETYPE_RAN_AIRSPAN_LTE_BS_CELL\":\"LTE_BS_CELL\",\"REPORTS_NETYPE_RAN_AIRSPAN_ODSC_VCU\":\"ODSC_VCU\",\"REPORTS_NETYPE_RAN_AIRSPAN_DRAN\":\"DRAN_CELL\",\"REPORTS_NETYPE_RAN_AIRSPAN_DRAN_VCU\":\"DRAN_VCU\",\"REPORTS_NETYPE_RAN_SERCOMM_IDSC\":\"IDSC_CELL\",\"REPORTS_NETYPE_CORE_NOKIA_CSD\":\"CSD\",\"REPORTS_NETYPE_CORE_F5_CGNAT\":\"CGNAT\",\"REPORTS_NETYPE_CORE_NOKIA_SPS\":\"SPS\",\"REPORTS_NETYPE_CORE_NOKIA_HSS\":\"HSS\",\"REPORTS_NETYPE_CORE_NOKIA_SDL\":\"SDL\",\"REPORTS_NETYPE_CORE_NOKIA_TAS\":\"TAS\",\"REPORTS_NETYPE_CORE_NOKIA_CSCF\":\"CSCF\",\"REPORTS_NETYPE_CORE_NOKIA_MRF\":\"MRF\",\"REPORTS_NETYPE_CORE_NOKIA_TITAN\":\"TITAN\",\"REPORTS_NETYPE_CORE_NOKIA_ASBC\":\"ASBC\",\"REPORTS_NETYPE_CORE_MAVENIR_SMSC\":\"SMSC\",\"REPORTS_NETYPE_CORE_NOKIA_CGF\":\"CGF\",\"REPORTS_NETYPE_CORE_NOKIA_Ribbon\":\"Ribbon\",\"REPORTS_NETYPE_CORE_NOKIA_NLS\":\"NLS\",\"REPORTS_NETYPE_CORE_NOKIA_FlowOne\":\"FlowOne\",\"REPORTS_NETYPE_CORE_NOKIA_NCM\":\"NCM\",\"REPORTS_NETYPE_CORE_CISCO_MME\":\"MME\",\"REPORTS_NETYPE_CORE_CISCO_SAEGW_C\":\"SAEGW_C\",\"REPORTS_NETYPE_CORE_MAVENIR_MSTORE\":\"MSTORE\",\"REPORTS_NETYPE_CORE_MAVENIR_PNS\":\"PNS\",\"REPORTS_NETYPE_CORE_MAVENIR_RCS_CORE\":\"RCS_CORE\",\"REPORTS_NETYPE_CORE_MAVENIR_RCS_MESSAGING\":\"RCS_MESSAGING\",\"REPORTS_NETYPE_CORE_MAVENIR_RCS_VOICE\":\"RCS_VOICE\",\"REPORTS_NETYPE_CORE_MAVENIR_SDC\":\"SDC\",\"REPORTS_NETYPE_CORE_MAVENIR_WRG\":\"WRG\",\"REPORTS_NETYPE_CORE_NOKIA_LI_THALES_SPYDER_MC\":\"LI_THALES_SPYDER_MC\",\"REPORTS_NETYPE_CORE_NOKIA_LI_THALES_ULIS_GW\":\"LI_THALES_ULIS_GW\",\"REPORTS_NETYPE_CORE_NOKIA_STPGW_MACH7\":\"STPGW_MACH7\",\"REPORTS_NETYPE_CORE_NOKIA_RIBBON_DSI\":\"RIBBON_DSI\",\"REPORTS_NETYPE_CORE_NOKIA_RIBBON_GSX\":\"RIBBON_GSX\",\"REPORTS_NETYPE_CORE_NOKIA_RIBBON_SBC\":\"RIBBON_SBC\",\"REPORTS_NETYPE_CORE_NOKIA_RIBBON_PSX\":\"RIBBON_PSX\",\"REPORTS_NETYPE_CORE_NOKIA_MSS\":\"MSS\",\"REPORTS_NETYPE_CORE_NOKIA_RIBBON_SGX\":\"RIBBON_SGX\",\"REPORTS_NETYPE_CORE_MAVENIR_WSG\":\"WSG\",\"REPORTS_NETYPE_CORE_NOKIA_AAA\":\"AAA\",\"REPORTS_NETYPE_CORE_NOKIA_MNP\":\"MNP\",\"REPORTS_NETYPE_CORE_NOKIA_EIR\":\"EIR\",\"REPORTS_NETYPE_CORE_NEC_RETWS\":\"RETWS\",\"REPORTS_NETYPE_CORE_MAVENIR_PRX\":\"PRX\",\"REPORTS_NETYPE_CORE_MAVENIR_DSN\":\"DSN\",\"REPORTS_NETYPE_CORE_MAVENIR_VMAS\":\"VMAS\",\"REPORTS_NETYPE_CORE_ALLOT_SGVE\":\"SGVE\",\"REPORTS_NETYPE_CORE_ALLOT_SG9700\":\"SG9700\",\"REPORTS_NETYPE_CORE_ALLOT_SMP\":\"SMP\",\"REPORTS_NETYPE_CORE_ALLOT_DSC\":\"DSC\",\"REPORTS_NETYPE_CORE_ALLOT_STC\":\"STC\",\"REPORTS_NETYPE_CORE_ALLOT_DM\":\"DM\",\"REPORTS_NETYPE_CORE_ALLOT_NSCM\":\"NSCM\",\"REPORTS_NETYPE_CORE_ALLOT_NSWS\":\"NSWS\",\"REPORTS_NETYPE_CORE_ALLOT_CS\":\"DWH\",\"REPORTS_NETYPE_CORE_ALLOT_NSF\":\"NSF\",\"REPORTS_NETYPE_CORE_ALLOT_NX\":\"NX\",\"REPORTS_NETYPE_CORE_NOKIA_IMPACT_IOT\":\"IMPACT_IOT\",\"REPORTS_NETYPE_CORE_RADCOM_RADCOM\":\"RADCOM\",\"REPORTS_NETYPE_CORE_MAVENIR_SIF\":\"SIF\",\"REPORTS_NETYPE_CORE_MAVENIR_DIF\":\"DIF\",\"REPORTS_NETYPE_CORE_MAVENIR_SIPFW\":\"SIPFW\",\"REPORTS_NETYPE_CORE_MAVENIR_XSS\":\"XSS\",\"REPORTS_NETYPE_CORE_MAVENIR_XADS\":\"XADS\",\"REPORTS_NETYPE_CORE_MAVENIR_XAFX\":\"XAFX\",\"REPORTS_NETYPE_CORE_MAVENIR_FMS\":\"FMS\",\"REPORTS_NETYPE_CORE_MAVENIR_MNSS_XMS\":\"MNSS_XMS\",\"REPORTS_NETYPE_CORE_CISCO_IR_GW\":\"IR_GW\",\"REPORTS_NETYPE_CORE_RADCOM_SERVICE_LINK\":\"SERVICE_LINK\",\"REPORTS_NETYPE_CORE_MAVENIR_XA\":\"XA\",\"REPORTS_NETYPE_CORE_NEC_RETWS_KVM_HOST\":\"RETWS_KVM_HOST\",\"REPORTS_NETYPE_CORE_NEC_RETWS_EBC\":\"RETWS_EBC\",\"REPORTS_NETYPE_CORE_NEC_RETWS_AEIDB\":\"RETWS_AEIDB\",\"REPORTS_NETYPE_CORE_NEC_RETWS_WS\":\"RETWS_WS\",\"REPORTS_NETYPE_CORE_NEC_RETWS_EMS\":\"RETWS_EMS\",\"BASE_NODE\":\"CELL,Small cell,SMALL_CELL,ODSC,IDSC,FEMTO,DAS,GNB_CELL,RIUD\",\"PARENT_NODE\":\"VCU,ODSC_VCU,IDSC_VCU,FEMTO_VCU,RIUD_VCU\",\"LimitForKpiUploadRB\":200,\"limitForNodeUploadRB\":200,\"limitForIndNodeUploadRB\":1500,\"numberOfDaysList\":365,\"OtherLevelName\":\"GC\",\"ZoneLevelName\":\"Zone\",\"EXECTYPE_TRINO\":\"trino\",\"FrequencyDisablebyNodeList\":[\"RIUD_VCU\",\"RIUD_RIU\",\"RIUD_DU\",\"RIUD_RIM\",\"RIUD\",\"LTE_BS_CELL\"],\"ZoneLevelValidNode\":[\"Cell\",\"FEMTO\",\"ODSC\",\"IDSC\",\"eNodeB\",\"vCU\",\"vDU\",\"DRAN\",\"DRAN_VCU\",\"RIUD\"],\"TransportCiscoTempNODE\":[\"TOR_LEAF\",\"APIC\",\"BORDER_LEAF\",\"SPINE\",\"TIER2_LEAF\"],\"StatusList\":[{\"name\":\"All\",\"value\":\"ALL\"},{\"name\":\"On Air\",\"value\":\"ONAIR\"},{\"name\":\"Planned\",\"value\":\"PLANNED\"}],\"hourlyOptionForCustomAndIndividual\":{\"hoursLimitForCustom\":7,\"hoursLimitForIndividual\":48,\"DaysLimitForIndividual\":2,\"daysLimitForNodeIndividual\":31},\"hourlyOptionForClubbed\":{\"enablePerHourForClubbed\":true},\"KPI_NODE_PAYLoad\":{\"GNB\":\"GNB,GNB_CELL,GNB_CU_UP,GNB_DU\",\"GNB_DU\":\"GNB_CELL,GNB_DU\",\"GNB_RU\":\"GNB_CELL\",\"RIUD_RRH\":\"RIUD\",\"RIUD_RIM\":\"RIUD\",\"RIUD_DU\":\"RIUD_DU,RIUD\",\"RIUD_RIU\":\"RIUD_DU,RIUD\",\"RIUD_VCU\":\"RIUD_VCU,RIUD_DU,RIUD\",\"DRAN\":\"DRAN\",\"DRAN_VCU\":\"DRAN,DRAN_VCU\",\"COMPUTE_NODE\":\"COMPUTE_NODE\",\"CONTROL_NODE\":\"CONTROL_NODE\",\"HDD_STORAGE_NODE\":\"HDD_STORAGE_NODE\",\"MGMT_NODE\":\"MGMT_NODE\",\"NON_ACC_COMPUTE\":\"NON_ACC_COMPUTE\",\"SSD_STORAGE_NODE\":\"SSD_STORAGE_NODE\",\"ENODEB\":\"CELL,ENODEB\",\"VCU\":\"CELL,ENODEB\",\"VDU\":\"CELL,ENODEB\",\"SLOT\":\"RADIO_ETHERNET\",\"DEGRADE\":\"RADIO_ETHERNET\",\"SERCOMM_IDSC\":\"IDSC\",\"SERCOMM_VCU\":\"IDSC\",\"SERCOMM_VDU\":\"IDSC\",\"SERCOMM_FEMTO\":\"FEMTO\",\"OLT_7360\":\"OLT_7360\",\"OLT_7360 PORT\":\"OLT_7360\",\"NETROUNDS_TA\":\"NR_BACKHAUL_TWAMP\",\"NETROUNDS_TA \":\"NR_BACKHAUL_TWAMP\",\"DAS\":\"CELL,ENODEB\",\"DAS_RIU\":\"CELL,ENODEB\",\"DAS_VCU\":\"CELL,ENODEB\",\"WDM\":\"WDM\",\"WDM TP\":\"WDM\",\"ODSC_VCU\":\"ODSC,ODSC_VCU\"},\"SAMPLE_TEMPLATES_LIST\":[\"ENodeB.csv\",\"Cell.csv\",\"\",\"vDU.csv\",\"vCU.csv\"],\"LEVEL_LIST\":[\"VNF\",\"VNFC\"],\"CORRELATION_LEVEL_LIST\":[\"ALL\",\"VNF\",\"VNFC\"],\"podLevelCondition\":[\"CORE_NOKIA\",\"CORE_F5\",\"CORE_CISCO\",\"SECURITY_F5\",\"PLATFORM_F5\"],\"podLevelList\":{\"DEFAULT\":[{\"name\":\"VNF\",\"value\":\"VNF\"},{\"name\":\"VNFC\",\"value\":\"VNFC\"}],\"CORE_NOKIA_RIBBON_GSX\":[{\"name\":\"BAREMETAL\",\"value\":\"BAREMETAL\"}],\"CORE_NOKIA_RIBBON_SGX\":[{\"name\":\"BAREMETAL\",\"value\":\"BAREMETAL\"}],\"CORE_NOKIA_RIBBON_DSI\":[{\"name\":\"BAREMETAL\",\"value\":\"BAREMETAL\"}]},\"SITE_CATEGORY_LIST\":[\"All\",\"Bronze\",\"Critical\",\"Platinum\",\"Gold\",\"Silver\",\"Regular\"],\"enableLevelForNodes\":[\"HSS\",\"SDL\",\"CSCF\",\"TAS\",\"TITAN\",\"MSS\",\"MNP\",\"EIR\"],\"enableSystemMmryUnitTransport\":[\"OOB_SWITCH\",\"AG1\",\"AG2\",\"IPBB\",\"OOB_L2\",\"OOB_L3\",\"AG3\",\"AG4\",\"ROAM\",\"CSR1kv\",\"SatelliteRT\",\"IGW\",\"BUILDING_SWITCH\",\"SC_TOR\",\"AGG_ROUTER\",\"VoLTE\"],\"hideShowConfig\":{\"enableSiteCategory\":true,\"enablePodLevel\":true,\"disableMOName\":true,\"enableGroupCenter\":true,\"enableSiteStatus\":true,\"enableKPIFormulaDialog\":true,\"enableCustomAggregation\":false,\"enableAggNodeForCustom\":true,\"enableAggNodeForCustom_On Demand\":true,\"enableAggNodeForCustom_Scheduled\":false,\"enableRRCConnectedUser\":false,\"enablePerHourForIndividual\":false,\"enableZoneMultiSelect\":false,\"enableCircleMultiSelect\":true,\"enableZoneMultiSelect_Exception Report\":false,\"enableCircleMultiSelect_Exception Report\":true},\"BBHNBHTRAFFICNAME\":{\"name\":\"Total Volume\",\"value\":\"TOTAL VOLUME\"},\"KPIGROUP\":{\"labelName\":\"KPI Group\",\"enableDropDownOfKPIGroup\":false},\"NODE_LIST\":{\"TRANSPORT_DZS\":{\"CLUBBED\":[{\"name\":\"All FLOOR_POE_SWITCH - Aggregated\",\"isDisabled\":false,\"id\":\"ALL FLOOR_POE_SWITCH - CLUBBED\",\"isSelected\":false},{\"name\":\"All STU - Aggregated\",\"isDisabled\":false,\"id\":\"ALL STU - CLUBBED\",\"isSelected\":false}],\"INDIVIDUAL\":[{\"name\":\"All FLOOR_POE_SWITCH - Individual\",\"isDisabled\":false,\"id\":\"ALL FLOOR_POE_SWITCH - INDIVIDUAL\",\"isSelected\":false},{\"name\":\"All STU - Individual\",\"isDisabled\":false,\"id\":\"ALL STU - INDIVIDUAL\",\"isSelected\":false}]},\"TRANSPORT_CIENA\":{\"CLUBBED\":[{\"name\":\"All WDM - Aggregated\",\"isDisabled\":false,\"id\":\"All WDM - CLUBBED\",\"isSelected\":false}],\"INDIVIDUAL\":[{\"name\":\"All WDM - Individual\",\"isDisabled\":false,\"id\":\"ALL WDM - INDIVIDUAL\",\"isSelected\":false},{\"name\":\"All WDM Interface - Individual\",\"isDisabled\":false,\"id\":\"ALL WDM INTERFACE - INDIVIDUAL\",\"isSelected\":false}]},\"TRANSPORT_SIKLU\":{\"CLUBBED\":[{\"name\":\"All WB_EB_DU - Aggregated\",\"isDisabled\":false,\"id\":\"All WB_EB_DU - CLUBBED\",\"isSelected\":false}],\"INDIVIDUAL\":[{\"name\":\"All WB_EB_DU - Individual\",\"isDisabled\":false,\"id\":\"ALL WB_EB_DU - INDIVIDUAL\",\"isSelected\":false}]},\"TRANSPORT_NETROUNDS\":{\"CLUBBED\":[{\"name\":\"All NETROUNDS_TA - Aggregated\",\"isDisabled\":false,\"id\":\"All NETROUNDS_TA - CLUBBED\",\"isSelected\":false},{\"name\":\"All NR_BACKHAUL_TWAMP - Aggregated\",\"isDisabled\":false,\"id\":\"All NR_BACKHAUL_TWAMP - CLUBBED\",\"isSelected\":false}],\"INDIVIDUAL\":[{\"name\":\"All NETROUNDS_TA - Individual\",\"isDisabled\":false,\"id\":\"ALL NETROUNDS_TA - INDIVIDUAL\",\"isSelected\":false},{\"name\":\"All NR_BACKHAUL_TWAMP - Individual\",\"isDisabled\":false,\"id\":\"ALL NR_BACKHAUL_TWAMP - INDIVIDUAL\",\"isSelected\":false}]},\"SECURITY_HPE\":{\"INDIVIDUAL\":[{\"name\":\"All KEYCLOAK - Individual\",\"isDisabled\":false,\"id\":\"ALL KEYCLOAK - INDIVIDUAL\",\"isSelected\":false}]}},\"FREQUENCY_LIST\":{\"RAN_ALL\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"RAN_AIRSPAN_LTE_BS_CELL\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_AIRSPAN_DRAN\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_AIRSPAN_DRAN_VCU\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_ALTIOSTAR_RIUD\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_ALTIOSTAR_RIUD_RIU\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_ALTIOSTAR\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_AIRSPAN\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_SERCOMM\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"RAN_QUCELL\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"WEEKLY_BBH\",\"visible\":true}],\"CORE_ALLOT\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"CORE_CISCO\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"CORE_MAVENIR\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"CORE_NEC\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"CORE_NOKIA\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"CORE_F5\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"CORE_RADCOM\":[{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"TRANSPORT_CIENA\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"TRANSPORT_CISCO\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true}],\"TRANSPORT_DZS\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"TRANSPORT_NOKIA\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"TRANSPORT_NETROUNDS\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"TRANSPORT_SIKLU\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"BSS_THALES\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"BSS_NETCRACKER\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"BSS_BootNext\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"BSS_NETCRACKER_CSR_POS\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"BSS_NETCRACKER_WEB\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"BSS_NETCRACKER_CSRD\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"BSS_NETCRACKER_BSOCT\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"BSS_NETCRACKER_OTHER\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"BSS_NETCRACKER_OM\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"APPLICATION_AXIGEN\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true}],\"TRANSPORT_CIENA_WDM\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"TRANSPORT_CIENA_WDM \":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"BUSIEST_DAY\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true},{\"name\":\"BBH\",\"visible\":true}],\"BSS_NETCRACKER_RBM_COLLECTIONS\":[{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"5_MIN\",\"visible\":true}],\"INFRA_QUANTA\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"SECURITY_HPE\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"SECURITY_CISCO\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"SECURITY_ALLOT\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"KATANA_BootNext\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"SECURITY_HASHICORP\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"OTF Report\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"Trend Report\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"Trend Report_TRANSPORT\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"LTE_BSS\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true}],\"PLATFORM_F5\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"PLATFORM_BootNext\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}],\"PLATFORM_CTC\":[{\"name\":\"PER_DAY\",\"visible\":true},{\"name\":\"PER_WEEK\",\"visible\":true},{\"name\":\"PER_MONTH\",\"visible\":true},{\"name\":\"15_MIN\",\"visible\":true},{\"name\":\"PER_HOUR\",\"visible\":true},{\"name\":\"NBH\",\"visible\":true}]},\"geographyL1List\":{\"On Demand_RAN\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"All Zone - Individual\",\"id\":\"All Zone - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_RAN\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\"},{\"name\":\"All Zone - Individual\",\"id\":\"All Zone - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_RAN_ALL\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"All Zone - Individual\",\"id\":\"All Zone - Individual\",\"isDisabled\":false}],\"Trend Report_Scheduled_RAN_ALL\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"All Zone - Individual\",\"id\":\"All Zone - Individual\",\"isDisabled\":false}],\"Exception Report_RAN\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_Scheduled_RAN\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"On Demand_CORE\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_CORE\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_CORE\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"On Demand_BSS\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_BSS\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false}],\"Exception Report_BSS\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false}],\"On Demand_INFRA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All GC - Individual\",\"id\":\"All GC - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_INFRA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All GC - Individual\",\"id\":\"All GC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false}],\"Exception Report_INFRA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All GC - Individual\",\"id\":\"All GC - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false}],\"On Demand_SECURITY\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_SECURITY\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_SECURITY\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All RDC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"On Demand_TRANSPORT_NOKIA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"CDC\",\"id\":\"CDC\",\"isDisabled\":false},{\"name\":\"DC\",\"id\":\"DC\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_TRANSPORT_NOKIA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"CDC\",\"id\":\"CDC\",\"isDisabled\":false},{\"name\":\"DC\",\"id\":\"DC\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_TRANSPORT_NOKIA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All GC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"TRANSPORT\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"TRANSPORT_CISCO\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"TRANSPORT_NOKIA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"TRANSPORT_SIKLU\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Region - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"GC\",\"id\":\"GC\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"On Demand_KATANA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_KATANA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_KATANA\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"On Demand_PLATFORM\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Scheduled_PLATFORM\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Clubbed\",\"isDisabled\":false},{\"name\":\"All Geography - Individual\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_PLATFORM\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}],\"Exception Report_TRANSPORT\":[{\"name\":\"PAN JAPAN\",\"id\":\"All Region - Individual\",\"isDisabled\":false},{\"name\":\"All CDC - Individual\",\"id\":\"All CDC - Individual\",\"isDisabled\":false},{\"name\":\"All DC - Individual\",\"id\":\"All DC - Individual\",\"isDisabled\":false},{\"name\":\"All GC - Individual\",\"id\":\"All RDC - Individual\",\"isDisabled\":false},{\"name\":\"Custom\",\"id\":\"Custom\",\"isDisabled\":false}]},\"coreGeographyList\":[\"CDC\",\"DC\",\"RDC\"],\"kpiPayloadDomainALL\":\"ALL\",\"dataCenterList\":\"CDC,RDC,DC,GC\",\"NetcrackerNodes5And15Min\":[\"CSR_POS\",\"WEB\",\"CSRD\",\"BSOCT\",\"Other\",\"OM\",\"RBM_Collections\"],\"architectureList\":{\"RAN_ALTIOSTAR_5G\":[\"ALL\",\"MMW\",\"SUB6\"]},\"LINK_TYPES\":\"NR_BACKHAUL_TWAMP\",\"reportTypeList\":[{\"name\":\"KPI Report\",\"value\":\"Performance Report\"},{\"name\":\"Exception Metrics Report\",\"value\":\"Exception Report\"},{\"name\":\"Custom Aggregation Report\",\"value\":\"Custom Aggregation Report\"},{\"name\":\"Trend Report\",\"value\":\"Trend Report\"},{\"name\":\"OTF Report\",\"value\":\"OTF Report\"}],\"defaultHeaderList\":{\"LTE_CORE\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}],\"LTE_RAN\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"RF Cluster\",\"value\":\"DL4\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"vCU\",\"value\":\"H1\"},{\"name\":\"vDU\",\"value\":\"H2\"},{\"name\":\"Zone\",\"value\":\"OG\"},{\"name\":\"Site\",\"value\":\"ENB\"},{\"name\":\"RIM\",\"value\":\"RIM\"},{\"name\":\"Cell\",\"value\":\"CEL\"},{\"name\":\"SARFID\",\"value\":\"SFID\"},{\"name\":\"Band\",\"value\":\"BND\"},{\"name\":\"Site Status\",\"value\":\"NS\"},{\"name\":\"Site Category\",\"value\":\"SC\"},{\"name\":\"ECGI\",\"value\":\"EGID\"},{\"name\":\"ENBID\",\"value\":\"EID\"}],\"ALL_RAN\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"RF Cluster\",\"value\":\"DL4\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"vCU\",\"value\":\"H1\"},{\"name\":\"vDU\",\"value\":\"H2\"},{\"name\":\"Zone\",\"value\":\"OG\"},{\"name\":\"Site\",\"value\":\"ENB\"},{\"name\":\"RIM\",\"value\":\"RIM\"},{\"name\":\"Cell\",\"value\":\"CEL\"},{\"name\":\"SARFID\",\"value\":\"SFID\"},{\"name\":\"Band\",\"value\":\"BND\"},{\"name\":\"Site Status\",\"value\":\"NS\"},{\"name\":\"Site Category\",\"value\":\"SC\"},{\"name\":\"ECGI\",\"value\":\"EGID\"},{\"name\":\"ENBID\",\"value\":\"EID\"}],\"LTE_TRANSPORT\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"NeType\",\"value\":\"NET\"},{\"name\":\"Node\",\"value\":\"ENB\"},{\"name\":\"MOType\",\"value\":\"MT\"},{\"name\":\"MOValue\",\"value\":\"MV\"}],\"LTE_TRANSPORT_OKI\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"JAPAN\",\"value\":\"DL1\"},{\"name\":\"Region\",\"value\":\"DL2\"},{\"name\":\"Prefecture\",\"value\":\"DL3\"},{\"name\":\"NeType\",\"value\":\"NET\"},{\"name\":\"Node\",\"value\":\"ENB\"},{\"name\":\"MOType\",\"value\":\"MT\"},{\"name\":\"MOValue\",\"value\":\"MV\"}],\"5G_RAN\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"RF Cluster\",\"value\":\"DL4\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"vCU\",\"value\":\"H1\"},{\"name\":\"vDU\",\"value\":\"H2\"},{\"name\":\"CU-UP\",\"value\":\"CUUP\"},{\"name\":\"Zone\",\"value\":\"OG\"},{\"name\":\"Site\",\"value\":\"ENB\"},{\"name\":\"Cell\",\"value\":\"CEL\"},{\"name\":\"NeType\",\"value\":\"NET\"},{\"name\":\"Band\",\"value\":\"BND\"},{\"name\":\"Site Status\",\"value\":\"NS\"},{\"name\":\"Site Category\",\"value\":\"SC\"},{\"name\":\"ECGI\",\"value\":\"EGID\"},{\"name\":\"ENBID\",\"value\":\"EID\"}],\"LTE_RAN_AIRSPAN\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"RF Cluster\",\"value\":\"DL4\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"vCU\",\"value\":\"H1\"},{\"name\":\"Zone\",\"value\":\"OG\"},{\"name\":\"Site\",\"value\":\"ENB\"},{\"name\":\"Cell\",\"value\":\"CEL\"},{\"name\":\"SARFID\",\"value\":\"SFID\"},{\"name\":\"NeType\",\"value\":\"NET\"},{\"name\":\"Band\",\"value\":\"BND\"},{\"name\":\"Site Status\",\"value\":\"NS\"},{\"name\":\"Site Category\",\"value\":\"SC\"},{\"name\":\"ECGI\",\"value\":\"EGID\"},{\"name\":\"ENBID\",\"value\":\"EID\"}],\"LTE_RAN_SERCOMM\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"RF Cluster\",\"value\":\"DL4\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"vCU\",\"value\":\"H1\"},{\"name\":\"Zone\",\"value\":\"OG\"},{\"name\":\"Site\",\"value\":\"ENB\"},{\"name\":\"Cell\",\"value\":\"CEL\"},{\"name\":\"SARFID\",\"value\":\"SFID\"},{\"name\":\"NeType\",\"value\":\"NET\"},{\"name\":\"Band\",\"value\":\"BND\"},{\"name\":\"Site Status\",\"value\":\"NS\"},{\"name\":\"Site Category\",\"value\":\"SC\"},{\"name\":\"ECGI\",\"value\":\"EGID\"},{\"name\":\"ENBID\",\"value\":\"EID\"}],\"LTE_INFRA\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}],\"LTE_TRANSPORT_NOKIA\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Region\",\"value\":\"DL1\"},{\"name\":\"Prefecture\",\"value\":\"DL2\"},{\"name\":\"City\",\"value\":\"DL3\"},{\"name\":\"Group Center\",\"value\":\"GC\"},{\"name\":\"NeType\",\"value\":\"NET\"},{\"name\":\"Node\",\"value\":\"ENB\"},{\"name\":\"MOType\",\"value\":\"MT\"},{\"name\":\"MOValue\",\"value\":\"MV\"}],\"LTE_BSS_NETCRACKER\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}],\"LTE_SECURITY\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}],\"LTE_KATANA\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}],\"LTE_PLATFORM\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}],\"LTE_BSS\":[{\"name\":\"Date\",\"value\":\"DATE\",\"checked\":false,\"disable\":false},{\"name\":\"Time\",\"value\":\"TIME\",\"checked\":false,\"disable\":false},{\"name\":\"Geography\",\"value\":\"DL1\"},{\"name\":\"Node\",\"value\":\"ENB\"}]},\"selectedHeaderList\":[],\"allVendorForDomain\":[\"RAN\"],\"coreBaremetalNodePodList\":[\"ALL RIBBON_DSI - INDIVIDUAL\",\"ALL RIBBON_GSX - INDIVIDUAL\",\"ALL RIBBON_SGX - INDIVIDUAL\"],\"scheduleDurationConfig\":{\"default\":[{\"name\":\"HOURLY\",\"visible\":true},{\"name\":\"DAILY\",\"visible\":true},{\"name\":\"WEEKLY\",\"visible\":true},{\"name\":\"MONTHLY\",\"visible\":true},{\"name\":\"REPORT_INSTANCE\",\"visible\":true},{\"name\":\"SCHEDULED_AT\",\"visible\":true}],\"Custom Aggregation Report\":[{\"name\":\"HOURLY\",\"visible\":true},{\"name\":\"DAILY\",\"visible\":true},{\"name\":\"WEEKLY\",\"visible\":true},{\"name\":\"MONTHLY\",\"visible\":true},{\"name\":\"REPORT_INSTANCE\",\"visible\":false},{\"name\":\"SCHEDULED_AT\",\"visible\":false}]},\"groupList\":[{\"name\":\"KPI\",\"value\":\"KPI\"},{\"name\":\"Counter\",\"value\":\"COUNTER\"}],\"nodeAggList\":[{\"name\":\"Min\",\"value\":\"Min\"},{\"name\":\"Max\",\"value\":\"Max\"},{\"name\":\"Sum\",\"value\":\"Sum\"}],\"timeAggList\":[{\"name\":\"Min\",\"value\":\"Min\"},{\"name\":\"Max\",\"value\":\"Max\"},{\"name\":\"Sum\",\"value\":\"Sum\"}]}";
        public static final String MAGNET_PARAM_CONFIG = "{\"RAN_2G\":{\"bcf\":\"M8\",\"cgi\":\"M70\",\"csr\":\"M1\",\"frl\":\"M43\",\"fru\":\"M42\",\"mcc\":\"M68\",\"mnc\":\"M69\",\"rac\":\"M52\",\"area\":\"M20\",\"cded\":\"M44\",\"cdef\":\"M45\",\"east\":\"M30\",\"jvid\":\"M34\",\"neid\":\"M16\",\"nesi\":\"M49\",\"sgsn\":\"M51\",\"bscid\":\"M2\",\"btsid\":\"M5\",\"cbcch\":\"M38\",\"egena\":\"M41\",\"emsip\":\"M72\",\"north\":\"M31\",\"notch\":\"M13\",\"bscgen\":\"M47\",\"cellid\":\"M11\",\"kitgen\":\"M19\",\"siteid\":\"M35\",\"mvendor\":\"M17\",\"bscinfo\":\"M7\",\"bscname\":\"M3\",\"csrname\":\"M9\",\"emsname\":\"M71\",\"manager\":\"M64\",\"mrbtsid\":\"M4\",\"nosdcch\":\"M14\",\"b3cities\":\"M59\",\"celltype\":\"M18\",\"earfcndl\":\"M73\",\"earfcnul\":\"M74\",\"sectorid\":\"M10\",\"sitetype\":\"M29\",\"trxcount\":\"M40\",\"antenaazi\":\"M27\",\"b3clutter\":\"M58\",\"celllacod\":\"M12\",\"cellowner\":\"M33\",\"optimiser\":\"M63\",\"segmentid\":\"M36\",\"antenaname\":\"M24\",\"antenatilt\":\"M26\",\"cellstatus\":\"M22\",\"feederloss\":\"M28\",\"segmentcfg\":\"M37\",\"tchdactive\":\"M50\",\"antenamodel\":\"M23\",\"attenuation\":\"M48\",\"beaconchunk\":\"M61\",\"bscobjectid\":\"M39\",\"polygonname\":\"M54\",\"quantumcode\":\"M21\",\"antenaheight\":\"M25\",\"b3unwindcity\":\"M57\",\"keycitynames\":\"M62\",\"postdistrict\":\"M60\",\"townsuburban\":\"M66\",\"b3performance\":\"M55\",\"frequencyband\":\"M67\",\"liveonnetwork\":\"M32\",\"mscneneidPlan\":\"M15\",\"b3pairedunwind\":\"M56\",\"multiratnodeid\":\"M6\",\"ericgsmcellname\":\"M53\",\"operationalstate\":\"M76\",\"umtscovergaeqsri\":\"M46\",\"administrativestate\":\"M75\",\"b3performanceexclunwind\":\"M65\"},\"RAN_3G\":{\"cgi\":\"M70\",\"csr\":\"M1\",\"mcc\":\"M68\",\"mnc\":\"M69\",\"msc\":\"M81\",\"rac\":\"M52\",\"sac\":\"M87\",\"area\":\"M20\",\"east\":\"M30\",\"jvid\":\"M34\",\"neid\":\"M16\",\"sgsn\":\"M51\",\"emsip\":\"M72\",\"north\":\"M31\",\"nume1\":\"M85\",\"rncid\":\"M77\",\"cellid\":\"M11\",\"kitgen\":\"M19\",\"siteid\":\"M35\",\"mvendor\":\"M17\",\"wbtsid\":\"M78\",\"wcelid\":\"M80\",\"carrier\":\"M82\",\"csrname\":\"M9\",\"emsname\":\"M71\",\"environ\":\"M83\",\"manager\":\"M64\",\"mrbtsid\":\"M4\",\"b3cities\":\"M59\",\"celltype\":\"M18\",\"sectorid\":\"M10\",\"sitetype\":\"M29\",\"uarfcndl\":\"M93\",\"uarfcnul\":\"M94\",\"antenaazi\":\"M27\",\"b3clutter\":\"M58\",\"celllacod\":\"M12\",\"cellowner\":\"M33\",\"nodebtype\":\"M88\",\"optimiser\":\"M63\",\"antenaname\":\"M24\",\"antenatilt\":\"M26\",\"cellstatus\":\"M22\",\"csrnodebid\":\"M79\",\"feederloss\":\"M28\",\"antenamodel\":\"M23\",\"beaconchunk\":\"M61\",\"polygonname\":\"M54\",\"quantumcode\":\"M21\",\"antenaheight\":\"M25\",\"b3unwindcity\":\"M57\",\"hsdpaenabled\":\"M84\",\"keycitynames\":\"M62\",\"postdistrict\":\"M60\",\"townsuburban\":\"M66\",\"b3performance\":\"M55\",\"frequencyband\":\"M67\",\"frequencytype\":\"M92\",\"liveonnetwork\":\"M32\",\"b3pairedunwind\":\"M56\",\"multiratnodeid\":\"M6\",\"scramblingcode\":\"M90\",\"operationalstate\":\"M76\",\"ericutrancellname\":\"M86\",\"frequencybandwidth\":\"M89\",\"administrativestate\":\"M75\",\"b3performanceexclunwind\":\"M65\"},\"RAN_5G\":{\"csr\":\"M1\",\"eci\":\"M101\",\"mcc\":\"M68\",\"mnc\":\"M69\",\"pci\":\"M105\",\"area\":\"M20\",\"east\":\"M30\",\"jvid\":\"M34\",\"ncgi\":\"M106\",\"neid\":\"M16\",\"emsip\":\"M72\",\"north\":\"M31\",\"cellid\":\"M11\",\"region\":\"M97\",\"siteid\":\"M35\",\"mvendor\":\"M17\",\"carrier\":\"M82\",\"celltac\":\"M96\",\"csrname\":\"M9\",\"emsname\":\"M71\",\"manager\":\"M64\",\"mrbtsid\":\"M4\",\"b3cities\":\"M59\",\"celltype\":\"M18\",\"gnodebid\":\"M102\",\"lteenbid\":\"M104\",\"narfcndl\":\"M108\",\"narfcnul\":\"M109\",\"sectorid\":\"M10\",\"sitetype\":\"M29\",\"antenaazi\":\"M27\",\"b3clutter\":\"M58\",\"cellowner\":\"M33\",\"gnodename\":\"M103\",\"optimiser\":\"M63\",\"antenaname\":\"M24\",\"antenatilt\":\"M26\",\"cellstatus\":\"M22\",\"csrnodebid\":\"M79\",\"feederloss\":\"M28\",\"antenamodel\":\"M23\",\"beaconchunk\":\"M61\",\"polygonname\":\"M54\",\"quantumcode\":\"M21\",\"antenaheight\":\"M25\",\"b3unwindcity\":\"M57\",\"keycitynames\":\"M62\",\"postdistrict\":\"M60\",\"townsuburban\":\"M66\",\"b3performance\":\"M55\",\"frequencyband\":\"M67\",\"frequencytype\":\"M92\",\"liveonnetwork\":\"M32\",\"b3pairedunwind\":\"M56\",\"multiratnodeid\":\"M6\",\"operationalstate\":\"M76\",\"frequencybandwidth\":\"M89\",\"administrativestate\":\"M75\",\"b3performanceexclunwind\":\"M65\"},\"RAN_LTE\":{\"csr\":\"M1\",\"eci\":\"M101\",\"mcc\":\"M68\",\"mnc\":\"M69\",\"pci\":\"M99\",\"area\":\"M20\",\"east\":\"M30\",\"ecgi\":\"M100\",\"jvid\":\"M34\",\"neid\":\"M16\",\"emsip\":\"M72\",\"north\":\"M31\",\"cellid\":\"M11\",\"region\":\"M97\",\"siteid\":\"M35\",\"mvendor\":\"M17\",\"carrier\":\"M82\",\"celltac\":\"M96\",\"csrname\":\"M9\",\"emsname\":\"M71\",\"manager\":\"M64\",\"mrbtsid\":\"M4\",\"b3cities\":\"M59\",\"celltype\":\"M18\",\"earfcndl\":\"M73\",\"earfcnul\":\"M74\",\"enodebid\":\"M95\",\"sectorid\":\"M10\",\"sitetype\":\"M29\",\"antenaazi\":\"M27\",\"b3clutter\":\"M58\",\"cellowner\":\"M33\",\"optimiser\":\"M63\",\"antenaname\":\"M24\",\"antenatilt\":\"M26\",\"cellstatus\":\"M22\",\"csrnodebid\":\"M79\",\"enodebname\":\"M98\",\"feederloss\":\"M28\",\"antenamodel\":\"M23\",\"beaconchunk\":\"M61\",\"polygonname\":\"M54\",\"quantumcode\":\"M21\",\"antenaheight\":\"M25\",\"b3unwindcity\":\"M57\",\"keycitynames\":\"M62\",\"postdistrict\":\"M60\",\"townsuburban\":\"M66\",\"b3performance\":\"M55\",\"frequencyband\":\"M67\",\"frequencytype\":\"M92\",\"liveonnetwork\":\"M32\",\"b3pairedunwind\":\"M56\",\"multiratnodeid\":\"M6\",\"operationalstate\":\"M76\",\"frequencybandwidth\":\"M89\",\"administrativestate\":\"M75\",\"b3performanceexclunwind\":\"M65\"},\"INFRA_COMMON\":{\"adapterkind\":\"M1\",\"cachedisk_resourceid\":\"M2\",\"cachedisk_resourcename\":\"M3\",\"capacitydisk_resourceid\":\"M4\",\"capacitydisk_resourcename\":\"M5\",\"clustercomputeresource_resourceid\":\"M6\",\"clustercomputeresource_resourcename\":\"M7\",\"code\":\"M8\",\"condition\":\"M9\",\"container_resourceid\":\"M10\",\"container_resourcename\":\"M11\",\"cpu\":\"M12\",\"daemonset\":\"M13\",\"datacenter_resourceid\":\"M14\",\"datacenter_resourcename\":\"M15\",\"datastore_resourceid\":\"M16\",\"datastore_resourcename\":\"M17\",\"deployment\":\"M18\",\"device\":\"M19\",\"distributedvirtualportgroup_resourceid\":\"M20\",\"distributedvirtualportgroup_resourcename\":\"M21\",\"dns_name\":\"M22\",\"edgecluster_resourceid\":\"M23\",\"edgecluster_resourcename\":\"M24\",\"edgeclustergroup_resourceid\":\"M25\",\"edgeclustergroup_resourcename\":\"M26\",\"firewallsection_resourceid\":\"M27\",\"firewallsection_resourcename\":\"M28\",\"firewallsectiongroup_resourceid\":\"M29\",\"firewallsectiongroup_resourcename\":\"M30\",\"group\":\"M31\",\"group_resourceid\":\"M32\",\"group_resourcename\":\"M33\",\"groups_resourceid\":\"M34\",\"groups_resourcename\":\"M35\",\"hostsystem_resourceid\":\"M36\",\"hostsystem_resourcename\":\"M37\",\"instance\":\"M38\",\"interface\":\"M39\",\"juju_application\":\"M40\",\"k8s_master_resourceid\":\"M41\",\"k8s_master_resourcename\":\"M42\",\"k8s_minion_resourceid\":\"M43\",\"k8s_minion_resourcename\":\"M44\",\"k8s_namespace_resourceid\":\"M45\",\"k8s_namespace_resourcename\":\"M46\",\"k8s_pod_resourceid\":\"M47\",\"k8s_pod_resourcename\":\"M48\",\"k8s_replicaset_resourceid\":\"M49\",\"k8s_replicaset_resourcename\":\"M50\",\"k8s_service_resourceid\":\"M51\",\"k8s_service_resourcename\":\"M52\",\"k8s_world_resourceid\":\"M53\",\"k8s_world_resourcename\":\"M54\",\"kubernetesadapter_instance_resourceid\":\"M55\",\"kubernetesadapter_instance_resourcename\":\"M56\",\"le\":\"M57\",\"loadbalancerpool_resourceid\":\"M58\",\"loadbalancerpool_resourcename\":\"M59\",\"loadbalancerservice_resourceid\":\"M60\",\"loadbalancerservice_resourcename\":\"M61\",\"loadbalancervirtualserver_resourceid\":\"M62\",\"loadbalancervirtualserver_resourcename\":\"M63\",\"loadbalancervirtualservergroup_resourceid\":\"M64\",\"loadbalancervirtualservergroup_resourcename\":\"M65\",\"logicalrouter_resourceid\":\"M66\",\"logicalrouter_resourcename\":\"M67\",\"logicalroutergroup_resourceid\":\"M68\",\"logicalroutergroup_resourcename\":\"M69\",\"logicalswitch_resourceid\":\"M70\",\"logicalswitch_resourcename\":\"M71\",\"logicalswitchgroup_resourceid\":\"M72\",\"logicalswitchgroup_resourcename\":\"M73\",\"managementappliances_resourceid\":\"M74\",\"managementappliances_resourcename\":\"M75\",\"managementcluster_resourceid\":\"M76\",\"managementcluster_resourcename\":\"M77\",\"managementnode_resourceid\":\"M78\",\"managementnode_resourcename\":\"M79\",\"managementservice_resourceid\":\"M80\",\"managementservice_resourcename\":\"M81\",\"name\":\"M82\",\"namespace\":\"M83\",\"node\":\"M84\",\"nsxtadapterinstance_resourceid\":\"M85\",\"nsxtadapterinstance_resourcename\":\"M86\",\"operation_type\":\"M87\",\"path\":\"M88\",\"resource\":\"M89\",\"resourcekind\":\"M90\",\"resourcepool_resourceid\":\"M91\",\"resourcepool_resourcename\":\"M92\",\"routerservice_resourceid\":\"M93\",\"routerservice_resourcename\":\"M94\",\"scope\":\"M95\",\"statefulset\":\"M96\",\"status\":\"M97\",\"subresource\":\"M98\",\"tier0logicalroutergroup_resourceid\":\"M99\",\"tier0logicalroutergroup_resourcename\":\"M100\",\"tier1logicalroutergroup_resourceid\":\"M101\",\"tier1logicalroutergroup_resourcename\":\"M102\",\"transportnode_resourceid\":\"M103\",\"transportnode_resourcename\":\"M104\",\"transportzone_resourceid\":\"M105\",\"transportzone_resourcename\":\"M106\",\"transportzonegroup_resourceid\":\"M107\",\"transportzonegroup_resourcename\":\"M108\",\"vc_ops_admin_ui_resourceid\":\"M109\",\"vc_ops_admin_ui_resourcename\":\"M110\",\"vc_ops_analytics_resourceid\":\"M111\",\"vc_ops_analytics_resourcename\":\"M112\",\"vc_ops_casa_resourceid\":\"M113\",\"vc_ops_casa_resourcename\":\"M114\",\"vc_ops_cluster_resourceid\":\"M115\",\"vc_ops_cluster_resourcename\":\"M116\",\"vc_ops_collector_resourceid\":\"M117\",\"vc_ops_collector_resourcename\":\"M118\",\"vc_ops_controller_resourceid\":\"M119\",\"vc_ops_controller_resourcename\":\"M120\",\"vc_ops_fsdb_resourceid\":\"M121\",\"vc_ops_fsdb_resourcename\":\"M122\",\"vc_ops_node_resourceid\":\"M123\",\"vc_ops_node_resourcename\":\"M124\",\"vc_ops_persistence_resourceid\":\"M125\",\"vc_ops_persistence_resourcename\":\"M126\",\"vc_ops_product_ui_resourceid\":\"M127\",\"vc_ops_product_ui_resourcename\":\"M128\",\"vc_ops_remote_collector_resourceid\":\"M129\",\"vc_ops_remote_collector_resourcename\":\"M130\",\"vc_ops_suite_api_resourceid\":\"M131\",\"vc_ops_suite_api_resourcename\":\"M132\",\"vc_ops_watchdog_resourceid\":\"M133\",\"vc_ops_watchdog_resourcename\":\"M134\",\"vcenter_operations_adapter_instance_resourceid\":\"M135\",\"vcenter_operations_adapter_instance_resourcename\":\"M136\",\"verb\":\"M137\",\"viewname\":\"M138\",\"virtualandphysicalsanadapter_instance_resourceid\":\"M139\",\"virtualandphysicalsanadapter_instance_resourcename\":\"M140\",\"virtualmachine_resourceid\":\"M141\",\"virtualmachine_resourcename\":\"M142\",\"virtualsandccluster_resourceid\":\"M143\",\"virtualsandccluster_resourcename\":\"M144\",\"virtualsandiskgroup_resourceid\":\"M145\",\"virtualsandiskgroup_resourcename\":\"M146\",\"virtualsanfaultdomain_resourceid\":\"M147\",\"virtualsanfaultdomain_resourcename\":\"M148\",\"vmfolder_resourceid\":\"M149\",\"vmfolder_resourcename\":\"M150\",\"vmwareadapter_instance_resourceid\":\"M151\",\"vmwareadapter_instance_resourcename\":\"M152\",\"vmwaredistributedvirtualswitch_resourceid\":\"M153\",\"vmwaredistributedvirtualswitch_resourcename\":\"M154\",\"vsan_world_resourceid\":\"M155\",\"vsan_world_resourcename\":\"M156\",\"vsphere_world_resourceid\":\"M157\",\"vsphere_world_resourcename\":\"M158\",\"hostfolder_resourceid\":\"M159\",\"hostfolder_resourcename\":\"M160\",\"job\":\"M161\",\"content_type\":\"M162\"},\"TRANSPORT_COMMON\":{\"link_ab\":\"M1\",\"transmission_site_ab\":\"M2\",\"reception_site_ab\":\"M3\",\"frequency_band_ab\":\"M4\",\"transmission_radio_ab\":\"M5\",\"branching_configuration_ab\":\"M6\",\"capacity_name_ab\":\"M7\",\"coordinated_power_dbm_ab\":\"M8\",\"received_signal_level_quality_dbm_ab\":\"M9\",\"modulation_ab\":\"M10\",\"direction_ab\":\"M11\",\"link_ba\":\"M12\",\"transmission_site_ba\":\"M13\",\"reception_site_ba\":\"M14\",\"frequency_band_ba\":\"M15\",\"transmission_radio_ba\":\"M16\",\"branching_configuration_ba\":\"M17\",\"capacity_name_ba\":\"M18\",\"coordinated_power_dbm_ba\":\"M19\",\"received_signal_level_quality_dbm_ba\":\"M20\",\"modulation_ba\":\"M21\",\"direction_ba\":\"M22\",\"swversion\":\"M23\",\"model\":\"M24\"}}";
    }
}
