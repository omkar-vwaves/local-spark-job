package com.enttribe.pm.job.hourly;

import static org.apache.commons.lang.StringUtils.replace;

import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import gnu.trove.map.hash.THashMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetCounterAndKPIDetailAtDriver_YB extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(GetCounterAndKPIDetailAtDriver_YB.class);

    private static final String COUNTER_AGGREGATION_MAPJSON = "COUNTER_AGGREGATION_MAPJSON";
    private static final String KPIFORMULA_MAPJSON = "KPIFORMULA_MAPJSON";
    private static final String ALL_KPI_CODE = "ALL_KPI_CODE";
    private static final String KPI_DATA_META_COLUMNS = "KPI_DATA_META_COLUMNS";
    private static final String POLYGON_NE_MAPJSON = "POLYGON_NE_MAPJSON";
    private static final String IS_FILE_FILTER_REQUIRED = "IS_FILE_FILTER_REQUIRED";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static Map<String, Map<String, Object>> kpiFormulaFinalMap = null;
    private static Pattern kpiPattern = Pattern.compile("KPI#(G?[0-9]{0,5})");
    private static String HASHHASH_STRING = "##";
    private static final String ENBID_MAPJSON = "ENBID_MAPJSON";

    public GetCounterAndKPIDetailAtDriver_YB() {
        super();
    }

    public GetCounterAndKPIDetailAtDriver_YB(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        Long startTime = System.currentTimeMillis();

        logger.info("🚀 GetCounterAndKPIDetailAtDriver_YB Execution Started! ✅");
        Map<String, String> contextMap = new HashMap<>();
        contextMap = jobContext.getParameters();

        logger.info("Received Context Map: {}", contextMap);

        Map<String, String> counterAggrMap = getCounterAggrMap(jobContext, contextMap);
        String kpiDataValue = getAllKpiCode(jobContext, contextMap);

        getKPIFormulaMap(jobContext, contextMap);
        String metaDataValue = getMetaColumn(jobContext, contextMap);
        getPolygonNEDetail(jobContext, contextMap);

        getPMCounterVariableAggregationQuery(jobContext, counterAggrMap);
        getPMCounterVariableSelectQuery(jobContext, counterAggrMap);

        getPMCounterVariableMapQuery(jobContext, counterAggrMap);
        getCounterVariableIndex(jobContext, counterAggrMap, contextMap);
        setValueNetypeRowKeyAppenderMap(jobContext, contextMap);
        getEnbidMap(jobContext, contextMap);
        String kpis = kpiDataValue + "," + metaDataValue;
        jobContext.setParameters("ALL_KPI", kpis);

        jobContext.setParameters("SEQUENCE_TO_COUNTER_QUERY",
                getCategorySequenceForCounterVariable(jobContext, contextMap));

        Long endTime = System.currentTimeMillis();
        logger.info("GetCounterAndKPIDetailAtDriver_YB Execution Completed! Time Taken: {} Seconds", (endTime - startTime) / 1000);
        return this.dataFrame;
    }

    private String getCategorySequenceForCounterVariable(JobContext jobContext, Map<String, String> contextMap) {

        StringBuilder counterVariableQuery = new StringBuilder("SELECT ");
        StringBuilder counterVariableQueryType = new StringBuilder("");
        StringBuilder orcPaths = new StringBuilder("");
        try {
            String vendor = contextMap.get("VENDOR").trim();
            String domain = contextMap.get("DOMAIN").trim();
            String technology = contextMap.get("TECHNOLOGY").trim();
            String nodes = contextMap.get("NODES");

            logger.info("Getting Category Sequence For Counter Variable With VENDOR={}, DOMAIN={}, TECHNOLOGY={}, NODES={}", vendor, domain, technology, nodes);

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT ")
                    .append("pcv.PM_COUNTER_VARIABLE_ID_PK, ")
                    .append("kc.SEQUENCE_NO, ")
                    .append("pc.CATEGORY_NAME ")
                    .append("FROM PM_COUNTER_VARIABLE pcv ")
                    .append("JOIN KPI_COUNTER kc ON pcv.KPI_COUNTER_ID_FK = kc.KPI_COUNTER_ID_PK ")
                    .append("JOIN PM_CATEGORY pc ON pcv.PM_CATEGORY_ID_FK = pc.PM_CATEGORY_ID_PK ")
                    .append("JOIN PM_NODE_VENDOR pnv ON pc.PM_NODE_VENDOR_ID_FK = pnv.PM_NODE_VENDOR_ID_PK ")
                    .append("WHERE pnv.DOMAIN = '").append(domain).append("' ")
                    .append("AND pnv.VENDOR = '").append(vendor).append("' ")
                    .append("AND pnv.TECHNOLOGY = '").append(technology).append("' ")
                    .append("AND pcv.NODE_AGGREGATION != '' ")
                    .append("AND pcv.NODE_AGGREGATION IS NOT NULL ")
                    .append("AND pcv.TIME_AGGREGATION != '' ")
                    .append("AND pcv.TIME_AGGREGATION IS NOT NULL ");

            if (nodes != null && !nodes.trim().isEmpty()) {
                String formattedNodes = Arrays.stream(nodes.split(","))
                        .map(String::trim)
                        .filter(node -> !node.isEmpty())
                        .collect(Collectors.joining("','", "'", "'"));
                sqlBuilder.append("AND pnv.NODE IN (").append(formattedNodes).append(") ");
            }

            String sqlQuery = sqlBuilder.toString();
            Set<String> categorySet = new HashSet<>();
            logger.info("Executing CategorySequenceForCounterVariable Query: {}", sqlQuery);

            try (ResultSet resultSet = createConnection(sqlQuery, contextMap)) {

                List<String> selectExpressions = new ArrayList<>();
                Set<String> dataTypeExpressions = new HashSet<>();

                selectExpressions.add("finalKey AS rowKey");
                selectExpressions.add("ptime AS PT");

                dataTypeExpressions.add("finalKey:STRING");
                dataTypeExpressions.add("ptime:STRING");

                while (resultSet != null && resultSet.next()) {

                    String variableId = resultSet.getString("PM_COUNTER_VARIABLE_ID_PK");
                    String sequenceNo = resultSet.getString("SEQUENCE_NO") != null ? resultSet.getString("SEQUENCE_NO")
                            : "null";
                    String categoryName = resultSet.getString("CATEGORY_NAME") != null
                            ? resultSet.getString("CATEGORY_NAME")
                            : "null";

                    if (variableId != null && sequenceNo != null && categoryName != null) {
                        String expression = String.format(
                                "CASE WHEN categoryname = '%s' THEN CAST(C%s AS STRING) END AS `%s`",
                                categoryName.replace("'", "''").toUpperCase(),
                                sequenceNo,
                                variableId);
                        selectExpressions.add(expression);
                        dataTypeExpressions.add("C" + sequenceNo + ":STRING");
                        String orcFilePath = contextMap.get("KPI_ORC_PATH");
                        if (orcFilePath != null && !orcFilePath.isEmpty()) {
                            orcFilePath = orcFilePath.replace("$categoryname/", categoryName.toUpperCase() + "/");
                            categorySet.add(orcFilePath);
                        } else {
                            throw new Exception("KPI_ORC_PATH Is NULL Or Empty! Please Check The Job Context!");
                        }
                    }
                }

                if (!selectExpressions.isEmpty() && selectExpressions.size() > 2) {
                    counterVariableQuery.append(String.join(", ", selectExpressions));
                    counterVariableQuery.append(" FROM orcTemp");
                    counterVariableQueryType.append(String.join(",", dataTypeExpressions));

                } else {
                    counterVariableQuery = new StringBuilder("SELECT 1 AS dummy FROM orcTemp");
                }
            }
            orcPaths.append(String.join(",", categorySet));

            String orcBasePath = StringUtils.substringBefore(contextMap.get("KPI_ORC_PATH"), "domain=");

            logger.info("ORC Base Path: {}", orcBasePath);
            logger.info("ORC Paths: {}", orcPaths.toString());
            logger.info("ORC Counters Datatype: {}", counterVariableQueryType.toString());
           

            jobContext.setParameters("ORC_COUNTERS_DATATYPE", counterVariableQueryType.toString());
            jobContext.setParameters("ORC_PATH", orcPaths.toString());
            jobContext.setParameters("ORC_BASE_PATH", orcBasePath);

        } catch (Exception e) {
            logger.error("Exception While Building Category Sequence For Counter Variable Query: {}", e.getMessage(),
                    e);
        }

        logger.info("Category Sequence For Counter Variable Query: {}", counterVariableQuery.toString());
        return counterVariableQuery.toString();
    }

    private void getEnbidMap(JobContext jobcontext, Map<String, String> contextMap) {
        logger.info("Getting ENB ID Map...");
        try {
            String isFileFilterRequired = contextMap.get(IS_FILE_FILTER_REQUIRED);
            String domain = contextMap.get("DOMAIN");
            String vendor = contextMap.get("VENDOR");
            String neType = contextMap.get("NE_TYPE");

            logger.info("Getting ENB ID Map With DOMAIN={}, VENDOR={}, NE_TYPE={}", domain, vendor, neType);

            if (isFileFilterRequired != null && !isFileFilterRequired.isEmpty()) {
                String sql = "SELECT n.ENB_ID, n.PM_EMS_ID FROM NETWORK_ELEMENT n WHERE n.DOMAIN = '"
                        + domain + "' AND n.VENDOR = '" + vendor
                        + "' AND n.DELETED = 0 AND n.PM_EMS_ID IS NOT NULL AND n.ENB_ID IS NOT NULL AND n.NE_TYPE IN ("
                        + neType + ") GROUP BY n.ENB_ID, n.PM_EMS_ID";
                logger.info("ENB ID Filter SQL: {}", sql);
                ResultSet rs = createConnection(sql, contextMap);
                Map<String, String> enbidMap = new HashMap<>();
                while (rs.next()) {
                    enbidMap.put(rs.getString(1), rs.getString(2));
                }
                String json = mapper.writeValueAsString(enbidMap);
                jobcontext.setParameters(ENBID_MAPJSON, json);
                logger.info("ENB ID Map Size: {}", enbidMap.size());
            }
        } catch (Exception e) {
            logger.error("Exception While Getting ENB ID Map in @getEnbidMap, Message: {}, Error: {}", e.getMessage(), e);
        }
    }

    private Map<String, String> getCounterAggrMap(JobContext jobcontext, Map<String, String> contextMap) {
        Map<String, String> counterMap = new THashMap<>();
        try {

            String domain = contextMap.get("DOMAIN");
            String vendor = contextMap.get("VENDOR");
            String technology = contextMap.get("TECHNOLOGY");
            String nodes = contextMap.get("NODES");

            logger.info("Getting Counter Aggr Map With DOMAIN={}, VENDOR={}, TECHNOLOGY={}, NODES={}", domain, vendor,
                    technology, nodes);

            String sqlQuery = "SELECT pcv.PM_COUNTER_VARIABLE_ID_PK, CONCAT(CONCAT(pcv.NODE_AGGREGATION,'#'), pcv.TIME_AGGREGATION) AS AGGREGATION_DETAILS FROM "
                    + "PM_COUNTER_VARIABLE pcv, PM_CATEGORY pc, PM_NODE_VENDOR pnv WHERE pcv.PM_CATEGORY_ID_FK=pc.PM_CATEGORY_ID_PK "
                    + "AND pc.PM_NODE_VENDOR_ID_FK = pnv.PM_NODE_VENDOR_ID_PK AND pnv.VENDOR  = '" + vendor + "' "
                    + " AND pnv.DOMAIN  = '" + domain + "' "
                    + "AND pcv.NODE_AGGREGATION != '' AND pcv.NODE_AGGREGATION IS NOT NULL AND pcv.TIME_AGGREGATION != '' AND pcv.TIME_AGGREGATION IS NOT NULL ";
            if (nodes != null) {
                if (!nodes.contains(Symbol.APOSTROPHE_STRING)) {
                    nodes = "'" + Arrays.asList(nodes.split(Symbol.COMMA_STRING)).stream()
                            .collect(Collectors.joining("','")) + "'";
                }
                sqlQuery = sqlQuery + " AND pnv.NODE IN (" + nodes + ") ";
            }

            if (StringUtils.isNotEmpty(technology)) {
                sqlQuery = sqlQuery + " AND pnv.TECHNOLOGY ='" + technology + "' ";
            }
            logger.info("Counter Aggregation Map Query :{}", sqlQuery);

            ResultSet rs = createConnection(sqlQuery, contextMap);
            if (rs != null) {
                while (rs.next()) {
                    counterMap.put(String.valueOf(rs.getInt(1)), rs.getString(2));
                }
            }

            logger.info("Counter Aggregation Map Size: {}", counterMap.size());
            String counterMapJson = mapper.writeValueAsString(counterMap);
            jobcontext.setParameters(COUNTER_AGGREGATION_MAPJSON, counterMapJson);
        } catch (Exception e) {
            logger.error("Exception While Getting Counter Map in @getCounterAggrMap, Message: {}", e.getMessage(), e);
        }
        return counterMap;
    }

    private void getPMCounterVariableMapQuery(JobContext jobcontext, Map<String, String> counterAggrMap)
            throws Exception {
        
        logger.info("Getting PM Counter Variable Map Query...");
        String counterQueryMap = null;
        try {
            StringBuilder mapQuery = new StringBuilder();
            for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
                if (entry.getKey() != null)
                    mapQuery = mapQuery.append("'" + entry.getKey() + "', `" + entry.getKey() + "`, ");
            }
            counterQueryMap = StringUtils.substringBeforeLast(mapQuery.toString(), ",");
            counterQueryMap = "Map(" + counterQueryMap + ") AS rawcounters";
        } catch (Exception e) {
            logger.error("Exception While Getting PM Counter Variable Map Query in @getPMCounterVariableMapQuery, Message: {}, Error: {}", e.getMessage(), e);
        }

        logger.info("Counter Map Query: {}", counterQueryMap);
        jobcontext.setParameters("COUNTER_MAP_QUERY", counterQueryMap);
    }

    private void getPMCounterVariableSelectQuery(JobContext jobcontext, Map<String, String> counterAggrMap)
            throws Exception {
        logger.info("Getting PM Counter Variable Select Query...");
        String mapSelect = null;
        String counterSelect = null;
        try {
            StringBuilder mapQuery = new StringBuilder();
            StringBuilder counterQuery = new StringBuilder();
            for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
                if (entry.getKey() != null) {
                    if (entry.getValue().split("#")[0].equalsIgnoreCase("AVG")
                            || entry.getValue().split("#")[0].equalsIgnoreCase("EXCLUDE_ZERO_AVG")) {
                        mapQuery = mapQuery
                                .append("`" + entry.getKey() + "`, S" + entry.getKey() + ", C" + entry.getKey() + ", ");
                        counterQuery = counterQuery.append("`" + entry.getKey() + "`, ");
                    } else {
                        mapQuery = mapQuery.append("`" + entry.getKey() + "`, ");
                        counterQuery = counterQuery.append("`" + entry.getKey() + "`, ");
                    }
                }
            }
            counterSelect = StringUtils.substringBeforeLast(counterQuery.toString(), ",");
            mapSelect = StringUtils.substringBeforeLast(mapQuery.toString(), ",");
        } catch (Exception e) {
            logger.error(
                    "Exception While Getting PM Counter Variable Select Query in @getPMCounterVariableSelectQuery, Message: {}, Error: {}", e.getMessage(), e);
        }
        logger.info("Counter Select Query: {}", counterSelect);
        logger.info("Map Select Query: {}", mapSelect);
        jobcontext.setParameters("COUNTER_DATA_SELECT_QUERY", counterSelect);
        jobcontext.setParameters("COUNTER_SELECT_QUERY", mapSelect);
    }

    private void getCounterVariableIndex(JobContext jobcontext, Map<String, String> counterAggrMap,
            Map<String, String> contextMap) throws Exception {

        logger.info("Getting Counter Variable Index Map...");
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        LinkedHashMap<String, String> map1 = new LinkedHashMap<>();
        try {
            int i = 2;
            for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
                if (entry.getKey() != null) {
                    map.put(entry.getKey(), String.valueOf(i));
                    map1.put(String.valueOf(i), "C" + entry.getKey());
                    i++;
                }
            }
        } catch (Exception e) {
            logger.error("Exception While Getting Counter Variable Index in @getCounterVariableIndex, Message: {}, Error: {}", e.getMessage(), e);
        }
        String json = mapper.writeValueAsString(map);
        logger.info("Counter Variable Index Map Size: {}", map.size());
        jobcontext.setParameters("COUNTER_VARIABLE_INDEX", json);
        json = mapper.writeValueAsString(map1);

        logger.info("Index vs Counter Variable Map Size: {}", map1.size());
        jobcontext.setParameters("INDEX_VS_COUNTER_VARIABLE", json);
    }

    private void getKPIFormulaMap(JobContext jobcontext, Map<String, String> contextMap) throws Exception {
        Map<String, Map<String, String>> tempKpiFormulaFinalMap = new THashMap<>();
        try {
            String domain = contextMap.get("DOMAIN");
            String vendor = contextMap.get("VENDOR");
            String technology = contextMap.get("TECHNOLOGY");

            String sql = "SELECT CONCAT(kf.KPI_CODE, "
                    + "CASE WHEN gk.CODE IS NOT NULL THEN CONCAT('##',COALESCE(gk.CODE,'null')) ELSE '' END, "
                    + "'##', kf.KPI_FORMULA_DESC) AS FORMULA, "
                    + "COALESCE(CAST(pmc.PM_COUNTER_VARIABLE_ID_PK AS CHAR),'null') AS PM_COUNTER_VARIABLE_ID_PK, "
                    + "pmc.UNIQUE_STRING "
                    + "FROM KPI_FORMULA kf "
                    + "LEFT JOIN FORMULA_COUNTER_MAPPING fcm ON fcm.KPI_FORMULA_ID_FK = kf.KPI_FORMULA_ID_PK "
                    + "LEFT JOIN PM_COUNTER_VARIABLE pmc ON fcm.PM_COUNTER_VARIABLE_ID_FK = pmc.PM_COUNTER_VARIABLE_ID_PK "
                    + "LEFT JOIN GENERIC_KPI_MAPPING gkm ON kf.KPI_FORMULA_ID_PK = gkm.KPI_FORMULA_ID_FK "
                    + "LEFT JOIN PM_GENERIC_KPI gk ON gkm.PM_GENERIC_KPI_ID_FK = gk.PM_GENERIC_KPI_ID_PK "
                    + "WHERE kf.DOMAIN = '" + domain + "' AND kf.VENDOR = '" + vendor + "' "
                    + "AND kf.DELETED = 0 "
                    + "AND kf.KPI_TYPE = 'REGULAR' "
                    + "AND kf.KPI_FORMULA_DESC NOT LIKE '%timeshift%'";

            if (!StringUtils.isEmpty(technology)) {
                sql = sql + " AND kf.TECHNOLOGY = '" + technology + "' ";
            }
            sql = sql
                    + " UNION "
                    + "SELECT CONCAT(gkpi.CODE,'##',gkpi.KPI_FORMULA_DESC) AS FORMULA, "
                    + "'null' AS PM_COUNTER_VARIABLE_ID_PK, "
                    + "null AS UNIQUE_STRING "
                    + "FROM PM_GENERIC_KPI gkpi "
                    + "LEFT JOIN GENERIC_KPI_MAPPING gkm ON gkpi.PM_GENERIC_KPI_ID_PK = gkm.PM_GENERIC_KPI_ID_FK "
                    + "WHERE gkm.PM_GENERIC_KPI_ID_FK IS NULL "
                    + "AND gkpi.CODE IS NOT NULL "
                    + "AND gkpi.KPI_FORMULA_DESC IS NOT NULL "
                    + "AND gkpi.KPI_FORMULA_DESC NOT LIKE '%timeshift%' "
                    + "AND gkpi.KPI_TYPE = 'REGULAR' "
                    + "AND gkpi.DOMAIN = '" + domain + "' "
                    + "AND gkpi.TECHNOLOGY = '" + technology + "' "
                    + "AND gkpi.DELETED = 0 ";

            logger.info("KPI Formula Map Query: {}", sql);

            ResultSet rs = createConnection(sql, contextMap);
            while (rs.next()) {
                Map<String, String> counterUniqueStringMap = tempKpiFormulaFinalMap.getOrDefault(rs.getString(1),
                        new HashMap<>());

                counterUniqueStringMap.put(rs.getString(2), rs.getString(3));
                tempKpiFormulaFinalMap.put(rs.getString(1), counterUniqueStringMap);
            }
        } catch (Exception e) {
            logger.error("Exception While Getting KPI Formula Map in @getKPIFormulaMap, Message: {}, Error: {}", e.getMessage(), e);
        }
        logger.info("KPI Formula Map Size: {}", tempKpiFormulaFinalMap.size());
        kpiFormulaFinalMap = updateKpiFormulaMap(tempKpiFormulaFinalMap);
        

        for (String kpi : kpiFormulaFinalMap.keySet()) {
            Map<String, Object> kpiformulaMap = kpiFormulaFinalMap.get(kpi); 
            if (kpiformulaMap != null) {
                String formulaString = (String) kpiformulaMap.get("formulaString");
                @SuppressWarnings("unchecked")
                Map<String, String> formulaCounterMap = (Map<String, String>) kpiformulaMap.get("formulaCounterMap");
                formulaString = getInnerKPIFormula(formulaString, formulaCounterMap);
                formulaString = replace(replace(replace(formulaString, "{", "("), "}", ")"), "\\", "");
                formulaCounterMap.remove("null");
                kpiformulaMap.put("formulaString", formulaString);
                kpiformulaMap.put("formulaCounterMap", formulaCounterMap);
            }
        }

        logger.info("KPI Formula Map Size After Update: {}", kpiFormulaFinalMap.size());
        String json = mapper.writeValueAsString(kpiFormulaFinalMap);
        jobcontext.setParameters(KPIFORMULA_MAPJSON, json);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<String, Object>> updateKpiFormulaMap(Map<String, Map<String, String>> tempKpiFormulaFinalMap)
            throws Exception {
        Map<String, Map<String, Object>> kpiFormulaFinalMap = new HashMap<>();
        Iterator<Map.Entry<String, Map<String, String>>> kpiFormula = tempKpiFormulaFinalMap.entrySet().iterator();
        while (kpiFormula.hasNext()) {
            Map.Entry<String, Map<String, String>> formula = kpiFormula.next();
            if (formula.getKey() != null && formula.getKey().contains(HASHHASH_STRING)) {
                String formulaId = StringUtils.substringBeforeLast(formula.getKey(), HASHHASH_STRING);
                String formulaString = StringUtils.substringAfterLast(formula.getKey(), HASHHASH_STRING);
                Map<String, Object> tempMap1 = new HashMap<>();
                Map<String, String> formulaCounterMap = formula.getValue();
                for (String counterVariable : formulaCounterMap.keySet()) {
                    formulaString = formulaString.replaceAll("(" + formulaCounterMap.get(counterVariable) + ")",
                            "CK" + counterVariable + "");
                }
                tempMap1.put("formulaString", formulaString);
                tempMap1.put("formulaCounterMap", formulaCounterMap);
                Set<String> genericKpi = new HashSet<>();
                if (kpiFormulaFinalMap.containsKey(StringUtils.substringBefore(formulaId, "##"))) {
                    Map<String, Object> i = kpiFormulaFinalMap.get(StringUtils.substringBefore(formulaId, "##"));
                    Object object = i.get("generickpi");
                    if (object != null)
                        genericKpi.addAll((Set<String>) object);
                }
                if (formulaId.contains("##")) {
                    Set<String> gKpi = new HashSet<>(
                            Arrays.asList(StringUtils.substringAfter(formulaId, "##").split("##")));
                    genericKpi.addAll(gKpi);
                    tempMap1.put("generickpi", genericKpi);
                }
                kpiFormulaFinalMap.put(StringUtils.substringBefore(formulaId, "##"), tempMap1);
            }
        }
        return kpiFormulaFinalMap;
    }

    private String getInnerKPIFormula(String formulaString, Map<String, String> formulaCounterMap) {
        Matcher kpiMatcher = kpiPattern.matcher(formulaString);
        while (kpiMatcher.find()) {
            String kpi = kpiMatcher.group(1);
            Map<String, Object> kpiMap = kpiFormulaFinalMap.get(kpi);
            if (kpiMap != null) {
                formulaString = formulaString.replace("KPI#" + kpi, (String) kpiMap.get("formulaString"));
                if (!formulaString.contains(kpi) && !formulaString.contains("NVL((KPI#" + kpi)
                        && !formulaString.contains("NVL(((KPI#" + kpi)) {
                    Map<String, String> counterMap = (Map<String, String>) kpiMap.get("formulaCounterMap");
                    formulaCounterMap.putAll(counterMap);
                    return getInnerKPIFormula(formulaString, formulaCounterMap);
                }
            }
        }
        return formulaString;
    }

    private void setValueNetypeRowKeyAppenderMap(JobContext jobcontext, Map<String, String> contextMap)
            throws Exception {

        logger.info("Setting Value Netype RowKey Appender Map...");

        Map<String, String> netypeRowKeyAppenderMap = new HashMap<>();
        String parameter = contextMap.get("ROW_KEY_APPENDER");
        if (!StringUtils.isEmpty(parameter)) {
            if (parameter != null && !parameter.equals("null")) {
                for (String netypeEntry : parameter.split("LINE")) {
                    String netypes = netypeEntry.split("##")[0];
                    String appender = netypeEntry.split("##")[1];
                    for (String netype : netypes.split(",")) {
                        netypeRowKeyAppenderMap.put(netype, appender);
                    }
                }
                String json = mapper.writeValueAsString(netypeRowKeyAppenderMap);
                jobcontext.setParameters("NETYPE_ROW_KEY_APPENDER_MAP", json);
            }
        } else {
            logger.error("RowKey Appender Parameter is Not Set in @setValueNetypeRowKeyAppenderMap.");
        }
        logger.info("Value Netype RowKey Appender Map Size: {}", netypeRowKeyAppenderMap.size());
    }

    private void doFinally(Connection conn, PreparedStatement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error("Error in Closing Statement in parse - {}", Utils.getStackTrace(e));
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Error in Closing connection in parse- {}", Utils.getStackTrace(e));
            }
        }
    }

    private String getAllKpiCode(JobContext jobcontext, Map<String, String> contextMap) {
        String returnValue = "";

        String domain = contextMap.get("DOMAIN");
        String vendor = contextMap.get("VENDOR");
        String technology = contextMap.get("TECHNOLOGY");
        String nodes = contextMap.get("NODES");

        logger.info("Getting All KPI Code With DOMAIN={}, VENDOR={}, TECHNOLOGY={}, NODES={}", domain, vendor,
                technology, nodes);

        String sql = "SELECT GROUP_CONCAT(KPI_CODE) FROM (SELECT kf.KPI_CODE AS KPI_CODE FROM KPI_FORMULA kf WHERE "
                + " kf.DOMAIN = '" + domain + "' AND kf.VENDOR = '" + vendor + "'"
                + " AND kf.DELETED = 0";
        if (nodes != null) {
            if (!nodes.contains(Symbol.APOSTROPHE_STRING)) {
                nodes = "'"
                        + Arrays.asList(nodes.split(Symbol.COMMA_STRING)).stream().collect(Collectors.joining("','"))
                        + "'";
            }
            sql = sql + " AND kf.NODE IN (" + nodes + ") ";
        }
        if (StringUtils.isNotEmpty(technology) && !technology.equalsIgnoreCase("null")) {
            sql = sql + " AND kf.TECHNOLOGY ='" + technology + "' ";
        }
        sql = sql
                + " UNION SELECT gkpi.CODE AS KPI_CODE FROM PM_GENERIC_KPI gkpi WHERE gkpi.CODE IS NOT NULL AND gkpi.DOMAIN = '"
                + domain + "' AND gkpi.DELETED = 0 ) u";

        logger.info("All KPI Code Query: {}", sql);
        ResultSet rs = createConnection(sql, contextMap);
        if (rs != null) {
            try {
                while (rs.next()) {
                    returnValue = rs.getString(1);
                    jobcontext.setParameters(ALL_KPI_CODE, rs.getString(1));
                }
            } catch (SQLException e) {
                logger.error("Exception While Getting All Kpis Code :{}", Utils.getStackTrace(e));
            }
        }
        logger.info("All KPI Code Size: {}", returnValue != null ? returnValue.length() : 0);
        return returnValue;
    }

    private String getMetaColumn(JobContext jobcontext, Map<String, String> contextMap) {
        String returnValue = "";
        String sql = "SELECT VALUE FROM PM_CONFIGURATION WHERE NAME = 'KPI_DATA_META_COLUMNS' AND TYPE = 'JOB'";

        logger.info("Meta Column Query: {}", sql);

        ResultSet rs = createConnection(sql, contextMap);
        if (rs != null) {
            try {
                while (rs.next()) {
                    returnValue = rs.getString(1);
                    jobcontext.setParameters(KPI_DATA_META_COLUMNS, rs.getString(1));
                }
            } catch (SQLException e) {
                logger.error("Error in fetching data from database in @getMetaColumn, Message: {}, Error: {}", e.getMessage(), e);
            }
        }
        if(returnValue==null || returnValue.isEmpty()) {
            returnValue = "BND,CEL,D,DL1,DL2,DL3,DL4,DOG,DT,EGID,EID,ENB,GC,H1,H2,HR,L1,L2,L3,L4,parquetLevel,NEID,NEL,NET,NS,OG,RowKeyAppender,SFID,V,nename,NAM,NN,SAT,SAV,H1_NEID,H2_NEID,ENB_NEID,RK,PT,Date,Time,TN,TC,MV,MT,DNAM,SC";
        }
        logger.info("Meta Column Size: {}", returnValue != null ? returnValue.length() : 0);
        return returnValue;
    }

    private void getPolygonNEDetail(JobContext jobcontext, Map<String, String> contextMap) throws Exception {
        // logger.info("Getting PolygonNEDetails getPolygonNEDetail");
        // Connection conn = null;
        // PreparedStatement stmt = null;
        Map<String, String> polygonNEmap = null;

        // commented
        // try {
        // String technology = contextMap.get("technology");
        // String sqlQuery = "select
        // n.nename,group_concat(concat(concat('DNAM@',og.geoName,'##','NAM@',og.geoCode,'##PCT@',COALESCE(json_extract(pol.metadata,
        // '$.category'),'null'))) SEPARATOR '---') "
        // + " from NetworkElement n JOIN NEGeographyMapping neg on
        // (n.networkelementid_pk=neg.networkelementid_fk) JOIN AdditionalGeo og "
        // + " on (og.id=neg.othergeographyid_fk) JOIN PMPolygon pol on
        // (og.geoCode=concat('P',pol.pmpolygonid_pk)) where n.domain='"
        // + contextMap.get("domain") + "' " + " and n.vendor='" +
        // contextMap.get("vendor")
        // + "' and og.type ='PMPOLYGON' and n.deleted=0 ";
        // if (!com.enttribe.commons.lang.StringUtils.isEmpty(technology)) {
        // sqlQuery = sqlQuery + " and n.technology ='" + technology + "' ";
        // }
        // sqlQuery = sqlQuery + " group by n.nename ";
        // ResultSet rs = createConnectionForPlatform(sqlQuery, contextMap);
        // polygonNEmap = new THashMap<>();
        // while (rs.next()) {
        // if(rs.getString(1) != null) {
        // polygonNEmap.put(rs.getString(1), rs.getString(2));
        // }
        // }
        // logger.info("polygonNEmap Size:{}", polygonNEmap.size());
        // } catch (Exception e) {
        // logger.error("Exception While Getting PolygonNEDetails getPolygonNEDetailp
        // :{}", Utils.getStackTrace(e));
        // } finally {
        // doFinally(conn, stmt);
        // }

        // commented

        ObjectMapper mapper = new ObjectMapper();
        String counterMapJson = mapper.writeValueAsString(polygonNEmap);
        jobcontext.setParameters(POLYGON_NE_MAPJSON, counterMapJson);

    }

    private void getPMCounterVariableAggregationQuery(JobContext jobcontext, Map<String, String> counterAggrMap)
            throws Exception {

        logger.info("Getting PM Counter Variable Aggregation Query...");

        String nodeAggrQueryFinal = null;
        String timeAggrQueryFinal = null;
        String rawFilenodeAggrQueryFinal = null;

        try {
            StringBuilder nodeAggrQuery = new StringBuilder();
            StringBuilder timeAggrQuery = new StringBuilder();
            StringBuilder rawFilenodeAggrQuery = new StringBuilder();
            for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
                String counterKey = entry.getKey();
                String aggrValue = entry.getValue();
                if (counterKey != null && aggrValue != null) {
                    String nodeAggr = aggrValue.split("#")[0];
                    String timeAggr = aggrValue.split("#")[1];
                    if (nodeAggr.equalsIgnoreCase("AVG")) {
                        rawFilenodeAggrQuery = rawFilenodeAggrQuery
                                .append("CAST(" + nodeAggr + "(`" + counterKey + "`) AS Double) AS `"
                                        + counterKey + "`," + "CAST(SUM" + "(`" + counterKey + "`) AS Double) AS `S"
                                        + counterKey + "`,CAST(COUNT(`"
                                        + counterKey + "`) AS Double) AS `C" + counterKey + "` ,");
                        nodeAggrQuery = nodeAggrQuery
                                .append("CAST(SUM(S" + counterKey + ")/SUM(C" + counterKey + ") AS Double) AS `"
                                        + counterKey + "`, CAST(SUM(S" + counterKey + ") AS Double) AS `S" + counterKey
                                        + "`, CAST(SUM(C" + counterKey
                                        + ") AS Double) AS  `C" + counterKey + "`, ");
                    } else if (nodeAggr.equalsIgnoreCase("COUNT")) {
                        rawFilenodeAggrQuery = rawFilenodeAggrQuery
                                .append("CAST(" + nodeAggr + "(`" + counterKey + "`) AS Double) AS `" + counterKey
                                        + "`,");
                        nodeAggrQuery = nodeAggrQuery
                                .append("CAST(SUM" + "(`" + counterKey + "`) AS Double) AS `" + counterKey + "`,");
                    } else if (nodeAggr.equalsIgnoreCase("EXCLUDE_ZERO_AVG")) {
                        rawFilenodeAggrQuery = rawFilenodeAggrQuery.append("CAST(AVG" + "(CASE WHEN `" + counterKey
                                + "`= 0 THEN NULL ELSE `" + counterKey + "` END) AS Double) AS `" + counterKey + "`,"
                                + "CAST(SUM" + "(`"
                                + counterKey + "`) AS Double) AS `S" + counterKey + "`," + "CAST(COUNT(CASE WHEN `"
                                + counterKey
                                + "`= 0 THEN NULL ELSE `" + counterKey + "` END) AS Double) AS `C" + counterKey + "` ,");
                        nodeAggrQuery = nodeAggrQuery
                                .append("CAST(SUM(S" + counterKey + ")/SUM(C" + counterKey + ") AS Double) AS `"
                                        + counterKey + "`, CAST(SUM(S" + counterKey + ") AS Double) AS `S" + counterKey
                                        + "`,CAST(SUM(C" + counterKey
                                        + ") AS Double) AS  `C" + counterKey + "`, ");
                    } else {
                        rawFilenodeAggrQuery = rawFilenodeAggrQuery
                                .append("CAST(" + nodeAggr + "(`" + counterKey + "`) AS Double) AS `" + counterKey
                                        + "`,");
                        nodeAggrQuery = nodeAggrQuery
                                .append("CAST(" + nodeAggr + "(`" + counterKey + "`) AS Double) AS `" + counterKey
                                        + "`,");
                    }

                    if (timeAggr.equalsIgnoreCase("EXCLUDE_ZERO_AVG")) {
                        timeAggrQuery = timeAggrQuery.append("CAST(AVG" + "(CASE WHEN `" + counterKey
                                + "`= 0 THEN NULL ELSE `" + counterKey + "` END) AS Double) AS `" + counterKey + "`,");
                    } else {
                        timeAggrQuery = timeAggrQuery
                                .append("CAST(" + timeAggr + "(`" + counterKey + "`) AS Double) AS `" + counterKey
                                        + "`,");
                    }
                }
            }
            nodeAggrQueryFinal = StringUtils.substringBeforeLast(nodeAggrQuery.toString(), ",");
            timeAggrQueryFinal = StringUtils.substringBeforeLast(timeAggrQuery.toString(), ",");
            rawFilenodeAggrQueryFinal = StringUtils.substringBeforeLast(rawFilenodeAggrQuery.toString(), ",");
        } catch (Exception e) {
            logger.error("Exception While Getting PM Counter Variable Aggregation Query in @getPMCounterVariableAggregationQuery, Message: {}, Error: {}", e.getMessage(), e);
        }
        logger.info("Node Aggregation Query Final: {}", nodeAggrQueryFinal);
        logger.info("Time Aggregation Query Final: {}", timeAggrQueryFinal);
        logger.info("Raw File Node Aggregation Query Final: {}", rawFilenodeAggrQueryFinal);

        jobcontext.setParameters("COUNTER_NODE_AGGR_QUERY", nodeAggrQueryFinal);
        jobcontext.setParameters("COUNTER_TIME_AGGR_QUERY", timeAggrQueryFinal);
        jobcontext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", rawFilenodeAggrQueryFinal);

    }

    private ResultSet createConnection(String sqlQuery, Map<String, String> contextMap) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
        final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
        final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
        final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");

        logger.info("JDBC Driver: {}, JDBC URL: {}, JDBC Username: {}, JDBC Password: {}", JDBC_DRIVER, DB_URL, USER,
                PASS);

        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("Error in executing query in @createConnection, Message: {}, Error: {}", e.getMessage(), e);
        } finally {
            doFinally(conn, stmt);
        }
        return rs;
    }

    private ResultSet createConnectionForPM(String sqlQuery, Map<String, String> contextMap) {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        // logger.info("Going to Execute query :{}, from
        // {}",sqlQuery,stackTraceElements[1].getMethodName());
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
        final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
        final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
        final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("Error in fetching data from database - {}", Utils.getStackTrace(e));
        } finally {
            doFinally(conn, stmt);
        }
        return rs;
    }

    private ResultSet createConnectionForPlatform(String sqlQuery, Map<String, String> contextMap) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        final String JDBC_DRIVER = contextMap.get("SPARK_PM_JDBC_DRIVER");
        final String DB_URL = contextMap.get("SPARK_PM_JDBC_URL");
        final String USER = contextMap.get("SPARK_PM_JDBC_USERNAME");
        final String PASS = contextMap.get("SPARK_PM_JDBC_PASSWORD");
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.prepareStatement(sqlQuery);
            rs = stmt.executeQuery();
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("Error in fetching data from database - {}", Utils.getStackTrace(e));
        } finally {
            doFinally(conn, stmt);
        }
        return rs;
    }
}
