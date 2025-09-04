package com.enttribe.pm.job.quarterly.common;
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
 
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
 
import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
 
public class GetStaticMapForPMProcess extends Processor {
 
    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(GetStaticMapForPMProcess.class);
    public String tempTable;
 
    private static String jdbcDriver = null;
    private static String jdbcUrl = null;
    private static String jdbcUsername = null;
    private static String jdbcPassword = null;
    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static final ObjectMapper mapper = new ObjectMapper();
 
 
    public GetStaticMapForPMProcess() {
        super();
        logger.info("GetStaticMapForPMProcess: Initialized With JobContext");
    }
 
    public GetStaticMapForPMProcess(Dataset<Row> dataframe, Integer id, String processorName,
            String tempTable) {
        super(id, processorName);
        this.dataFrame = dataframe;
        this.tempTable = tempTable;
 
        logger.debug("GetStaticMapForPMProcess: Initialized with ID: {}, ProcessorName: {}, TempTable: {}",
                id, processorName, tempTable);
    }
 
    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) {
 
        Map<String, String> contextMap = jobContext.getParameters();
 
        jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
        jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
        jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
        jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");
 
        logger.debug("JDBC Connection Details: Driver={}, URL={}, Username={}, Password={}",
                jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);
 
        logger.debug("GetStaticMapForPMProcess Execution Started :: ");
        try {
            Map<String, List<Map<String, String>>> geAllCounterCategoryMap = geAllCounterCategoryMap(jobContext,
                    contextMap);
            getCategoryVsDeltaCounterMap(geAllCounterCategoryMap, jobContext);
            initializeCategoryCounterMapJson(jobContext, contextMap);
            initializeAllCategoryCounterMapJson(jobContext, contextMap);
            initializeMaxSequenceNo(jobContext, contextMap);
          
        } catch (Exception e) {
            logger.error("Exception Occurred During GetStaticMapForPMProcess Execution: {}", e.getMessage(), e);
        }
 
        return this.dataFrame;
    }
 
    private Map<String, List<Map<String, String>>> geAllCounterCategoryMap(JobContext jobcontext,
            Map<String, String> contextMap) throws JsonProcessingException {
        Map<String, List<Map<String, String>>> categoryCounterDetailsMap = new HashMap<>();
        try {
            // Extract values from contextMap
            String vendor = contextMap.get("VENDOR");
            String domain = contextMap.get("DOMAIN");
            String technology = contextMap.get("TECHNOLOGY");
            String nodes = contextMap.get("NODES");
 
            // Construct the SQL query dynamically
            StringBuilder sql = new StringBuilder(
                    "SELECT " +
                            "    UPPER(pn.NODE) AS Node, " +
                            "    UPPER(pc.CATEGORY_NAME) AS CategoryName, " +
                            "    pc.PM_CATEGORY_ID_PK, " +
                            "    kc.KPI_COUNTER_ID_PK, " +
                            "    COALESCE(UPPER(kc.RAW_FILE_COUNTER_ID), 'null') AS RawFileCounterId, " +
                            "    COALESCE(UPPER(kc.COUNTER_HEADER_NAME), 'null') AS CounterHeaderName, " +
                            "    COALESCE(kc.SEQUENCE_NO, 'null') AS SequenceNo, " +
                            "    COALESCE(UPPER(kc.CATEGORY_ALIAS_NAME), 'null') AS CategoryAliasName, " +
                            "    COALESCE(kc.ATTRIBUTE, 'null') AS Attribute, " +
                            "    COALESCE(kc.DELTA, 'null') AS Delta " +
                            "FROM " +
                            "    KPI_COUNTER kc " +
                            "JOIN " +
                            "    PM_CATEGORY pc " +
                            "    ON kc.PM_CATEGORY_ID_FK = pc.PM_CATEGORY_ID_PK " +
                            "JOIN " +
                            "    PM_NODE_VENDOR pn " +
                            "    ON pc.PM_NODE_VENDOR_ID_FK = pn.PM_NODE_VENDOR_ID_PK " +
                            "WHERE " +
                            "    pc.CATEGORY_NAME != 'COUNTERTIME' ");
 
            if (!com.enttribe.commons.lang.StringUtils.isEmpty(vendor)) {
                sql.append(" AND pn.VENDOR = '").append(vendor).append("' ");
            }
            if (!com.enttribe.commons.lang.StringUtils.isEmpty(domain)) {
                sql.append(" AND pn.DOMAIN = '").append(domain).append("' ");
            }
            if (!com.enttribe.commons.lang.StringUtils.isEmpty(technology)) {
                sql.append(" AND pn.TECHNOLOGY = '").append(technology).append("' ");
            }
            if (!com.enttribe.commons.lang.StringUtils.isEmpty(nodes)) {
                String formattedNodes = "'"
                        + Arrays.stream(nodes.split(Symbol.COMMA_STRING)).collect(Collectors.joining("','")) + "'";
                sql.append(" AND pn.NODE IN (").append(formattedNodes).append(") ");
            }
 
            sql.append(" ORDER BY pc.CATEGORY_NAME, kc.SUB_CAT_INDEX");
 
            ResultSet rs = getResultSetFromPMDatabase(sql.toString(), contextMap);
            while (rs.next()) {
                // String mapKey = rs.getString("Node") + "_" + rs.getString("CategoryName");
                String mapKey = rs.getString("CategoryName");
                List<Map<String, String>> orDefault = categoryCounterDetailsMap.getOrDefault(mapKey, new ArrayList<>());
                orDefault.add(getAllCounterInfo(rs));
                categoryCounterDetailsMap.put(mapKey, orDefault);
            }
        } catch (Exception e) {
            logger.error("Exception while getting All Category Counter :{}", Utils.getStackTrace(e));
        }
        return categoryCounterDetailsMap;
    }
 
    public static Map<String, String> getAllCounterInfo(ResultSet rs) throws SQLException {
        Map<String, String> map = new HashMap<>();
        map.put("Node", rs.getString(1));
        map.put("CategoryName", rs.getString(2));
        map.put("CounterColumnkey", rs.getString(3));
        map.put("PMCategoryId_PK", rs.getString(4));
        map.put("RawFileCounterId", rs.getString(5));
        map.put("CounterHeaderName", rs.getString(6));
        map.put("SequenceNo", rs.getString(7));
        map.put("CategoryAliasName", rs.getString(8));
        map.put("Attribute", rs.getString(9));
        map.put("Delta", rs.getString(10));
        return map;
    }
 
    private void getCategoryVsDeltaCounterMap(
            Map<String, List<Map<String, String>>> geAllCounterCategoryMap, JobContext jobContext)
            throws JsonProcessingException {
        Map<String, List<String>> categoryVsDeltaCounterMap = new HashMap<>();
        geAllCounterCategoryMap.forEach((k, v) -> {
            v.forEach(e -> {
                String delta = e.get("Delta");
                String attribute = e.get("Attribute");
                String node = e.get("Node");
                if (!StringUtils.isEmpty(delta) && delta.equalsIgnoreCase("1") && attribute.equalsIgnoreCase("0")) {
                    List<String> orDefault = categoryVsDeltaCounterMap.getOrDefault(k, new ArrayList<>());
                    String deltaCounter = !StringUtils.isEmpty(e.get("RawFileCounterId")) ? e.get("RawFileCounterId")
                            : e.get("CounterHeaderName");
                    orDefault.add(deltaCounter);
                    categoryVsDeltaCounterMap.put(k, orDefault);
 
                }
            });
        });
        String categoryVsDeltaString = mapper.writeValueAsString(categoryVsDeltaCounterMap);
        logger.debug("CATEGORY_VS_DELTACOUNTER :{}", categoryVsDeltaString);
        jobContext.setParameters(CATEGORY_VS_DELTACOUNTER, categoryVsDeltaString);
    }
 
    private void initializeMaxSequenceNo(JobContext jobContext, Map<String, String> contextMap) {
        String maxSequenceNo = "0";
 
        try {
            String vendor = contextMap.get("VENDOR");
            String domain = contextMap.get("DOMAIN");
            String technology = contextMap.get("TECHNOLOGY");
            String nodes = contextMap.get("NODES");
 
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT MAX(kc.SEQUENCE_NO) ")
                    .append("FROM KPI_COUNTER kc ")
                    .append("JOIN PM_CATEGORY pc ON kc.PM_CATEGORY_ID_FK = pc.PM_CATEGORY_ID_PK ")
                    .append("JOIN PM_NODE_VENDOR pn ON pc.PM_NODE_VENDOR_ID_FK = pn.PM_NODE_VENDOR_ID_PK ")
                    .append("WHERE pn.VENDOR = '").append(vendor).append("' ")
                    .append("AND pn.DOMAIN = '").append(domain).append("' ");
 
            if (technology != null && !technology.trim().isEmpty()) {
                sqlBuilder.append("AND pn.TECHNOLOGY = '").append(technology.trim()).append("' ");
            }
 
            if (nodes != null && !nodes.trim().isEmpty()) {
                String formattedNodes = Arrays.stream(nodes.split(","))
                        .map(String::trim)
                        .filter(node -> !node.isEmpty())
                        .collect(Collectors.joining("','", "'", "'"));
                sqlBuilder.append("AND pn.NODE IN (").append(formattedNodes).append(") ");
            }
 
            String finalSql = sqlBuilder.toString();
            logger.debug("Executing Max Sequence No Query: {}", finalSql);
 
            ResultSet resultSet = getResultSetFromPMDatabase(finalSql, contextMap);
            if (resultSet != null && resultSet.next()) {
                String result = resultSet.getString(1);
                if (result != null) {
                    maxSequenceNo = result;
                }
            }
 
            logger.debug("Max Sequence Number: {}", maxSequenceNo);
 
        } catch (Exception e) {
            logger.error("Exception while getting Max Sequence Number: {}", e.getMessage(), e);
        }
 
        jobContext.setParameters("sequenceno", maxSequenceNo);
    }
 
    private void initializeAllCategoryCounterMapJson(JobContext jobContext, Map<String, String> contextMap) {
        Map<String, List<Map<String, String>>> categoryCounterMap = new HashMap<>();
        try {
            String vendor = contextMap.get("VENDOR");
            String domain = contextMap.get("DOMAIN");
            String technology = contextMap.get("TECHNOLOGY");
            String nodes = contextMap.get("NODES");
 
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT UPPER(pc.CATEGORY_NAME), ")
                    .append("CONCAT(COALESCE(kc.KPI_COUNTER_ID_PK, 'null'), '##', ")
                    .append("COALESCE(UPPER(kc.RAW_FILE_COUNTER_ID), 'null'), '##', ")
                    .append("COALESCE(kc.SEQUENCE_NO, 'null'), '##', ")
                    .append("COALESCE(UPPER(kc.CATEGORY_ALIAS_NAME), 'null')) AS category_counter_map ")
                    .append("FROM KPI_COUNTER kc ")
                    .append("JOIN PM_CATEGORY pc ON kc.PM_CATEGORY_ID_FK = pc.PM_CATEGORY_ID_PK ")
                    .append("JOIN PM_NODE_VENDOR pn ON pc.PM_NODE_VENDOR_ID_FK = pn.PM_NODE_VENDOR_ID_PK ")
                    .append("WHERE pn.VENDOR = '").append(vendor).append("' ")
                    .append("AND pn.DOMAIN = '").append(domain).append("' ");
 
            if (technology != null && !technology.trim().isEmpty()) {
                sqlBuilder.append("AND pn.TECHNOLOGY = '").append(technology.trim()).append("' ");
            }
 
            if (nodes != null && !nodes.trim().isEmpty()) {
                String formattedNodes = Arrays.stream(nodes.split(","))
                        .map(String::trim)
                        .filter(node -> !node.isEmpty())
                        .collect(Collectors.joining("','", "'", "'"));
                sqlBuilder.append("AND pn.NODE IN (").append(formattedNodes).append(") ");
            }
 
            String finalSql = sqlBuilder.toString();
            logger.info("ALL_CATEGORY_COUNTER_MAPJSON Query: {}", finalSql);
 
            ResultSet resultSet = getResultSetFromPMDatabase(finalSql, contextMap);
 
            while (resultSet != null && resultSet.next()) {
                String category = resultSet.getString(1);
                Map<String, String> counterInfoMap = getAllCounterInfoMap(resultSet, category);
                categoryCounterMap
                        .computeIfAbsent(category, k -> new ArrayList<>())
                        .add(counterInfoMap);
            }
 
            logger.info("ALL_CATEGORY_COUNTER_MAPJSON Size: {}", categoryCounterMap.size());
 
        } catch (Exception e) {
            logger.error("Exception While Getting ALL_CATEGORY_COUNTER_MAPJSON : {}", e.getMessage(), e);
        }
 
        try {
            String counterMapJson = new ObjectMapper().writeValueAsString(categoryCounterMap);
            jobContext.setParameters("ALL_CATEGORY_COUNTER_MAPJSON", counterMapJson);
 
        } catch (Exception e) {
            logger.error("Exception While Building ALL_CATEGORY_COUNTER_MAPJSON : {}", e.getMessage(), e);
        }
    }
 
 
    public static Map<String, String> getAllCounterInfoMap(ResultSet resultSet, String categoryName)
            throws SQLException {
        String rawCounterString = resultSet.getString(2);
        Map<String, String> counterInfoMap = new HashMap<>();
 
        if (rawCounterString != null && !rawCounterString.trim().isEmpty()) {
            String[] counterParts = rawCounterString.split("##", -1);
            if (counterParts.length >= 3) {
                String counterColumnKey = counterParts[0];
                String counterName = counterParts[1];
                String sequenceNo = counterParts[2];
 
                if (counterColumnKey != null && !"null".equalsIgnoreCase(counterColumnKey.trim())) {
                    counterInfoMap.put("CounterColumnkey", counterColumnKey.trim());
                }
                counterInfoMap.put("CounterName", counterName != null ? counterName.trim() : "");
                counterInfoMap.put("sequenceno", sequenceNo != null ? sequenceNo.trim() : "");
            }
        }
        counterInfoMap.put("Category", categoryName);
        return counterInfoMap;
    }
 
    private void initializeCategoryCounterMapJson(JobContext jobContext, Map<String, String> contextMap) {
        Map<String, List<Map<String, String>>> counterCategoryMap = null;
 
        try {
 
            String vendor = contextMap.get("VENDOR");
            String domain = contextMap.get("DOMAIN");
            String technology = contextMap.get("TECHNOLOGY");
            String nodes = contextMap.get("NODES");
 
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(
                    "SELECT DISTINCT pc.PM_COUNTER_VARIABLE_ID_PK, UPPER(c.CATEGORY_NAME), c.PM_CATEGORY_ID_PK, ")
                    .append("(SELECT UPPER(RAW_FILE_COUNTER_ID) FROM KPI_COUNTER ki WHERE pc.KPI_COUNTER_ID_FK IS NOT NULL AND ki.KPI_COUNTER_ID_PK = pc.KPI_COUNTER_ID_FK) AS rawfilecounterId, ")
                    .append("(SELECT UPPER(COUNTER_HEADER_NAME) FROM KPI_COUNTER ki WHERE pc.KPI_COUNTER_ID_FK IS NOT NULL AND ki.KPI_COUNTER_ID_PK = pc.KPI_COUNTER_ID_FK) AS counterHeaderName, ")
                    .append("(SELECT COUNTER_INDEX FROM KPI_COUNTER ki WHERE pc.KPI_COUNTER_ID_FK IS NOT NULL AND ki.KPI_COUNTER_ID_PK = pc.KPI_COUNTER_ID_FK) AS RawFileCounterIndex, ")
                    .append("(SELECT COUNTER_INDEX FROM KPI_COUNTER ki WHERE pc.KPI_COUNTER_ID_FK IS NOT NULL AND ki.KPI_COUNTER_ID_PK = pc.KPI_COUNTER1_ID_FK) AS SubCategoryOneHeaderName, ")
                    .append("(SELECT COUNTER_INDEX FROM KPI_COUNTER ki WHERE pc.KPI_COUNTER_ID_FK IS NOT NULL AND ki.KPI_COUNTER_ID_PK = pc.KPI_COUNTER2_ID_FK) AS SubCategoryTwoHeaderName, ")
                    .append("(SELECT COUNTER_INDEX FROM KPI_COUNTER ki WHERE pc.KPI_COUNTER_ID_FK IS NOT NULL AND ki.KPI_COUNTER_ID_PK = pc.KPI_COUNTER3_ID_FK) AS SubCategoryThreeHeaderName, ")
                    .append("pc.SUBCATEGORY1_VALUE, pc.SUBCATEGORY2_VALUE, pc.SUBCATEGORY3_VALUE, pc.NODE_AGGREGATION, pc.TIME_AGGREGATION ")
                    .append("FROM PM_COUNTER_VARIABLE pc ")
                    .append("JOIN PM_CATEGORY c ON pc.PM_CATEGORY_ID_FK = c.PM_CATEGORY_ID_PK ")
                    .append("JOIN PM_NODE_VENDOR pn ON c.PM_NODE_VENDOR_ID_FK = pn.PM_NODE_VENDOR_ID_PK ")
                    .append("JOIN FORMULA_COUNTER_MAPPING fm ON pc.PM_COUNTER_VARIABLE_ID_PK = fm.PM_COUNTER_VARIABLE_ID_FK ")
                    .append("JOIN KPI_FORMULA kf ON fm.KPI_FORMULA_ID_FK = kf.KPI_FORMULA_ID_PK ")
                    .append("WHERE kf.KPI_TYPE = 'REGULAR' AND kf.DELETED = 0 AND kf.KPI_FORMULA_DESC NOT LIKE '%timeshift%' ")
                    .append("AND pn.VENDOR = '").append(vendor).append("' ")
                    .append("AND pn.DOMAIN = '").append(domain).append("' ")
                    .append("AND pc.NODE_AGGREGATION IS NOT NULL AND pc.NODE_AGGREGATION != '' ")
                    .append("AND pc.TIME_AGGREGATION IS NOT NULL AND pc.TIME_AGGREGATION != '' ");
 
            if (technology != null && !technology.trim().isEmpty()) {
                sqlBuilder.append("AND pn.TECHNOLOGY = '").append(technology).append("' ");
            }
 
            if (nodes != null && !nodes.trim().isEmpty()) {
                String formattedNodes = Arrays.stream(nodes.split(","))
                        .map(node -> "'" + node + "'")
                        .collect(Collectors.joining(","));
                sqlBuilder.append("AND pn.NODE IN (").append(formattedNodes).append(") ");
            }
 
            String sqlQuery = sqlBuilder.toString();
            logger.debug("CATEGORY_COUNTER_MAPJSON Query: {}", sqlQuery);
 
            ResultSet resultSet = getResultSetFromPMDatabase(sqlQuery, contextMap);
            counterCategoryMap = new HashMap<>();
            while (resultSet.next()) {
                if (!counterCategoryMap.isEmpty() && counterCategoryMap.containsKey(resultSet.getString(2))) {
                    List<Map<String, String>> counterInfoMapList = counterCategoryMap.get(resultSet.getString(2));
                    counterInfoMapList.add(getCounterInfoMap(resultSet));
                    counterCategoryMap.put(resultSet.getString(2), counterInfoMapList);
                } else {
                    List<Map<String, String>> counterInfoMapList = new ArrayList<>();
                    counterInfoMapList.add(getCounterInfoMap(resultSet));
                    counterCategoryMap.put(resultSet.getString(2), counterInfoMapList);
                }
            }
            logger.debug("CATEGORY_COUNTER_MAPJSON Size: {}", counterCategoryMap.size());
            String counterMapJson = new ObjectMapper().writeValueAsString(counterCategoryMap);
            jobContext.setParameters("CATEGORY_COUNTER_MAPJSON", counterMapJson);
 
        } catch (Exception e) {
            logger.error("Exception While Building CATEGORY_COUNTER_MAPJSON : {}", e.getMessage(), e);
        }
    }
 
    public static Map<String, String> getCounterInfoMap(ResultSet resultSet) throws SQLException {
        HashMap<String, String> counterInfoMap = new HashMap<>();
        counterInfoMap.put("CounterColumnkey", String.valueOf(resultSet.getString(1)));
        counterInfoMap.put("Category", resultSet.getString(2));
        counterInfoMap.put("pmcategoryid_pk", resultSet.getString(3));
        counterInfoMap.put("RawFileCounterName", resultSet.getString(4));
        counterInfoMap.put("CounterHeaderName", resultSet.getString(5));
        counterInfoMap.put("ColumnIndex", resultSet.getString(6));
        counterInfoMap.put("SubCategoryOneIndex", resultSet.getString(7));
        counterInfoMap.put("SubCategoryTwoIndex", resultSet.getString(8));
        counterInfoMap.put("SubCategoryThreeIndex", resultSet.getString(9));
        counterInfoMap.put("SubCategoryOne", resultSet.getString(10));
        counterInfoMap.put("SubCategoryTwo", resultSet.getString(11));
        counterInfoMap.put("SubCategoryThree", resultSet.getString(12));
        counterInfoMap.put("NodeAgg", resultSet.getString(13));
        counterInfoMap.put("TimeAggr", resultSet.getString(14));
        return counterInfoMap;
    }
 
    private void doFinally(Connection connection, PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                logger.error("Exception While Closing PreparedStatement : {}", e.getMessage(), e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("Exception While Closing Connection : {}", e.getMessage(), e);
            }
        }
    }
 
    private ResultSet getResultSetFromPMDatabase(String sqlQuery, Map<String, String> contextMap) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            preparedStatement = connection.prepareStatement(sqlQuery);
            resultSet = preparedStatement.executeQuery();
        } catch (ClassNotFoundException e) {
            logger.error("Failed To Load JDBC Driver: Driver={}, Message={}, Exception={}",
                    jdbcDriver, e.getMessage(), e);
        } catch (SQLException e) {
            logger.error("Failed To Execute Database Operation: Query={}, Message={}, Exception={}",
                    sqlQuery, e.getMessage(), e);
        } finally {
            doFinally(connection, preparedStatement);
        }
        return resultSet;
    }
}
 
 