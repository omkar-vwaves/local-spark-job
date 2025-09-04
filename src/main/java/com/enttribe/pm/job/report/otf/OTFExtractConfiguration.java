package com.enttribe.pm.job.report.otf;

import com.enttribe.commons.lang.StringUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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

public class OTFExtractConfiguration extends Processor {

   private static final Logger logger = LoggerFactory.getLogger(OTFExtractConfiguration.class);

   private static final String FALLBACK_SPARK_PM_JDBC_DRIVER = "org.mariadb.jdbc.Driver";
   private static final String FALLBACK_SPARK_PM_JDBC_URL = "jdbc:mysql://mysql-nst-cluster.nstdb.svc.cluster.local:6446/PERFORMANCE_A_LAB?autoReconnect=true";
   private static final String FALLBACK_SPARK_PM_JDBC_USERNAME = "PERFORMANCE";
   private static final String FALLBACK_SPARK_PM_JDBC_PASSWORD = "perform!123";

   public OTFExtractConfiguration() {
      super();
      logger.error("OTFExtractConfiguration Constructor Called!");
   }

   public OTFExtractConfiguration(Dataset<Row> dataFrame, int processorId, String processorName) {
      super(processorId, processorName);
      logger.error("OTFExtractConfiguration Constructor Called with Input DataFrame With ID: {} and Process Name: {}",
            processorId, processorName);
   }

   @Override
   public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

      logger.error("OTFExtractConfiguration Execution Started !");

      if (this.dataFrame == null && this.dataFrame.isEmpty()) {
         logger.error("OTFExtractConfiguration Execution Failed! Input DataFrame is Empty!");
         return this.dataFrame;
      }

      this.dataFrame.show(5);
      logger.error("++++++++++[REPORT WIDGET DETAILS]++++++++++");

      List<Row> rowList = this.dataFrame.collectAsList();
      if (rowList.isEmpty()) {
         logger.error("OTFExtractConfiguration Execution Failed! Input DataFrame is Empty!");
         return this.dataFrame;
      }

      Row row = rowList.get(0);
      Map<String, String> reportWidgetDetailsMap = getReportWidgetDetailsMap(row, jobContext);

      String configuration = reportWidgetDetailsMap.get("CONFIGURATION");

      Map<String, Map<String, String>> ragConfigurationMap = getRagConfiguration(configuration);
      if (ragConfigurationMap == null || ragConfigurationMap.isEmpty()) {
         ragConfigurationMap = new HashMap<>();
      } else {
         if (ragConfigurationMap.containsKey("")) {
            ragConfigurationMap.remove("");
         }
      }

      String ragConfiguration = new ObjectMapper().writeValueAsString(ragConfigurationMap);
      jobContext.setParameters("RAG_CONFIGURATION", ragConfiguration);
      Map<String, String> nodeAndAggregationDetailsMap = getNodeAndAggregationDetails(reportWidgetDetailsMap,
            jobContext);
      Map<String, String> extraParametersMap = getExtraParameters(reportWidgetDetailsMap, jobContext);
      String trinoOrcFilePaths = getTrinoOrcFilePaths(extraParametersMap, reportWidgetDetailsMap, jobContext);
      Map<String, String> metaColumnsMap = getMetaColumnsMap(extraParametersMap);

      logger.error("Meta Columns Map: {}", metaColumnsMap);
      logger.error("Report Widget Details Map: {}", reportWidgetDetailsMap);
      logger.error("RAG Configuration Map: {}", ragConfigurationMap);
      logger.error("Node And Aggregation Details Map: {}", nodeAndAggregationDetailsMap);
      logger.error("Extra Parameters Map: {}", extraParametersMap);

      Map<String, String> kpiDetailsMap = new LinkedHashMap<>();
      Map<String, String> kpiCodeWithKpiNameMap = new LinkedHashMap<>();
      Map<String, String> counterDetailsMap = new LinkedHashMap<>();
      Map<String, String> counterIdWithCounterNameMap = new LinkedHashMap<>();
      List<String> kpiCodeList = new ArrayList<>();
      List<String> counterIdList = new ArrayList<>();
      initializeKPICounterDetails(reportWidgetDetailsMap, kpiDetailsMap, counterDetailsMap, kpiCodeList, counterIdList,
            kpiCodeWithKpiNameMap, counterIdWithCounterNameMap);

      String kpiCodeCommaSeparated = kpiCodeList.stream().collect(Collectors.joining(","));
      String counterIdCommaSeparated = counterIdList.stream().collect(Collectors.joining(","));

      jobContext.setParameters("KPI_CODE_COMMA_SEPARATED", kpiCodeCommaSeparated);
      jobContext.setParameters("COUNTER_ID_COMMA_SEPARATED", counterIdCommaSeparated);

      logger.error("KPI Code Comma Separated: {}", kpiCodeCommaSeparated);
      logger.error("Counter Id Comma Separated: {}", counterIdCommaSeparated);

      Map<String, List<String>> kpiGroupMap = new HashMap<>();
      if (!kpiCodeCommaSeparated.isEmpty()) {
         kpiGroupMap = getKpiGroupMap(configuration);
      }

      if (kpiGroupMap == null || kpiGroupMap.isEmpty()) {
         kpiGroupMap = new HashMap<>();
      }

      String kpiGroup = new ObjectMapper().writeValueAsString(kpiGroupMap);
      jobContext.setParameters("EXCEL_HEADER_GROUP", kpiGroup);

      logger.error("KPI Group Map: {}", kpiGroupMap);
      logger.error("KPI Details Map: {}", kpiDetailsMap);
      logger.error("Counter Details Map: {}", counterDetailsMap);
      logger.error("KPI Code List: {}", kpiCodeList);
      logger.error("Counter Id List: {}", counterIdList);
      logger.error("KPI Code With KPI Name Map: {}", kpiCodeWithKpiNameMap);
      logger.error("Counter Id With Counter Name Map: {}", counterIdWithCounterNameMap);

      String reportWidgetDetails = new ObjectMapper().writeValueAsString(reportWidgetDetailsMap);
      String nodeAndAggregationDetails = new ObjectMapper().writeValueAsString(nodeAndAggregationDetailsMap);
      String extraParameters = new ObjectMapper().writeValueAsString(extraParametersMap);

      String kpiDetails = new ObjectMapper().writeValueAsString(kpiDetailsMap);
      String counterDetails = new ObjectMapper().writeValueAsString(counterDetailsMap);
      String kpiCodeWithKpiName = new ObjectMapper().writeValueAsString(kpiCodeWithKpiNameMap);
      String counterIdWithCounterName = new ObjectMapper().writeValueAsString(counterIdWithCounterNameMap);

      jobContext.setParameters("REPORT_WIDGET_DETAILS", reportWidgetDetails);
      jobContext.setParameters("NODE_AND_AGGREGATION_DETAILS", nodeAndAggregationDetails);
      jobContext.setParameters("EXTRA_PARAMETERS", extraParameters);
      jobContext.setParameters("KPI_DETAILS", kpiDetails);
      jobContext.setParameters("COUNTER_DETAILS", counterDetails);
      jobContext.setParameters("TRINO_ORC_FILE_PATHS", trinoOrcFilePaths);
      jobContext.setParameters("KPI_CODE_LIST", kpiCodeList.toString());
      jobContext.setParameters("COUNTER_ID_LIST", counterIdList.toString());
      jobContext.setParameters("KPI_CODE_WITH_KPI_NAME_MAP", kpiCodeWithKpiName);
      jobContext.setParameters("COUNTER_ID_WITH_COUNTER_NAME_MAP", counterIdWithCounterName);

      String frequency = extraParametersMap.get("FREQUENCY");
      jobContext.setParameters("FREQUENCY", frequency);

      String fromDate = extraParametersMap.get("FROM_DATE");
      String toDate = extraParametersMap.get("TO_DATE");

      String timeKeysCommaSeparated = getTimeKeys(fromDate, toDate, jobContext);
      logger.error("Time Keys Comma Separated: {}", timeKeysCommaSeparated);
      jobContext.setParameters("TIME_KEYS_COMMA_SEPARATED", timeKeysCommaSeparated);

      String isKpiCodeListEmpty = kpiCodeList.isEmpty() ? "true" : "false";
      jobContext.setParameters("IS_KPI_CODE_LIST_EMPTY", isKpiCodeListEmpty);

      String aggregationLevel = getAggregationLevel(reportWidgetDetailsMap, nodeAndAggregationDetailsMap, jobContext);
      String geoL1 = nodeAndAggregationDetailsMap.get("GEOGRAPHY_L1");
      if (geoL1.equalsIgnoreCase("CUSTOM")) {
         // No Need Of Sub-Category Here
         String node = nodeAndAggregationDetailsMap.get("NODE");
         String mo = nodeAndAggregationDetailsMap.get("MO");
         if (node.contains("INDIVIDUAL")) {
            // If Node
            if (mo.contains("INDIVIDUAL")) {
               aggregationLevel = "NAM";
            } else {
               aggregationLevel = "H1";
            }

         } else {
            // Node Aggregated
            aggregationLevel = "L0";
         }
      } else {
         aggregationLevel = updateAggregationLevel(aggregationLevel, jobContext, reportWidgetDetailsMap);
      }

      logger.error("Aggregation Level: {} Set to Job Context Successfully!", aggregationLevel);

      jobContext.setParameters("AGGREGATION_LEVEL", aggregationLevel);
      if ((aggregationLevel.equalsIgnoreCase("H1") || aggregationLevel.equalsIgnoreCase("NAM"))
            && extraParametersMap.get("SELECTED_HEADER").equalsIgnoreCase("default")) {
         metaColumnsMap.remove("NODENAME");
         metaColumnsMap.put("ENTITY_ID", "Node");
         metaColumnsMap.put("ENTITY_NAME", "Node Name");
      }

      String metaColumns = new ObjectMapper().writeValueAsString(metaColumnsMap);
      jobContext.setParameters("META_COLUMNS", metaColumns);

      if (isKpiCodeListEmpty.equals("true")) {
         logger.error("KPI Code List is Empty! No Need Of KPI Computation!");

         proceedWithOnlyCounterNodeAndTimeAggregation(jobContext, timeKeysCommaSeparated);

         Map<String, Map<String, String>> kpiFormulaFinalMap = new HashMap<>();

         logger.error("KPI Formula Final Map: {}", kpiFormulaFinalMap);
         String KPI_FORMULA_MAP_JSON = new ObjectMapper().writeValueAsString(kpiFormulaFinalMap);
         jobContext.setParameters("KPI_FORMULA_MAP", KPI_FORMULA_MAP_JSON);

         return this.dataFrame;

      }

      // Added To Achieve The KPI With Sub KPI And Time Shift With Sub KPI

      // Set<String> normalKPIs = new HashSet<>();
      // Set<String> timeShiftedKPIs = new HashSet<>();
      // Set<String> kpiCodeSet = new HashSet<>(kpiCodeList);
      // getNormalAndTimeShiftKPIs(kpiCodeSet, normalKPIs, timeShiftedKPIs,
      // jobContext);
      // logger.error("Normal KPIs: {}", normalKPIs);
      // logger.error("Time Shifted KPIs: {}", timeShiftedKPIs);

      // Set<String> kpiWithSubKPI = new HashSet<>();
      // kpiWithSubKPI = getNormalSubkpiWithKPI(normalKPIs, kpiWithSubKPI,
      // timeShiftedKPIs, jobContext);
      // logger.error("KPI With Sub KPI: {}", kpiWithSubKPI);

      // Set<String> timeShiftWithSubkpi = new HashSet<>();
      // Map<String, Set<String>> map = new HashMap<>();
      // timeShiftWithSubkpi = getTimeShiftSubkpiWithKPI(timeShiftedKPIs,
      // timeShiftWithSubkpi, map, jobContext);
      // logger.error("Time Shift With Sub KPI: {}", timeShiftWithSubkpi);

      // Set<String> timeShiftkpiIds = timeShiftWithSubkpi.stream().filter(e ->
      // !e.contains("TimeShift"))
      // .collect(Collectors.toSet());
      // logger.error("TimeShift KPI IDs={} ", timeShiftkpiIds);

      // UPTO HERE

      Map<String, Map<String, String>> nodeTimeAggrMap = getNodeTimeAggrList(counterDetailsMap, jobContext);

      Map<String, Map<String, String>> counterInfoMap = new LinkedHashMap<>();

      counterInfoMap = getCounterInfoMap(jobContext, reportWidgetDetailsMap,
            kpiCodeCommaSeparated);

      logger.error("Node Time Aggr Map: {}", nodeTimeAggrMap);
      logger.error("Counter Info Map: {}", counterInfoMap);

      String COUNTER_INFO_MAP_JSON = new ObjectMapper().writeValueAsString(counterInfoMap);
      jobContext.setParameters("COUNTER_INFO_MAP", COUNTER_INFO_MAP_JSON);

      String CATEGORY_LIST = counterInfoMap.entrySet().stream().map(e -> e.getValue().get("CATEGORY_NAME"))
            .distinct().collect(Collectors.joining(","));

      logger.error("Category List: {}", CATEGORY_LIST);
      jobContext.setParameters("CATEGORY_LIST", CATEGORY_LIST);

      Map<String, List<Map<String, String>>> catgoryInfoMap = getCatgoryInfoMap(counterInfoMap);
      logger.error("Catgory Info Map: {}", catgoryInfoMap);

      String CATEGORY_INFO_MAP_JSON = new ObjectMapper().writeValueAsString(catgoryInfoMap);
      jobContext.setParameters("CATEGORY_INFO_MAP", CATEGORY_INFO_MAP_JSON);

      getPMCounterVariableAggrQuery(jobContext, frequency, timeKeysCommaSeparated, nodeTimeAggrMap);

      String kpiCodesCommaSeparated = kpiCodeList.stream().collect(Collectors.joining(","));
      logger.error("KPI Codes Comma Separated: {}", kpiCodesCommaSeparated);

      Map<String, Map<String, String>> kpiFormulaFinalMap = getKpiFormulaMap(kpiCodesCommaSeparated,
            reportWidgetDetailsMap, jobContext);

      logger.error("KPI Formula Final Map: {}", kpiFormulaFinalMap);
      String KPI_FORMULA_MAP_JSON = new ObjectMapper().writeValueAsString(kpiFormulaFinalMap);
      jobContext.setParameters("KPI_FORMULA_MAP", KPI_FORMULA_MAP_JSON);

      String finalCounterInfo = counterInfoMap.entrySet().stream()
            .map(e -> "C" + e.getValue().get("SEQUENCE_NO") + "#" + e.getValue().get("PM_COUNTER_VARIABLE_ID_PK"))
            .distinct().collect(Collectors.joining(","));

      logger.error("Final Counter Info: {}", finalCounterInfo);

      jobContext.setParameters("FINAL_COUNTER_INFO", finalCounterInfo);

      return this.dataFrame;
   }

   private String updateAggregationLevel(String aggregationLevel, JobContext jobContext,
         Map<String, String> reportWidgetDetailsMap) {

      String configuration = reportWidgetDetailsMap.get("CONFIGURATION");
      String fixedJson = configuration.replace("'", "\"");
      JSONObject jsonObject = new JSONObject(fixedJson);

      JSONArray moHierarchy = jsonObject.getJSONArray("moHierarchy");
      for (int i = 0; i < moHierarchy.length(); i++) {
         JSONObject moHierarchyObject = moHierarchy.optJSONObject(i);
         if (moHierarchyObject == null) {
            continue;
         }
         String subCatHeader = moHierarchyObject.optString("subCatHeader", "");
         String subCatValue = moHierarchyObject.optString("subCatValue", "");
         String subCategoryIndex = moHierarchyObject.optString("subCategoryIndex", "");
         logger.error("Sub Cat Header: {}, Sub Cat Value: {}, Sub Category Index: {}", subCatHeader, subCatValue,
               subCategoryIndex);

         if (subCategoryIndex.equalsIgnoreCase("1") && subCatValue.contains("INDIVIDUAL")) {
            aggregationLevel = "NAM";
            logger.error("Aggregation Level Updated To: {}", aggregationLevel);
            break;
         } else if (subCategoryIndex.equalsIgnoreCase("1") && !subCatValue.contains("INDIVIDUAL")) {
            aggregationLevel = "H1";
            logger.error("Aggregation Level Updated To: {}", aggregationLevel);
            break;
         }
      }
      return aggregationLevel;
   }

   private Set<String> getTimeShiftSubkpiWithKPI(Set<String> timeShiftKPI, Set<String> kpiWithSubKPI,
         Map<String, Set<String>> map, JobContext jobContext) {

      logger.error("Getting Time Shift KPI with Sub KPI: Time Shift KPI={}, KPI With Sub KPI={}, Map={}", timeShiftKPI,
            kpiWithSubKPI, map);

      Map<String, String> contextMap = jobContext.getParameters();
      Connection connection = null;
      PreparedStatement preparedStatement = null;
      ResultSet resultSet = null;

      Set<String> subkpi = new HashSet<>();
      String timeShiftKPIs = Joiner.on(",").join(timeShiftKPI);

      String query = "SELECT KPI_FORMULA_DESC, KPI_CODE FROM KPI_FORMULA WHERE KPI_CODE IN ('"
            + timeShiftKPIs.replace(",", "','") + "')";

      logger.error("MySQL Query To Get TimeShift KPI With Sub KPI: {}", query);
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
               logger.error("KPI Formula Desc: {}", resultSet.getString(1));
               subkpi = getSubKPICodeForTimeShift(resultSet.getString(1));
               logger.error("Sub KPI: {}", subkpi);
               kpiWithSubKPI.addAll(subkpi);
               logger.error("Added KPI With Sub KPI: {}", kpiWithSubKPI);
               getPossibleSubKpiListForTimeShift1(map, kpiWithSubKPI, resultSet.getString(2), subkpi);
               logger.error("KPI With Sub KPI: {}", kpiWithSubKPI);
            }
         }
         if (!subkpi.isEmpty()) {
            getTimeShiftMap(map, subkpi);
            logger.error("Time Shift Map: {}", map);
         }
         if (subkpi != null && !subkpi.isEmpty()) {
            logger.error(
                  "Sub KPI is not Empty, Getting Time Shift KPI with Sub KPI: Sub KPI={}, KPI With Sub KPI={}, Map={}",
                  subkpi, kpiWithSubKPI, map);
            kpiWithSubKPI = getTimeShiftSubkpiWithKPI(subkpi, kpiWithSubKPI, map, jobContext);
            logger.error("KPI With Sub KPI: {}", kpiWithSubKPI);
         }
         kpiWithSubKPI.addAll(timeShiftKPI);
      } catch (Exception e) {
         logger.error("Exception While Getting TimeShift KPI with Sub KPI, Message={}, Error={}", e.getMessage(), e);
      } finally {
         close(connection, preparedStatement, resultSet);
      }
      return kpiWithSubKPI;
   }

   private void getTimeShiftMap(Map<String, Set<String>> map, Set<String> subKpiListForTimeShift) {
      logger.error("Getting Time Shift Map: Map={}, Sub KPI List For Time Shift={}", map, subKpiListForTimeShift);
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
      logger.error("Time Shift Map: {}", map);
   }

   private void getPossibleSubKpiListForTimeShift1(Map<String, Set<String>> map, Set<String> subKpiListForTimeShift,
         String kpiCode, Set<String> temp) {
      logger.error(
            "Getting Possible Sub KPI List For Time Shift: Map={}, Sub KPI List For Time Shift={}, KPI Code={}, Temp={}",
            map, subKpiListForTimeShift, kpiCode, temp);
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

   // public static Set<String> getSubKPICodeForTimeShift(String kpiFormulaDesc) {
   // logger.error("Getting Sub KPI Code For Time Shift: KPI Formula Desc={}",
   // kpiFormulaDesc);
   // Set<String> kpicodeList = new HashSet<>();
   // if (kpiFormulaDesc.contains("KPI#")) {
   // kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI"))
   // .filter(e -> e.contains("TimeShift") && e.contains("#"))
   // .map(e -> StringUtils.substringBetween(e, "#", ")")
   // + ")).TimeShift("
   // + StringUtils.substringBetween(e, "TimeShift(", ")"))
   // .distinct().collect(Collectors.toSet()));
   // kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI"))
   // .filter(e -> e.contains(")") && e.contains("#"))
   // .map(e -> StringUtils
   // .substringBefore(StringUtils.substringBefore(e, ")"), ".")
   // .replace("#", ""))
   // .distinct().collect(Collectors.toSet()));
   // }
   // logger.error("Sub KPI Code For Time Shift: {}", kpicodeList);
   // return kpicodeList;
   // }
   public static Set<String> getSubKPICodeForTimeShift(String kpiFormulaDesc) {
      logger.error("Getting Sub KPI Code For Time Shift: KPI Formula Desc={}", kpiFormulaDesc);
      Set<String> kpicodeList = new HashSet<>();
      if (kpiFormulaDesc.contains("KPI#")) {
         kpicodeList.addAll(Arrays.stream(kpiFormulaDesc.split("KPI#"))
               .filter(e -> e.contains("TimeShift")) // ensure it's a timeshift KPI
               .map(e -> StringUtils.substringBefore(e, ")")) // take until closing bracket
               .map(e -> e.replaceAll("[^0-9]", "")) // keep only digits
               .filter(e -> !e.isEmpty())
               .collect(Collectors.toSet()));
      }
      logger.error("Sub KPI Code For Time Shift: {}", kpicodeList);
      return kpicodeList;
   }

   private Set<String> getNormalSubkpiWithKPI(Set<String> kpiCode, Set<String> kpiWithSubKPI,
         Set<String> timeShiftKPI, JobContext jobContext) {

      logger.error("Getting Normal KPI with Sub KPI: KPI Code={}, KPI With Sub KPI={}, Time Shift KPI={}", kpiCode,
            kpiWithSubKPI, timeShiftKPI);

      Map<String, String> contextMap = jobContext.getParameters();
      Connection connection = null;
      PreparedStatement preparedStatement = null;
      ResultSet resultSet = null;

      String kpiCodes = Joiner.on(",").join(kpiCode);
      Set<String> subkpi = new HashSet<>();

      String query = "SELECT KPI_FORMULA_DESC, KPI_CODE FROM KPI_FORMULA WHERE KPI_CODE IN ('"
            + kpiCodes.replace(",", "','")
            + "')";

      logger.error("MySQL Query To Get Sub KPI Formula Desc With KPI: {}", query);
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
         logger.error("Sub KPI: {}", subkpi);
         logger.error("KPI With Sub KPI: {}", kpiWithSubKPI);
         logger.error("Time Shift KPI: {}", timeShiftKPI);
         if (subkpi != null && !subkpi.isEmpty()) {
            logger.error(
                  "Sub KPI is not Empty, Getting Normal KPI with Sub KPI: Sub KPI={}, KPI With Sub KPI={}, Time Shift KPI={}",
                  subkpi,
                  kpiWithSubKPI, timeShiftKPI);
            kpiWithSubKPI = getNormalSubkpiWithKPI(subkpi, kpiWithSubKPI, timeShiftKPI, jobContext);
         }
         kpiWithSubKPI.addAll(Arrays.asList(kpiCodes.split(",")));
      } catch (Exception e) {
         logger.error("Exception While Getting Normal KPI with Sub KPI, Message={}, Error={}", e.getMessage(), e);
      } finally {
         close(connection, preparedStatement, resultSet);
      }
      return kpiWithSubKPI;
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
      return kpicodeList;
   }

   private void getNormalAndTimeShiftKPIs(Set<String> kpiCode, Set<String> normalKPIs, Set<String> timeShiftedKPIs,
         JobContext jobContext) {

      logger.error("Getting Normal And TimeShift KPIs: KPI Code={}, Normal KPIs={}, TimeShift KPIs={}", kpiCode,
            normalKPIs, timeShiftedKPIs);

      Map<String, String> contextMap = jobContext.getParameters();
      Connection connection = null;
      PreparedStatement preparedStatement = null;
      ResultSet resultSet = null;

      String kpiCodes = Joiner.on(",").join(kpiCode);
      String query = "SELECT KPI_FORMULA_DESC, KPI_CODE FROM KPI_FORMULA WHERE KPI_CODE IN ('"
            + kpiCodes.replace(",", "','")
            + "')";

      logger.error("MySQL Query To Get KPI Formula Desc: {}", query);

      try {
         final String jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
         final String jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
         final String jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
         final String jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

         logger.error("JDBC Driver={}, JDBC URL={}, JDBC Username={}, JDBC Password={}", jdbcDriver, jdbcUrl,
               jdbcUsername, jdbcPassword);

         Class.forName(jdbcDriver);
         connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
         preparedStatement = connection.prepareStatement(query);
         resultSet = preparedStatement.executeQuery();
         while (resultSet.next()) {
            if (!StringUtils.isEmpty(resultSet.getString(1)) && resultSet.getString(1).contains("TimeShift")) {
               timeShiftedKPIs.add(resultSet.getString(2));
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
   }

   private static void close(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
      if (resultSet != null) {
         try {
            resultSet.close();
         } catch (SQLException e) {
         }
      }
      if (preparedStatement != null) {
         try {
            preparedStatement.close();
         } catch (SQLException e) {
         }
      }
      if (connection != null) {
         try {
            connection.close();
         } catch (SQLException e) {
         }
      }
   }

   public static Map<String, List<String>> getKpiGroupMap(String configuration) {
      Map<String, List<String>> kpiGroupMap = new HashMap<>();

      // Ensure Valid JSON By Replacing Single Quotes With Double Quotes
      String fixedJson = configuration.replace("'", "\"");
      JSONObject jsonObject = new JSONObject(fixedJson);

      // Early Return For CSV Report Format
      String reportFormatType = jsonObject.optString("reportFormatType", "").trim();
      if (reportFormatType.equalsIgnoreCase("csv")) {
         return kpiGroupMap;
      }

      // Proceed For Excel Report Format
      JSONArray kpiArray = jsonObject.optJSONArray("kpi");
      if (kpiArray == null) {
         return kpiGroupMap;
      }

      // Temporary Map To Hold HeaderName → Group
      Map<String, String> individualKpiGroupMap = new LinkedHashMap<>();

      for (int i = 0; i < kpiArray.length(); i++) {
         JSONObject kpiObject = kpiArray.getJSONObject(i);

         // String type = kpiObject.optString("type", "").trim();
         // if (type.equalsIgnoreCase("KPI")) {
         // Try HeaderName First, Fallback To KpiName
         String headerName = kpiObject.optString("headerName", "").trim();
         if (headerName.isEmpty()) {
            headerName = kpiObject.optString("kpiName", "N/A").trim();
         }

         String kpiGroup = kpiObject.optString("kpigroup", "OTHER").trim();
         if (kpiGroup.isEmpty()) {
            kpiGroup = "OTHER";
         }

         individualKpiGroupMap.put(headerName, kpiGroup);
         // }
      }

      // Group HeaderNames By Group Key
      for (Map.Entry<String, String> entry : individualKpiGroupMap.entrySet()) {
         String headerName = entry.getKey();
         String group = entry.getValue();

         kpiGroupMap.computeIfAbsent(group, k -> new ArrayList<>()).add(headerName);
      }

      logger.error("Individual KPI Group Map: {}", individualKpiGroupMap);
      logger.error("Distinct Groups: {}", kpiGroupMap.keySet());
      logger.error("KPI Group Map: {}", kpiGroupMap);

      return kpiGroupMap;
   }

   public static Map<String, Map<String, String>> getRagConfiguration(String configuration) {

      String fixedJson = configuration.replace("'", "\"");
      Map<String, Map<String, String>> ragConfigurationMap = new HashMap<>();
      JSONObject jsonObject = new JSONObject(fixedJson);
      JSONArray kpiArray = jsonObject.getJSONArray("kpi");

      for (int i = 0; i < kpiArray.length(); i++) {
         JSONObject kpiObject = kpiArray.getJSONObject(i);
         String kpiCode = kpiObject.getString("kpicode");

         JSONArray colorInputRangeConfigArray = kpiObject.getJSONArray("colorInputRangeConfig");
         if (!ragConfigurationMap.containsKey(kpiCode)) {
            ragConfigurationMap.put(kpiCode, new HashMap<>());
         }

         Map<String, String> conditionMap = ragConfigurationMap.get(kpiCode);

         for (int j = 0; j < colorInputRangeConfigArray.length(); j++) {
            JSONObject config = colorInputRangeConfigArray.getJSONObject(j);
            String selectedCondition = config.getString("selectedCondition");

            String condition;
            String selectedHexColor;

            if (selectedCondition.equals("inRange")) {
               JSONArray inputRangeArray = config.getJSONArray("inputRange");
               for (int k = 0; k < inputRangeArray.length(); k++) {
                  JSONObject range = inputRangeArray.getJSONObject(k);
                  String inputFrom = range.getString("inputFrom");
                  String inputTo = range.getString("inputTo");
                  selectedHexColor = range.getString("selectedHexColor");

                  condition = "(KPI#" + kpiCode + " >= " + inputFrom + " && KPI#" + kpiCode + " <= " + inputTo
                        + ")";
                  conditionMap.put(condition, selectedHexColor);
               }
            } else {
               String inputFilter = config.optString("inputFilter", null);
               if (inputFilter == null || inputFilter.isEmpty() || inputFilter.equalsIgnoreCase("null")) {
                  continue;
               }

               selectedHexColor = config.getString("selectedHexColor");

               String operator = switch (selectedCondition) {
                  case "equals" -> " = ";
                  case "notEquals" -> " != ";
                  case "gt" -> " > ";
                  case "gte" -> " >= ";
                  case "lt" -> " < ";
                  case "lte" -> " <= ";
                  default -> null;
               };

               if (operator != null) {
                  condition = "(KPI#" + kpiCode + operator + inputFilter + ")";
                  conditionMap.put(condition, selectedHexColor);
               }
            }
         }
      }

      return ragConfigurationMap;
   }

   private static Map<String, Map<String, String>> getNodeTimeAggrList(Map<String, String> nodeAndAggregationDetailsMap,
         JobContext jobContext) {
      Map<String, Map<String, String>> result = new HashMap<>();

      if (nodeAndAggregationDetailsMap == null || nodeAndAggregationDetailsMap.isEmpty()) {
         return result;
      }

      for (Map.Entry<String, String> entry : nodeAndAggregationDetailsMap.entrySet()) {
         String counterId = entry.getKey();
         String valueString = entry.getValue();

         Map<String, String> valueMap = new HashMap<>();
         try {
            String[] parts = valueString.split("##");
            for (String part : parts) {
               String[] keyValue = part.split("=", 2);
               if (keyValue.length == 2) {
                  String key = keyValue[0].trim();
                  String value = keyValue[1].trim();

                  if ("NODE_AGGREGATION".equalsIgnoreCase(key)) {
                     valueMap.put("NODE_AGGREGATION", value);
                  }
                  if ("TIME_AGGREGATION".equalsIgnoreCase(key)) {
                     valueMap.put("TIME_AGGREGATION", value);
                  }
               }
            }
            if (!valueMap.isEmpty()) {
               result.put(counterId, valueMap);
            }
         } catch (Exception e) {
            logger.error("Error Parsing Entry For CounterId: {} | {}", counterId, e.getMessage());
         }
      }

      return result;
   }

   private static void proceedWithOnlyCounterNodeAndTimeAggregation(JobContext jobContext,
         String timeKeysCommaSeparated) {

      String frequency = jobContext.getParameter("FREQUENCY");

      logger.error("Get PM Counter Variable Aggr Query, With Frequency={}", frequency);

      StringBuilder NODE_AGGREGATION_QUERY_BUILDER = new StringBuilder();
      StringBuilder COUNTER_WITH_NODE_AGGR_BUILDER = new StringBuilder();
      StringBuilder COUNTER_WITH_TIME_AGGR_BUILDER = new StringBuilder();
      StringBuilder FILTER_QUERY_BUILDER = new StringBuilder();

      COUNTER_WITH_NODE_AGGR_BUILDER.append(
            " SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, finalKey, categoryname, NAM, FIRST_VALUE(metaData) AS metaData, ");

      StringBuilder mapQuery = new StringBuilder();
      String COUNTER_QUERY_MAP = null;

      Set<String> fiveMinuteKeys = Set.of("5 MIN", "FIVEMIN");
      Set<String> quarterKeys = Set.of("15 MIN", "QUARTERLY");
      Set<String> dateKeys = Set.of("DAILY", "PERDAY");
      Set<String> hourKeys = Set.of("HOURLY", "PERHOUR");

      String timeKey = "";
      String upperFreq = frequency.toUpperCase();
      if (fiveMinuteKeys.contains(upperFreq)) {
         timeKey = "fiveMinuteKey ";
      } else if (quarterKeys.contains(upperFreq)) {
         timeKey = "quarterKey ";
      } else if (dateKeys.contains(upperFreq)) {
         timeKey = "dateKey ";
      } else if (hourKeys.contains(upperFreq)) {
         timeKey = "hourKey ";
      } else {
         timeKey = "dateKey ";
      }

      String counterDetailsMapJson = jobContext.getParameter("COUNTER_DETAILS");

      List<Map<String, String>> counterDetailsList = getCounterDetailsList(counterDetailsMapJson);

      logger.error("Counter Details List: {}", counterDetailsList);

      Map<String, Map<String, String>> counterInfoMap = new HashMap<>();

      for (Map<String, String> counterDetails : counterDetailsList) {
         String kpiCounterIdPk = counterDetails.get("KPI_COUNTER_ID_PK");
         String categoryName = counterDetails.get("CATEGORY_NAME");
         String sequenceNo = counterDetails.get("SEQUENCE_NO");

         String counterKey = "C" + sequenceNo + "#" + kpiCounterIdPk;
         String counterId = counterKey.split("#")[1];
         String nodeAggregation = counterDetails.get("NODE_AGGREGATION");
         String timeAggregation = counterDetails.get("TIME_AGGREGATION");

         Map<String, String> counterInfoSubMap = new HashMap<>();
         counterInfoSubMap.put("SEQUENCE_NO", sequenceNo);
         counterInfoSubMap.put("KPI_COUNTER_ID_PK", kpiCounterIdPk);
         counterInfoMap.put(counterKey, counterInfoSubMap);

         if (!nodeAggregation.isEmpty() && !timeAggregation.isEmpty()) {
            mapQuery.append("'").append(counterKey).append("', `").append(counterKey)
                  .append("`, '").append(counterId).append("', `").append(counterKey).append("`, ");
         }

         logger.error("Generated Map Query={}", mapQuery);

         String date = timeKeysCommaSeparated;

         if (!nodeAggregation.isEmpty()) {

            if (nodeAggregation.equalsIgnoreCase("AVG")) {

               COUNTER_WITH_NODE_AGGR_BUILDER.append(nodeAggregation).append("(CASE WHEN UPPER(categoryname) = '")
                     .append(categoryName.toUpperCase())
                     .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                     .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE)  ELSE NULL END) AS `")
                     .append(counterKey).append("`, sum(CASE WHEN UPPER(categoryname) = '")
                     .append(categoryName.toUpperCase())
                     .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                     .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE) ELSE NULL END) AS `S")
                     .append(counterKey).append("`, COUNT(CASE WHEN UPPER(categoryname) = '")
                     .append(categoryName.toUpperCase())
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

               COUNTER_WITH_NODE_AGGR_BUILDER.append(nodeAggregation).append("(CASE WHEN UPPER(categoryname) = '")
                     .append(categoryName.toUpperCase())
                     .append("' AND ").append(timeKey).append(" IN ('").append(date.replace(",", "','"))
                     .append("') THEN CAST(`").append(counterKey).append("` AS DOUBLE) ELSE NULL END) AS `")
                     .append(counterKey).append("`, ");

               NODE_AGGREGATION_QUERY_BUILDER.append(nodeAggregation).append("(`").append(counterKey)
                     .append("`) AS `")
                     .append(counterKey).append("`, ");

               FILTER_QUERY_BUILDER.append("(`").append(counterKey).append("`) AS `").append(counterKey)
                     .append("`, ");

            }

         }

         if (!timeAggregation.isEmpty()) {
            COUNTER_WITH_TIME_AGGR_BUILDER.append(timeAggregation).append("(`").append(counterKey).append("`) AS `")
                  .append(counterKey).append("`,");
         }
      }

      String finalCounterInfo = counterInfoMap.entrySet().stream()
            .map(e -> "C" + e.getValue().get("SEQUENCE_NO") + "#" + e.getValue().get("KPI_COUNTER_ID_PK"))
            .distinct().collect(Collectors.joining(","));

      logger.error("Final Counter Info: {}", finalCounterInfo);
      jobContext.setParameters("FINAL_COUNTER_INFO", finalCounterInfo);

      logger.error("COUNTER_WITH_NODE_AGGR_BUILDER={}", COUNTER_WITH_NODE_AGGR_BUILDER);
      logger.error("COUNTER_WITH_TIME_AGGR_BUILDER={}", COUNTER_WITH_TIME_AGGR_BUILDER);
      logger.error("NODE_AGGREGATION_QUERY_BUILDER={}", NODE_AGGREGATION_QUERY_BUILDER);
      logger.error("FILTER_QUERY_BUILDER={}", FILTER_QUERY_BUILDER);
      logger.error("MAP_QUERY={}", mapQuery);

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
            + " FROM JOINED_RESULT GROUP BY fiveMinuteKey, quarterKey, finalKey, dateKey, hourKey, NAM, categoryname";

      String COUNTER_TIME_AGGR_QUERY = "SELECT finalKey, FIRST_VALUE(metaData) AS metaData, " + COUNTER_WITH_TIME_AGGR
            + " FROM finalNodeAggrData GROUP BY finalKey ORDER BY finalKey";

      jobContext.setParameters("COUNTER_MAP_QUERY", COUNTER_QUERY_MAP);
      jobContext.setParameters("FILTER_QUERY_FINAL", FILTER_QUERY_FINAL);
      jobContext.setParameters("COUNTER_NODE_AGGR_QUERY", COUNTER_NODE_AGGR_QUERY);
      jobContext.setParameters("COUNTER_TIME_AGGR_QUERY", COUNTER_TIME_AGGR_QUERY);
      jobContext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", RAW_FILE_COUNTER_NODE_AGGR_QUERY);

      logger.error("COUNTER_MAP_QUERY={}", COUNTER_QUERY_MAP);
      logger.error("FILTER_QUERY_FINAL={}", FILTER_QUERY_FINAL);
      logger.error("COUNTER_NODE_AGGR_QUERY={}", COUNTER_NODE_AGGR_QUERY);
      logger.error("COUNTER_TIME_AGGR_QUERY={}", COUNTER_TIME_AGGR_QUERY);
      logger.error("RAW_FILE_COUNTER_NODE_AGGR_QUERY={}", RAW_FILE_COUNTER_NODE_AGGR_QUERY);

   }

   private static List<Map<String, String>> getCounterDetailsList(String counterDetailsMapJson) {
      List<Map<String, String>> counterDetailsList = new ArrayList<>();

      if (counterDetailsMapJson == null || counterDetailsMapJson.trim().isEmpty()) {
         return counterDetailsList;
      }

      try {
         Map<String, String> rawMap = new ObjectMapper().readValue(
               counterDetailsMapJson, new TypeReference<Map<String, String>>() {
               });

         for (Map.Entry<String, String> entry : rawMap.entrySet()) {
            String id = entry.getKey();
            String rawDetails = entry.getValue();

            Map<String, String> detailMap = new HashMap<>();
            detailMap.put("KPI_COUNTER_ID_PK", id);

            if (rawDetails != null && !rawDetails.trim().isEmpty()) {
               String[] keyValuePairs = rawDetails.split("##");
               for (String pair : keyValuePairs) {
                  String[] kv = pair.split("=", 2);
                  if (kv.length == 2) {
                     detailMap.put(kv[0].trim(), kv[1].trim());
                  }
               }
            }

            counterDetailsList.add(detailMap);
         }

      } catch (JsonMappingException e) {
         logger.error("Exception Mapping Counter Details Map: {}", e.getMessage());
      } catch (JsonProcessingException e) {
         logger.error("Exception Processing Counter Details Map: {}", e.getMessage());
      }

      return counterDetailsList;
   }

   private static String getAggregationLevel(Map<String, String> reportWidgetDetailsMap,
         Map<String, String> nodeAndAggregationDetails, JobContext jobContext) {

      logger.error("Report Widget Details Map: {}", reportWidgetDetailsMap);
      logger.error("Node And Aggregation Details: {}", nodeAndAggregationDetails);

      String aggregationLevel = "H1";

      String geoL1 = nodeAndAggregationDetails.get("GEOGRAPHY_L1");
      String geoL2 = nodeAndAggregationDetails.get("GEOGRAPHY_L2");
      String geoL3 = nodeAndAggregationDetails.get("GEOGRAPHY_L3");
      String geoL4 = nodeAndAggregationDetails.get("GEOGRAPHY_L4");
      String node = nodeAndAggregationDetails.get("NODE");
      String mo = nodeAndAggregationDetails.get("MO");

      boolean isGeoL1MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("IS_GEOGRAPHY_L1_MULTI_SELECT"));
      boolean isGeoL2MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("IS_GEOGRAPHY_L2_MULTI_SELECT"));
      boolean isGeoL3MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("IS_GEOGRAPHY_L3_MULTI_SELECT"));
      boolean isGeoL4MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("IS_GEOGRAPHY_L4_MULTI_SELECT"));

      String isNodeMultiSelect = nodeAndAggregationDetails.get("IS_NODE_MULTI_SELECT");
      logger.error("Is Node Multi Select: {}", isNodeMultiSelect);

      if (isNodeMultiSelect.equals("true")) {
         logger.error("CASE 0: CUSTOM NODE SELECTED!!");

          // Added For Managed Objects (MO)
         if (node.contains("CLUBBED") && mo.contains("CLUBBED")) {
            aggregationLevel = "L0";
            jobContext.setParameters("CUSTOM_INDIA_LEVEL", "TRUE");
         } else if (node.contains("INDIVIDUAL") && mo.contains("CLUBBED")) {
            aggregationLevel = "H1";
         } else if (node.contains("INDIVIDUAL") && mo.contains("INDIVIDUAL")) {
            aggregationLevel = "NAM";
         }
         return aggregationLevel;
      }

      jobContext.setParameters("CUSTOM_INDIA_LEVEL", "FALSE");

      if (geoL1.contains("CLUBBED") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 1: CLUBBED, CLUBBED, CLUBBED, CLUBBED, CLUBBED");
         aggregationLevel = "L0";

      } else if (geoL1.contains("CLUBBED") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 2: CLUBBED, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 3: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, CLUBBED");
         aggregationLevel = "L1";
      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 4: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";
      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 5: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED");
         aggregationLevel = "L2";
      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 6: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";
      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 7: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");
         aggregationLevel = "L3";

      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 8: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

         logger.error("CASE 9: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED");
         aggregationLevel = "L4";

      } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 10: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");
         aggregationLevel = "H1";
      } else if (isGeoL1MultiSelect && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 11: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, CLUBBED");
         aggregationLevel = "L1";

      } else if (isGeoL1MultiSelect && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 12: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 13: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED");
         aggregationLevel = "L2";

      } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 14: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 15: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");
         aggregationLevel = "L3";

      } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 16: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

         logger.error("CASE 17: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED");
         aggregationLevel = "L4";

      } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 18: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 19: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, CLUBBED");
         aggregationLevel = "L2";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("CLUBBED")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 20: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 21: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED");
         aggregationLevel = "L3";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 22: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
            && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

         logger.error("CASE 23: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED");
         aggregationLevel = "L3";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
            && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 24: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
            && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

         logger.error("CASE 25: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED");
         aggregationLevel = "L4";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
            && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 26: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect && isGeoL4MultiSelect
            && node.contains("CLUBBED")) {

         logger.error("CASE 27: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED");
         aggregationLevel = "L4";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect && isGeoL4MultiSelect
            && node.contains("INDIVIDUAL")) {

         logger.error("CASE 28: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL");
         aggregationLevel = "H1";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

         logger.error("CASE 29: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED");
         aggregationLevel = "L4";

      } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
            && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

         logger.error("CASE 30: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");
         aggregationLevel = "H1";

      } else {

         logger.error("=========This Case In Not Implemented==========");
         aggregationLevel = "H1";
      }

      // Added For Managed Objects (MO)
      String moCopy = nodeAndAggregationDetails.get("MO");
      if (moCopy != null && !moCopy.isEmpty()) {
         if (moCopy.contains("INDIVIDUAL")) {
            aggregationLevel = "NAM";
         } else if (moCopy.contains("CLUBBED")) {
            aggregationLevel = "H1";
         }
      }

      return aggregationLevel;
   }

   private static String getTimeKeys(String fromDate, String toDate, JobContext jobContext) {

      DateTimeFormatter[] inputFormats = new DateTimeFormatter[] {
            DateTimeFormatter.ofPattern("MMM d,yyyy H:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM dd,yyyy H:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM d,yyyy HH:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM dd,yyyy HH:mm", Locale.ENGLISH)
      };

      String frequency = jobContext.getParameter("FREQUENCY");
      logger.error("Getting Time Key, With From Date={}, To Date={}, Frequency={}", fromDate, toDate, frequency);

      DateTimeFormatter keyFormat = null;

      if (frequency.equalsIgnoreCase("5 MIN") || frequency.equalsIgnoreCase("FIVEMIN")) {
         keyFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
      } else if ("15 Min".equalsIgnoreCase(frequency) ||
            "QUARTERLY".equalsIgnoreCase(frequency)) {
         keyFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
      } else if ("HOURLY".equalsIgnoreCase(frequency) ||
            "PERHOUR".equalsIgnoreCase(frequency)) {
         keyFormat = DateTimeFormatter.ofPattern("yyyyMMddHH");
      } else if ("DAILY".equalsIgnoreCase(frequency) ||
            "PERDAY".equalsIgnoreCase(frequency)) {
         keyFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
      } else {
         keyFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
      }

      LocalDateTime start = parseFlexibleDate(fromDate, inputFormats);
      LocalDateTime end = parseFlexibleDate(toDate, inputFormats);

      List<String> timeKeys = new ArrayList<>();
      while (!start.isAfter(end)) {
         timeKeys.add(start.format(keyFormat));
         if (frequency.equalsIgnoreCase("5 MIN") || frequency.equalsIgnoreCase("FIVEMIN")) {
            start = start.plusMinutes(5);
         } else if ("15 Min".equalsIgnoreCase(frequency) ||
               "QUARTERLY".equalsIgnoreCase(frequency)) {
            start = start.plusMinutes(15);
         } else if ("HOURLY".equalsIgnoreCase(frequency) ||
               "PERHOUR".equalsIgnoreCase(frequency)) {
            start = start.plusHours(1);
         } else if ("DAILY".equalsIgnoreCase(frequency) ||
               "PERDAY".equalsIgnoreCase(frequency)) {
            start = start.plusDays(1);
         } else {
            start = start.plusDays(1);
         }
      }

      return String.join(",", timeKeys);
   }

   private static LocalDateTime parseFlexibleDate(String dateStr, DateTimeFormatter[] formatters) {
      for (DateTimeFormatter formatter : formatters) {
         try {
            return LocalDateTime.parse(dateStr, formatter);
         } catch (Exception e) {
            // Try next format
         }
      }
      throw new IllegalArgumentException("❌ Unable to parse date: " + dateStr);
   }

   private static Map<String, Map<String, String>> getKpiFormulaMap(String kpiCodesCommaSeparated,
         Map<String, String> reportWidgetDetailsMap, JobContext jobContext) {

      Map<String, Map<String, String>> kpiFormulaFinalMap = new LinkedHashMap<>();

      logger.error("Getting KPI Formula Map With KPI IDs={}", kpiCodesCommaSeparated);
      try {

         String kpiFormulaQuery = getKpiFormulaQuery(kpiCodesCommaSeparated, reportWidgetDetailsMap);
         ResultSet resultSet = getResultSet(kpiFormulaQuery, jobContext);

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

   private static String getKpiFormulaQuery(String kpiCodesCommaSeparated, Map<String, String> reportWidgetDetailsMap) {

      String kpiFormulaQuery = "SELECT DISTINCT CONCAT(kf.KPI_CODE, '##', kf.KPI_FORMULA_DESC) AS KPI_CODE_FORMULA, CAST(COALESCE(CONCAT('C', kc.SEQUENCE_NO, '#', pmc.PM_COUNTER_VARIABLE_ID_PK), 'null') AS BINARY) AS BINARY_VALUE, pmc.UNIQUE_STRING AS UNIQUE_STRING FROM KPI_FORMULA kf LEFT JOIN ( FORMULA_COUNTER_MAPPING fcm JOIN PM_COUNTER_VARIABLE pmc ON fcm.PM_COUNTER_VARIABLE_ID_FK = pmc.PM_COUNTER_VARIABLE_ID_PK ) ON fcm.KPI_FORMULA_ID_FK = kf.KPI_FORMULA_ID_PK LEFT JOIN GENERIC_KPI_MAPPING gkm ON kf.KPI_FORMULA_ID_PK = gkm.KPI_FORMULA_ID_FK LEFT JOIN PM_GENERIC_KPI gk ON gkm.PM_GENERIC_KPI_ID_FK = gk.PM_GENERIC_KPI_ID_PK LEFT JOIN KPI_COUNTER kc ON kc.KPI_COUNTER_ID_PK = pmc.KPI_COUNTER_ID_FK WHERE kf.DOMAIN = '$DOMAIN' AND kf.VENDOR = '$VENDOR' AND kf.DELETED = 0";

      kpiFormulaQuery = kpiFormulaQuery.replace("$DOMAIN", reportWidgetDetailsMap.get("DOMAIN"))
            .replace("$VENDOR", reportWidgetDetailsMap.get("VENDOR"))
            .replace("$KPI_CODES", kpiCodesCommaSeparated);

      if (!kpiCodesCommaSeparated.isEmpty()) {
         kpiFormulaQuery = kpiFormulaQuery + " AND kf.KPI_CODE IN ($KPI_CODES)";
         kpiFormulaQuery = kpiFormulaQuery.replace("$KPI_CODES", kpiCodesCommaSeparated);
      }

      return kpiFormulaQuery;

   }

   private void getPMCounterVariableAggrQuery(JobContext jobContext, String frequency, String timeKeysCommaSeparated,
         Map<String, Map<String, String>> nodeTimeAggrMap)
         throws Exception {

      logger.error("Get PM Counter Variable Aggr Query, With Frequency={}", frequency);

      StringBuilder NODE_AGGREGATION_QUERY_BUILDER = new StringBuilder();
      StringBuilder COUNTER_WITH_NODE_AGGR_BUILDER = new StringBuilder();
      StringBuilder COUNTER_WITH_TIME_AGGR_BUILDER = new StringBuilder();
      StringBuilder FILTER_QUERY_BUILDER = new StringBuilder();

      StringBuilder mapQuery = new StringBuilder();
      String COUNTER_QUERY_MAP = null;

      Set<String> fiveMinKeys = Set.of("5 MIN", "FIVEMIN");
      Set<String> quarterKeys = Set.of("15 MIN", "QUARTERLY");
      Set<String> dateKeys = Set.of("DAILY", "PERDAY");
      Set<String> hourKeys = Set.of("HOURLY", "PERHOUR");

      String timeKey = "";
      String upperFreq = frequency.toUpperCase();
      if (fiveMinKeys.contains(upperFreq)) {
         timeKey = "fiveminutekey ";
      } else if (quarterKeys.contains(upperFreq)) {
         timeKey = "quarterKey ";
      } else if (dateKeys.contains(upperFreq)) {
         timeKey = "dateKey ";
      } else if (hourKeys.contains(upperFreq)) {
         timeKey = "hourKey ";
      } else {
         timeKey = "dateKey ";
      }

      String CATEGORY_INFO_MAP_JSON = jobContext.getParameter("CATEGORY_INFO_MAP");
      @SuppressWarnings("unchecked")
      Map<String, List<Map<String, String>>> CATEGORY_INFO_MAP = new ObjectMapper().readValue(CATEGORY_INFO_MAP_JSON,
            Map.class);

      String CATEGORY_LIST = jobContext.getParameter("CATEGORY_LIST");

      logger.error("Category List={}", CATEGORY_LIST);
      logger.error("Category Info Map={}", CATEGORY_INFO_MAP);

      if (upperFreq.equalsIgnoreCase("5 MIN") || upperFreq.equalsIgnoreCase("FIVEMIN")) {
         COUNTER_WITH_NODE_AGGR_BUILDER.append(
               " SELECT fiveminutekey, quarterKey, dateKey, hourKey, finalKey, categoryname, NAM, FIRST_VALUE(metaData) AS metaData, ");
      } else {
         COUNTER_WITH_NODE_AGGR_BUILDER.append(
               " SELECT quarterKey, dateKey, hourKey, finalKey, categoryname, NAM, FIRST_VALUE(metaData) AS metaData, ");
      }

      for (String category : CATEGORY_LIST.split(",")) {

         String categoryName = category.split("@")[0];
         List<Map<String, String>> CATEGORY_INFO_LIST = CATEGORY_INFO_MAP.get(category);
         if (CATEGORY_INFO_LIST == null) {
            continue;
         }

         for (Map<String, String> EACH_CATEGORY_INFO_MAP : CATEGORY_INFO_LIST) {

            String SEQUENCE_NO = EACH_CATEGORY_INFO_MAP.get("SEQUENCE_NO");
            String PM_COUNTER_VARIABLE_ID_PK = EACH_CATEGORY_INFO_MAP.get("PM_COUNTER_VARIABLE_ID_PK");

            String NODE_AGGREGATION = "";
            String TIME_AGGREGATION = "";

            if (nodeTimeAggrMap.containsKey(PM_COUNTER_VARIABLE_ID_PK)) {
               Map<String, String> nodeTimeAggrSubMap = nodeTimeAggrMap.get(PM_COUNTER_VARIABLE_ID_PK);
               NODE_AGGREGATION = nodeTimeAggrSubMap.get("NODE_AGGREGATION");
               TIME_AGGREGATION = nodeTimeAggrSubMap.get("TIME_AGGREGATION");
            } else {
               NODE_AGGREGATION = EACH_CATEGORY_INFO_MAP.get("NODE_AGGREGATION");
               TIME_AGGREGATION = EACH_CATEGORY_INFO_MAP.get("TIME_AGGREGATION");
            }

            logger.error("📊 NODE_AGGREGATION: {}", NODE_AGGREGATION);
            logger.error("📊 TIME_AGGREGATION: {}", TIME_AGGREGATION);

            String counterKey = "C" + SEQUENCE_NO + "#" + PM_COUNTER_VARIABLE_ID_PK;
            String counterId = counterKey.split("#")[1];
            String nodeAggrVal = NODE_AGGREGATION;
            String timeAggrVal = TIME_AGGREGATION;

            if (!nodeAggrVal.isEmpty() && !timeAggrVal.isEmpty()) {
               mapQuery.append("'").append(counterKey).append("', `").append(counterKey)
                     .append("`, '").append(counterId).append("', `").append(counterKey).append("`, ");
            }

            logger.error("Generated Map Query={}", mapQuery);

            String date = timeKeysCommaSeparated;

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

      logger.error("COUNTER_WITH_NODE_AGGR_BUILDER={}", COUNTER_WITH_NODE_AGGR_BUILDER);
      logger.error("COUNTER_WITH_TIME_AGGR_BUILDER={}", COUNTER_WITH_TIME_AGGR_BUILDER);
      logger.error("NODE_AGGREGATION_QUERY_BUILDER={}", NODE_AGGREGATION_QUERY_BUILDER);
      logger.error("FILTER_QUERY_BUILDER={}", FILTER_QUERY_BUILDER);
      logger.error("MAP_QUERY={}", mapQuery);

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
      String RAW_FILE_COUNTER_NODE_AGGR_QUERY = "";

      RAW_FILE_COUNTER_NODE_AGGR_QUERY = COUNTER_WITH_NODE_AGGR
            + " FROM JOINED_RESULT GROUP BY fiveMinuteKey, quarterKey, finalKey, dateKey, hourKey, NAM, categoryname";

      String COUNTER_TIME_AGGR_QUERY = "SELECT finalKey, FIRST_VALUE(metaData) AS metaData, " + COUNTER_WITH_TIME_AGGR
            + " FROM finalNodeAggrData GROUP BY finalKey ORDER BY finalKey";

      jobContext.setParameters("COUNTER_MAP_QUERY", COUNTER_QUERY_MAP);
      jobContext.setParameters("FILTER_QUERY_FINAL", FILTER_QUERY_FINAL);
      jobContext.setParameters("COUNTER_NODE_AGGR_QUERY", COUNTER_NODE_AGGR_QUERY);
      jobContext.setParameters("COUNTER_TIME_AGGR_QUERY", COUNTER_TIME_AGGR_QUERY);
      jobContext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", RAW_FILE_COUNTER_NODE_AGGR_QUERY);

      logger.error("COUNTER_MAP_QUERY={}", COUNTER_QUERY_MAP);
      logger.error("FILTER_QUERY_FINAL={}", FILTER_QUERY_FINAL);
      logger.error("COUNTER_NODE_AGGR_QUERY={}", COUNTER_NODE_AGGR_QUERY);
      logger.error("COUNTER_TIME_AGGR_QUERY={}", COUNTER_TIME_AGGR_QUERY);
      logger.error("RAW_FILE_COUNTER_NODE_AGGR_QUERY={}", RAW_FILE_COUNTER_NODE_AGGR_QUERY);
   }

   private static Map<String, List<Map<String, String>>> getCatgoryInfoMap(
         Map<String, Map<String, String>> counterInfoMap) {
      Map<String, List<Map<String, String>>> catgoryInfoMap = new LinkedHashMap<>();

      for (Map<String, String> value : counterInfoMap.values()) {

         List<Map<String, String>> categoryInfoList = catgoryInfoMap.get(value.get("CATEGORY_NAME"));
         if (categoryInfoList == null) {
            categoryInfoList = new ArrayList<>();
         }
         Map<String, String> categoryInfoMap = new LinkedHashMap<>();
         categoryInfoMap.put("SEQUENCE_NO", value.get("SEQUENCE_NO"));
         categoryInfoMap.put("COUNTER_HEADER_NAME", value.get("COUNTER_HEADER_NAME"));
         categoryInfoMap.put("PM_COUNTER_VARIABLE_ID_PK", value.get("PM_COUNTER_VARIABLE_ID_PK"));
         categoryInfoMap.put("UNIQUE_STRING", value.get("UNIQUE_STRING"));
         categoryInfoMap.put("NODE_AGGREGATION", value.get("NODE_AGGREGATION"));
         categoryInfoMap.put("TIME_AGGREGATION", value.get("TIME_AGGREGATION"));

         // SUB_CATEGORY_HEADER1

         if (value.get("SUB_CATEGORY_HEADER1") != null && !value.get("SUB_CATEGORY_HEADER1").isEmpty()
               && !value.get("SUB_CATEGORY_HEADER1").equalsIgnoreCase("null")) {

            categoryInfoMap.put("SUB_CATEGORY_HEADER1", value.get("SUB_CATEGORY_HEADER1"));
            categoryInfoMap.put("SUB_CATEGORY_VALUE1", value.get("SUB_CATEGORY_VALUE1"));
         }

         // SUB_CATEGORY_HEADER2

         if (value.get("SUB_CATEGORY_HEADER2") != null && !value.get("SUB_CATEGORY_HEADER2").isEmpty()
               && !value.get("SUB_CATEGORY_HEADER2").equalsIgnoreCase("null")) {

            categoryInfoMap.put("SUB_CATEGORY_HEADER2", value.get("SUB_CATEGORY_HEADER2"));
            categoryInfoMap.put("SUB_CATEGORY_VALUE2", value.get("SUB_CATEGORY_VALUE2"));
         }

         // SUB_CATEGORY_HEADER3

         if (value.get("SUB_CATEGORY_HEADER3") != null && !value.get("SUB_CATEGORY_HEADER3").isEmpty()
               && !value.get("SUB_CATEGORY_HEADER3").equalsIgnoreCase("null")) {

            categoryInfoMap.put("SUB_CATEGORY_HEADER3", value.get("SUB_CATEGORY_HEADER3"));
            categoryInfoMap.put("SUB_CATEGORY_VALUE3", value.get("SUB_CATEGORY_VALUE3"));
         }

         // SUB_CATEGORY_HEADER4

         if (value.get("SUB_CATEGORY_HEADER4") != null && !value.get("SUB_CATEGORY_HEADER4").isEmpty()
               && !value.get("SUB_CATEGORY_HEADER4").equalsIgnoreCase("null")) {

            categoryInfoMap.put("SUB_CATEGORY_HEADER4", value.get("SUB_CATEGORY_HEADER4"));
            categoryInfoMap.put("SUB_CATEGORY_VALUE4", value.get("SUB_CATEGORY_VALUE4"));
         }

         categoryInfoList.add(categoryInfoMap);
         catgoryInfoMap.put(value.get("CATEGORY_NAME"), categoryInfoList);
      }
      return catgoryInfoMap;
   }

   public static Map<String, Map<String, String>> getCounterInfoMap(JobContext jobContext,
         Map<String, String> reportWidgetDetailsMap, String commaSeparatedKpiCodes) throws SQLException {

      Map<String, Map<String, String>> counterInfoMap = new LinkedHashMap<>();

      String counterInfoMapQuery = getCounterInfoMapQuery(jobContext, reportWidgetDetailsMap, commaSeparatedKpiCodes);

      logger.error("Counter Info Map Query: {}", counterInfoMapQuery);

      ResultSet resultSet = getResultSet(counterInfoMapQuery, jobContext);

      if (resultSet == null) {
         throw new SQLException("No Data Found in Counter Info Map Query Result Set!");
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

         Map<String, String> categoryInfoMap = counterInfoMap.get(COUNTER_HEADER_NAME);
         if (categoryInfoMap == null) {
            categoryInfoMap = new LinkedHashMap<>();
         }
         categoryInfoMap.put("COUNTER_HEADER_NAME", COUNTER_HEADER_NAME);
         categoryInfoMap.put("PM_COUNTER_VARIABLE_ID_PK", PM_COUNTER_VARIABLE_ID_PK);
         categoryInfoMap.put("CATEGORY_NAME", CATEGORY_NAME);
         categoryInfoMap.put("SEQUENCE_NO", SEQUENCE_NO);
         categoryInfoMap.put("UNIQUE_STRING", UNIQUE_STRING);
         categoryInfoMap.put("NODE_AGGREGATION", NODE_AGGREGATION);
         categoryInfoMap.put("TIME_AGGREGATION", TIME_AGGREGATION);
         prepareCategoryInfoMap(SUBCATEGORY1_VALUE, SUBCAT_HEADER1, categoryInfoMap, "SUB_CATEGORY_HEADER1",
               "SUB_CATEGORY_VALUE1");
         prepareCategoryInfoMap(SUBCATEGORY2_VALUE, SUBCAT_HEADER2, categoryInfoMap, "SUB_CATEGORY_HEADER2",
               "SUB_CATEGORY_VALUE2");
         prepareCategoryInfoMap(SUBCATEGORY3_VALUE, SUBCAT_HEADER3, categoryInfoMap, "SUB_CATEGORY_HEADER3",
               "SUB_CATEGORY_VALUE3");
         prepareCategoryInfoMap(SUBCATEGORY4_VALUE, SUBCAT_HEADER4, categoryInfoMap, "SUB_CATEGORY_HEADER4",
               "SUB_CATEGORY_VALUE4");

         String COUNTER_INFO_MAP_KEY = "C" + SEQUENCE_NO + "#" + PM_COUNTER_VARIABLE_ID_PK;
         counterInfoMap.put(COUNTER_INFO_MAP_KEY, categoryInfoMap);
      }
      return counterInfoMap;
   }

   private static void prepareCategoryInfoMap(String subCategoryValue, String subCategoryHeader,
         Map<String, String> categoryInfoMap,
         String headerColumn, String valueColumn) {
      if (subCategoryValue != null && !subCategoryValue.equalsIgnoreCase("null")
            && !subCategoryValue.contains("INDIVIDUAL") && !subCategoryValue.contains("AGGREGATED")) {
         categoryInfoMap.put(headerColumn, subCategoryHeader);
         if (categoryInfoMap.get(valueColumn) != null && !categoryInfoMap.get(valueColumn).equalsIgnoreCase("null")) {
            subCategoryValue = categoryInfoMap.get(valueColumn) + "','" + subCategoryValue;
            categoryInfoMap.put(valueColumn, subCategoryValue);
         } else {
            categoryInfoMap.put(valueColumn, subCategoryValue);
         }
      }
   }

   private static String getCounterInfoMapQuery(JobContext jobContext, Map<String, String> reportWidgetDetailsMap,
         String commaSeparatedKpiCodes) {

      String counterInfoMapQuery = "SELECT DISTINCT UPPER(REPLACE(cv.COUNTER, ' ', '')) AS COUNTER_HEADER_NAME, cv.PM_COUNTER_VARIABLE_ID_PK AS PM_COUNTER_VARIABLE_ID_PK, UPPER(REPLACE(kc.CATEGORY_ALIAS_NAME, ' ', '')) AS CATEGORY_NAME, cv.ATTRIBUTE AS ATTRIBUTE, cv.SUBCATEGORY1_VALUE AS SUBCATEGORY1_VALUE, cv.SUBCATEGORY2_VALUE AS SUBCATEGORY2_VALUE, cv.SUBCATEGORY3_VALUE AS SUBCATEGORY3_VALUE, cv.SUBCATEGORY4_VALUE AS SUBCATEGORY4_VALUE, CONCAT('C', subcat1.SEQUENCE_NO) AS SUBCAT_HEADER1, CONCAT('C', subcat2.SEQUENCE_NO) AS SUBCAT_HEADER2, CONCAT('C', subcat3.SEQUENCE_NO) AS SUBCAT_HEADER3, CONCAT('C', subcat4.SEQUENCE_NO) AS SUBCAT_HEADER4, kc.SEQUENCE_NO, UPPER(REPLACE(pc.PM_CATEGORY_ID_PK, ' ', '')) AS CATEGORY_ID, cv.UNIQUE_STRING, cv.NODE_AGGREGATION AS NODE_AGGREGATION, cv.TIME_AGGREGATION AS TIME_AGGREGATION FROM KPI_FORMULA kpi INNER JOIN FORMULA_COUNTER_MAPPING map ON kpi.KPI_FORMULA_ID_PK = map.KPI_FORMULA_ID_FK INNER JOIN PM_COUNTER_VARIABLE cv ON cv.PM_COUNTER_VARIABLE_ID_PK = map.PM_COUNTER_VARIABLE_ID_FK INNER JOIN KPI_COUNTER kc ON UPPER(kc.KPI_COUNTER_ID_PK) = UPPER(cv.KPI_COUNTER_ID_FK) INNER JOIN PM_CATEGORY pc ON pc.PM_CATEGORY_ID_PK = cv.PM_CATEGORY_ID_FK LEFT JOIN KPI_COUNTER subcat1 ON subcat1.KPI_COUNTER_ID_PK = cv.KPI_COUNTER1_ID_FK LEFT JOIN KPI_COUNTER subcat2 ON subcat2.KPI_COUNTER_ID_PK = cv.KPI_COUNTER2_ID_FK LEFT JOIN KPI_COUNTER subcat3 ON subcat3.KPI_COUNTER_ID_PK = cv.KPI_COUNTER3_ID_FK LEFT JOIN KPI_COUNTER subcat4 ON subcat4.KPI_COUNTER_ID_PK = cv.KPI_COUNTER4_ID_FK WHERE kpi.DOMAIN = '$DOMAIN' AND kpi.VENDOR = '$VENDOR' AND kpi.TECHNOLOGY = '$TECHNOLOGY'";

      counterInfoMapQuery = counterInfoMapQuery.replace("$DOMAIN", reportWidgetDetailsMap.get("DOMAIN"))
            .replace("$VENDOR", reportWidgetDetailsMap.get("VENDOR"))
            .replace("$TECHNOLOGY", reportWidgetDetailsMap.get("TECHNOLOGY"));

      if (!commaSeparatedKpiCodes.isEmpty()) {
         counterInfoMapQuery = counterInfoMapQuery + " AND kpi.KPI_CODE IN ($KPI_CODES)";
         counterInfoMapQuery = counterInfoMapQuery.replace("$KPI_CODES", commaSeparatedKpiCodes);
      }

      return counterInfoMapQuery;
   }

   private static Map<String, String> getMetaColumnsMap(Map<String, String> extraParametersMap) {

      String metaColumns = extraParametersMap.get("META_COLUMNS");
      if (metaColumns == null || metaColumns.isEmpty()) {
         return Collections.emptyMap();
      }

      Map<String, String> metaColumnsMap = new LinkedHashMap<>();
      String[] metaColumnsArray = metaColumns.split("#");
      String str1 = metaColumnsArray[0];
      String str2 = metaColumnsArray[1];
      String[] str1Array = str1.split(",");
      String[] str2Array = str2.split(",");

      for (int i = 0; i < str1Array.length; i++) {
         metaColumnsMap.put(str1Array[i].toUpperCase().trim(), str2Array[i].trim());
      }

      return metaColumnsMap;
   }

   private static void initializeKPICounterDetails(Map<String, String> reportWidgetDetails,
         Map<String, String> kpiDetailsMap,
         Map<String, String> counterDetailsMap, List<String> kpiCodeList, List<String> counterIdList,
         Map<String, String> kpiCodeWithKpiNameMap, Map<String, String> counterIdWithCounterNameMap) {

      String configuration = reportWidgetDetails.get("CONFIGURATION");
      if (configuration == null || configuration.isEmpty()) {
         return;
      }

      String fixedJson = configuration.replace("'", "\"");
      JSONObject jsonObject = new JSONObject(fixedJson);

      try {
         JSONArray kpiArray = jsonObject.getJSONArray("kpi");

         for (int i = 0; i < kpiArray.length(); i++) {
            JSONObject kpiObject = kpiArray.getJSONObject(i);

            String type = kpiObject.optString("type", "").toUpperCase();

            if (type.equals("KPI")) {

               String kpiCode = kpiObject.optString("kpicode", "N/A").trim();
               String kpiName = kpiObject.optString("kpiName", "N/A").trim();
               String headerName = kpiObject.optString("headerName", "N/A").trim();

               String value = "KPI_CODE=" + kpiCode + "##" + "KPI_NAME=" + kpiName + "##" + "HEADER_NAME=" + headerName;
               kpiCodeList.add(kpiCode);
               kpiCodeWithKpiNameMap.put(kpiCode, kpiName);
               kpiDetailsMap.put(kpiCode, value);

            } else if (type.equals("COUNTER")) {

               String kpiName = kpiObject.optString("kpiName", "N/A").trim();
               String headerName = kpiObject.optString("headerName", "N/A").trim();
               String nodeAggregation = kpiObject.optString("nodeAggregation", "").trim();
               String timeAggregation = kpiObject.optString("timeAggregation", "").trim();
               String sequenceNo = kpiObject.optString("sequenceNo", "").trim();
               String kpigroup = kpiObject.optString("kpigroup", "").trim();
               String id = kpiObject.optString("id", "").trim();
               String categoryId = kpiObject.optString("categoryId", "N/A").trim();
               String categoryName = kpiObject.optString("categoryName", "N/A").trim();

               String value = "COUNTER_NAME=" + kpiName + "##" + "HEADER_NAME=" + headerName + "##"
                     + "NODE_AGGREGATION=" + nodeAggregation + "##" + "TIME_AGGREGATION=" + timeAggregation + "##"
                     + "SEQUENCE_NO=" + sequenceNo + "##" + "KPI_GROUP=" + kpigroup + "##"
                     + "PM_COUNTER_VARIABLE_ID_PK=" + id + "##" + "PM_CATEGORY_ID_PK=" + categoryId + "##"
                     + "CATEGORY_NAME=" + categoryName;

               counterIdList.add(id);
               counterIdWithCounterNameMap.put(id, kpiName);
               counterDetailsMap.put(id, value);

            }

         }

      } catch (Exception e) {
         System.out.println("Error in Extracting KPI List, Message: " + e.getMessage() + ", Error: " + e);
      }

   }

   private static Map<String, String> getExtraParameters(Map<String, String> reportWidgetDetails,
         JobContext jobContext) {

      Map<String, String> extraParameters = new LinkedHashMap<>();

      try {

         String fixedJson = reportWidgetDetails.get("CONFIGURATION").replace("'", "\"");
         JSONObject jsonObject = new JSONObject(fixedJson);

         String metaColumns = jsonObject.optString("metaColumns", "");
         String reportFormatType = jsonObject.optString("reportFormatType", "");
         String toDate = jsonObject.optString("toDate", "");
         String fromDate = jsonObject.optString("fromDate", "");
         String kpiExpression = jsonObject.optString("kpiExpression", "");
         String startTime = convertToExpectedTimeStampFormat(toDate);
         String endTime = convertToExpectedTimeStampFormat(fromDate);
         String utcOffsetInMinute = jsonObject.optString("utcOffsetInMinute", "0");

         String frequency = jsonObject.getJSONArray("frequency").get(0).toString();
         String selectedHeader = jsonObject.getString("selectedHeader");
         jobContext.setParameters("selectedHeader", selectedHeader);

         extraParameters.put("META_COLUMNS", metaColumns);
         extraParameters.put("REPORT_FORMAT_TYPE", reportFormatType);
         extraParameters.put("TO_DATE", toDate);
         extraParameters.put("FROM_DATE", fromDate);
         extraParameters.put("START_TIME", startTime);
         extraParameters.put("END_TIME", endTime);
         extraParameters.put("KPI_EXPRESSION", kpiExpression);
         extraParameters.put("FREQUENCY", frequency);
         extraParameters.put("SELECTED_HEADER", selectedHeader);
         extraParameters.put("UTC_OFFSET_IN_MINUTE", utcOffsetInMinute);

         jobContext.setParameters("FROM_DATE", fromDate);
         jobContext.setParameters("TO_DATE", toDate);
         jobContext.setParameters("UTC_OFFSET_IN_MINUTE", utcOffsetInMinute);

         String isSpecificDate = "false";
         JSONArray specificDate = jsonObject.getJSONArray("specificDate");
         if (specificDate.length() > 0) {
            isSpecificDate = "true";
         }
         extraParameters.put("IS_SPECIFIC_DATE", isSpecificDate);

         if (isSpecificDate.equals("true")) {

            List<Long> specificDateList = new ArrayList<>();

            logger.error("Specific Date Found, Processing Specific Date List!");

            for (int i = 0; i < specificDate.length(); i++) {
               specificDateList.add(specificDate.getLong(i));
            }

            String dateList = specificDateList.stream()
                  .map(String::valueOf)
                  .collect(Collectors.joining(","));

            logger.error("Formatted Date List: {}", dateList);

            extraParameters.put("DATE_LIST", dateList);

         } else {
            logger.error("No Specific Date Found, Using Default Date Range!");
         }

      } catch (Exception e) {
         logger.error("Error in Getting CQL Parameters, Message: {}, Error: {}", e.getMessage(), e);

      }
      return extraParameters;
   }

   private static String getTrinoOrcFilePaths(Map<String, String> extraParametersMap,
         Map<String, String> reportWidgetDetails, JobContext jobContext) {

      String isSpecificDate = extraParametersMap.get("IS_SPECIFIC_DATE");
      if (isSpecificDate != null && isSpecificDate.equals("true")) {
         logger.error("Specific Date Found, Need to Generate Trino ORC File Paths [Not Implemented Yet]!");
         return "";
      }

      String trinoOrcBasePath = jobContext.getParameters().get("BASE_TRINO_ORC_PATH");
      if (trinoOrcBasePath == null || trinoOrcBasePath.isEmpty()) {
         trinoOrcBasePath = "s3a://performance/JOB/ORC/";
      }

      String fromDate = extraParametersMap.get("FROM_DATE");
      String toDate = extraParametersMap.get("TO_DATE");
      String frequency = extraParametersMap.get("FREQUENCY");

      logger.error("Trino ORC Base Path: {}", trinoOrcBasePath);
      logger.error("From Date: {}", fromDate);
      logger.error("To Date: {}", toDate);
      logger.error("Frequency: {}", frequency);

      String domain = reportWidgetDetails.getOrDefault("DOMAIN", "NA");
      String vendor = reportWidgetDetails.getOrDefault("VENDOR", "NA");
      String emstype = reportWidgetDetails.getOrDefault("EMSTYPE", "NA");
      String technology = reportWidgetDetails.getOrDefault("TECHNOLOGY", "NA");

      DateTimeFormatter[] inputFormats = new DateTimeFormatter[] {
            DateTimeFormatter.ofPattern("MMM d,yyyy H:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM dd,yyyy H:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM d,yyyy HH:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM dd,yyyy HH:mm", Locale.ENGLISH)
      };
      DateTimeFormatter folderDateFormat = DateTimeFormatter.ofPattern("yyMMdd");
      DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HHmm");

      LocalDateTime start = parseFlexibleDate(fromDate, inputFormats);
      LocalDateTime end = parseFlexibleDate(toDate, inputFormats);

      String frequencyPartition = "";

      if (frequency.equalsIgnoreCase("FIVEMIN")) {
         frequencyPartition = "FIVE_MIN";
      } else {
         frequencyPartition = "QUARTERLY";
      }

      List<String> pathList = new ArrayList<>();

      while (!start.isAfter(end)) {
         String dateStr = start.format(folderDateFormat);
         String timeStr = start.format(timeFormat);

         String fullPath = String.format(
               "%sfrequency=%s/domain=%s/vendor=%s/emstype=%s/technology=%s/date=%s/time=%s/ptime=*/categoryname=*/",
               trinoOrcBasePath, frequencyPartition, domain, vendor, emstype, technology, dateStr, timeStr);

         pathList.add(fullPath);
         if (frequency.equalsIgnoreCase("5 MIN") || frequency.equalsIgnoreCase("FIVEMIN")) {
            start = start.plusMinutes(5);
         } else {
            start = start.plusMinutes(15);
         }
      }

      logger.error("Updated Trino ORC File Path List Size: {}", pathList.size());
      return String.join(",", pathList);
   }

   private static String convertToExpectedTimeStampFormat(String date) {
      try {
         DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("MMM d,yyyy H:mm");
         DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxx");
         LocalDateTime localDateTime = LocalDateTime.parse(date, inputFormatter);
         return localDateTime.atOffset(ZoneOffset.UTC).format(outputFormatter);
      } catch (Exception e) {
         LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
         DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxx");
         return now.atOffset(ZoneOffset.UTC).format(outputFormatter);
      }
   }

   private static boolean containsNumericIds(JSONArray array) {
      if (array == null || array.isEmpty()) {
         return false;
      }

      for (int i = 0; i < array.length(); i++) {
         try {
            Object value = array.get(i);
            if (value instanceof Number) {
               return true;
            } else if (value instanceof String) {
               String strValue = (String) value;
               if (!strValue.trim().isEmpty()) {
                  try {
                     Long.parseLong(strValue.trim());
                     return true;
                  } catch (NumberFormatException e) {
                  }
               }
            }
         } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error Checking Numeric ID At Index {} | Message: {} | Error: {}", i,
                  e.getMessage(), e);
         }
      }
      return false;
   }

   private static JSONArray getGeographyNamesByIds(JSONArray ids, String level, JobContext jobContext) {
      JSONArray namesArray = new JSONArray();

      if (ids == null || ids.isEmpty()) {
         return namesArray;
      }

      try {
         String tableName = switch (level.toUpperCase()) {
            case "L1" -> "PRIMARY_GEO_L1";
            case "L2" -> "PRIMARY_GEO_L2";
            case "L3" -> "PRIMARY_GEO_L3";
            case "L4" -> "PRIMARY_GEO_L4";
            default -> "PRIMARY_GEO_L1";
         };

         List<String> numericIds = new ArrayList<>();
         for (int i = 0; i < ids.length(); i++) {
            try {
               Object value = ids.get(i);
               if (value instanceof Number) {
                  numericIds.add(String.valueOf(((Number) value).longValue()));
               } else if (value instanceof String) {
                  String strValue = (String) value;
                  if (!strValue.trim().isEmpty()) {
                     try {
                        Long.parseLong(strValue.trim());
                        numericIds.add(strValue.trim());
                     } catch (NumberFormatException e) {
                     }
                  }
               }
            } catch (Exception e) {
               logger.error("Error Parsing Numeric ID at index {} | Message: {} | Error: {}", i, e.getMessage(),
                     e);
            }
         }

         if (numericIds.isEmpty()) {
            logger.error("[ReadConfigUDF] No Numeric IDs Found for Geography Level: {}", level);
            return namesArray;
         }

         String quotedIds = numericIds.stream()
               .map(id -> "'" + id + "'")
               .collect(Collectors.joining(","));

         String query = "SELECT ID, UPPER(GEO_NAME) AS GEO_NAME FROM " + tableName + " WHERE ID IN (" + quotedIds
               + ")";
         logger.error("[ReadConfigUDF] Geography Query For Level {}: {}", level, query);

         ResultSet resultSet = getResultSet(query, jobContext);
         if (resultSet != null) {
            Map<String, String> idToNameMap = new HashMap<>();
            while (resultSet.next()) {
               String id = resultSet.getString("ID");
               String name = resultSet.getString("GEO_NAME");
               idToNameMap.put(id, name);
            }

            for (String id : numericIds) {
               String name = idToNameMap.get(id);
               if (name != null) {
                  namesArray.put(name);
               } else {
                  logger.error("[ReadConfigUDF] No Name Found for Geography ID: {} In Level: {}", id, level);
                  namesArray.put(id);
               }
            }
         }

      } catch (Exception e) {
         logger.error(
               "[ReadConfigUDF] Error in Getting Geography Names By IDs For Level {} | Message: {} | Error: {}",
               level, e.getMessage(), e);
      }

      logger.error("[ReadConfigUDF] Converted {} IDs to {} Names for Geography Level: {}",
            ids.length(), namesArray.length(), level);
      return namesArray;
   }

   private static Map<String, String> getNodeAndAggregationDetails(Map<String, String> reportWidgetDetails,
         JobContext jobContext) {

      Map<String, String> nodeAndAggregationDetails = new LinkedHashMap<>();

      try {
         String fixedJson = reportWidgetDetails.get("CONFIGURATION").replace("'", "\"");
         JSONObject jsonObject = new JSONObject(fixedJson);

         JSONArray geoL1JSONArray = jsonObject.getJSONArray("geography_l1");
         JSONArray geoL2JSONArray = jsonObject.getJSONArray("geography_l2");
         JSONArray geoL3JSONArray = jsonObject.getJSONArray("geography_l3");
         JSONArray geoL4JSONArray = jsonObject.getJSONArray("geography_l4");
         JSONArray nodeArray = jsonObject.getJSONArray("node");
         JSONArray moArray = jsonObject.has("mo")
               ? jsonObject.optJSONArray("mo")
               : new JSONArray(Arrays.asList("CLUBBED"));

         if (containsNumericIds(geoL1JSONArray)) {
            geoL1JSONArray = getGeographyNamesByIds(geoL1JSONArray, "L1", jobContext);
         }
         if (containsNumericIds(geoL2JSONArray)) {
            geoL2JSONArray = getGeographyNamesByIds(geoL2JSONArray, "L2", jobContext);
         }
         if (containsNumericIds(geoL3JSONArray)) {
            geoL3JSONArray = getGeographyNamesByIds(geoL3JSONArray, "L3", jobContext);
         }
         if (containsNumericIds(geoL4JSONArray)) {
            geoL4JSONArray = getGeographyNamesByIds(geoL4JSONArray, "L4", jobContext);
         }

         logger.error("Geo & Node Details => GeoL1: {}, GeoL2: {}, GeoL3: {}, GeoL4: {}, Node: {}, Mo: {}",
               geoL1JSONArray, geoL2JSONArray, geoL3JSONArray, geoL4JSONArray, nodeArray, moArray);

         List<String> geoL1List = getStringListFromArray(geoL1JSONArray);
         List<String> geoL2List = getStringListFromArray(geoL2JSONArray);
         List<String> geoL3List = getStringListFromArray(geoL3JSONArray);
         List<String> geoL4List = getStringListFromArray(geoL4JSONArray);
         List<String> nodeList = getStringListFromArray(nodeArray);
         List<String> moList = getStringListFromArray(moArray);

         geoL1List = geoL1List != null && !geoL1List.isEmpty() ? geoL1List : new ArrayList<String>();
         geoL2List = geoL2List != null && !geoL2List.isEmpty() ? geoL2List : new ArrayList<String>();
         geoL3List = geoL3List != null && !geoL3List.isEmpty() ? geoL3List : new ArrayList<String>();
         geoL4List = geoL4List != null && !geoL4List.isEmpty() ? geoL4List : new ArrayList<String>();
         nodeList = nodeList != null && !nodeList.isEmpty() ? nodeList : new ArrayList<String>();
         moList = moList != null && !moList.isEmpty() ? moList : new ArrayList<String>();

         logger.error(
               "Geo & Node Lists => GeoL1: {}, GeoL2: {}, GeoL3: {}, GeoL4: {}, Node: {}, Mo: {}",
               geoL1List, geoL2List, geoL3List, geoL4List, nodeList, moList);

         String geoL1 = (geoL1List != null && !geoL1List.isEmpty() && geoL1List.get(0) != null)
               ? geoL1List.get(0).toUpperCase()
               : "";

         String geoL1Copy = (geoL1List != null && !geoL1List.isEmpty() && geoL1List.get(0) != null)
               ? geoL1List.get(0)
               : "";

         String geoL2 = (geoL2List != null && !geoL2List.isEmpty() && geoL2List.get(0) != null)
               ? geoL2List.get(0).toUpperCase()
               : "";

         String geoL3 = (geoL3List != null && !geoL3List.isEmpty() && geoL3List.get(0) != null)
               ? geoL3List.get(0).toUpperCase()
               : "";

         String geoL4 = (geoL4List != null && !geoL4List.isEmpty() && geoL4List.get(0) != null)
               ? geoL4List.get(0).toUpperCase()
               : "";

         String node = (nodeList != null && !nodeList.isEmpty() && nodeList.get(0) != null
               && !nodeList.get(0).isEmpty())
                     ? nodeList.get(0).toUpperCase()
                     : "";

         String mo = (moList != null && !moList.isEmpty() && moList.get(0) != null
               && !moList.get(0).isEmpty())
                     ? moList.get(0).toUpperCase()
                     : "";

         logger.error(
               "Geo & Node Info => GeoL1: {}, GeoL2: {}, GeoL3: {}, GeoL4: {}, Node: {}, Mo: {}",
               geoL1, geoL2, geoL3, geoL4, node, mo);

         String netype = jsonObject.getString("netype");
         netype = netype != null && !netype.isEmpty() ? netype.toUpperCase() : "";

         logger.error("Provided Node & Aggragtaion Details: geoL1={}, geoL2={}, geoL3={}, geoL4={}, node={}",
               geoL1, geoL2, geoL3, geoL4, node);

         String polygonName = jsonObject.optString("polygonName", "");

         if (geoL1.contains("CUSTOM") && polygonName.isEmpty()) {
            jobContext.setParameters("CUSTOM_OR_POLYGON_NAME", geoL1Copy);
            nodeAndAggregationDetails.put("CUSTOM_OR_POLYGON_NAME", geoL1Copy);
         } else if (!polygonName.isEmpty()) {
            jobContext.setParameters("CUSTOM_OR_POLYGON_NAME", polygonName);
            nodeAndAggregationDetails.put("CUSTOM_OR_POLYGON_NAME", polygonName);
         }

         nodeAndAggregationDetails.put("GEOGRAPHY_L1", geoL1);
         nodeAndAggregationDetails.put("CUSTOM_OR_POLYGON_NAME", geoL1);
         nodeAndAggregationDetails.put("GEOGRAPHY_L2", geoL2);
         nodeAndAggregationDetails.put("GEOGRAPHY_L3", geoL3);
         nodeAndAggregationDetails.put("GEOGRAPHY_L4", geoL4);
         nodeAndAggregationDetails.put("NODE", node);
         nodeAndAggregationDetails.put("MO", mo);
         nodeAndAggregationDetails.put("NETYPE", netype);
         nodeAndAggregationDetails.put("GEOGRAPHY_L1_LIST", geoL1List.toString());
         nodeAndAggregationDetails.put("GEOGRAPHY_L2_LIST", geoL2List.toString());
         nodeAndAggregationDetails.put("GEOGRAPHY_L3_LIST", geoL3List.toString());
         nodeAndAggregationDetails.put("GEOGRAPHY_L4_LIST", geoL4List.toString());
         nodeAndAggregationDetails.put("NODE_LIST", nodeList.toString());
         nodeAndAggregationDetails.put("MO_LIST", moList.toString());

         String isGeoL1MultiSelect = !geoL1.contains("CLUBBED") && !geoL1.contains("INDIVIDUAL")
               && !geoL1.contains("CUSTOM") && !geoL1.isEmpty() ? "true" : "false";
         String isGeoL2MultiSelect = !geoL2.contains("CLUBBED") && !geoL2.contains("INDIVIDUAL")
               && !geoL2.contains("CUSTOM") && !geoL2.isEmpty() ? "true" : "false";
         String isGeoL3MultiSelect = !geoL3.contains("CLUBBED") && !geoL3.contains("INDIVIDUAL")
               && !geoL3.contains("CUSTOM") && !geoL3.isEmpty() ? "true" : "false";
         String isGeoL4MultiSelect = !geoL4.contains("CLUBBED") && !geoL4.contains("INDIVIDUAL")
               && !geoL4.contains("CUSTOM") && !geoL4.isEmpty() ? "true" : "false";

         logger.error(
               "Geo Multi-Select Flags => GeoL1: {}, GeoL2: {}, GeoL3: {}, GeoL4: {}",
               isGeoL1MultiSelect, isGeoL2MultiSelect, isGeoL3MultiSelect, isGeoL4MultiSelect);

         nodeAndAggregationDetails.put("IS_GEOGRAPHY_L1_MULTI_SELECT", isGeoL1MultiSelect);
         nodeAndAggregationDetails.put("IS_GEOGRAPHY_L2_MULTI_SELECT", isGeoL2MultiSelect);
         nodeAndAggregationDetails.put("IS_GEOGRAPHY_L3_MULTI_SELECT", isGeoL3MultiSelect);
         nodeAndAggregationDetails.put("IS_GEOGRAPHY_L4_MULTI_SELECT", isGeoL4MultiSelect);

         if (geoL1.equalsIgnoreCase("CUSTOM")) {

            JSONArray nodeNameArray = jsonObject.getJSONArray("cells");
            List<String> nodeNameList = getStringListFromArray(nodeNameArray);
            if (nodeNameList.isEmpty()) {
               nodeNameList.add("NO_NODE_PROVIDED");
            }
            String formattedNodes = nodeNameList.stream()
                  .map(name -> "'" + name + "'")
                  .collect(Collectors.joining(","));
            logger.error("Node Info List: {}", formattedNodes);
   
            nodeAndAggregationDetails.put("NODE_NAME_LIST", formattedNodes);

            nodeAndAggregationDetails.put("IS_NODE_MULTI_SELECT", "true");

         } else {
            nodeAndAggregationDetails.put("IS_NODE_MULTI_SELECT", "false");
         }

      } catch (Exception e) {
         logger.error("Error in getting Node and Aggregation Details, Message: {}, Error: {}", e.getMessage(), e);
      }

      return nodeAndAggregationDetails;

   }

   private static ResultSet getResultSet(String query, JobContext jobContext) {

      Map<String, String> jobContextMap = jobContext.getParameters();

      ResultSet resultSet = null;
      try {
         String SPARK_PM_JDBC_DRIVER = jobContextMap.get("SPARK_PM_JDBC_DRIVER");
         String SPARK_PM_JDBC_URL = jobContextMap.get("SPARK_PM_JDBC_URL");
         String SPARK_PM_JDBC_USERNAME = jobContextMap.get("SPARK_PM_JDBC_USERNAME");
         String SPARK_PM_JDBC_PASSWORD = jobContextMap.get("SPARK_PM_JDBC_PASSWORD");

         SPARK_PM_JDBC_DRIVER = SPARK_PM_JDBC_DRIVER != null && !SPARK_PM_JDBC_DRIVER.isEmpty()
               ? SPARK_PM_JDBC_DRIVER
               : FALLBACK_SPARK_PM_JDBC_DRIVER;
         SPARK_PM_JDBC_URL = SPARK_PM_JDBC_URL != null && !SPARK_PM_JDBC_URL.isEmpty() ? SPARK_PM_JDBC_URL
               : FALLBACK_SPARK_PM_JDBC_URL;
         SPARK_PM_JDBC_USERNAME = SPARK_PM_JDBC_USERNAME != null && !SPARK_PM_JDBC_USERNAME.isEmpty()
               ? SPARK_PM_JDBC_USERNAME
               : FALLBACK_SPARK_PM_JDBC_USERNAME;
         SPARK_PM_JDBC_PASSWORD = SPARK_PM_JDBC_PASSWORD != null && !SPARK_PM_JDBC_PASSWORD.isEmpty()
               ? SPARK_PM_JDBC_PASSWORD
               : FALLBACK_SPARK_PM_JDBC_PASSWORD;

         Class.forName(SPARK_PM_JDBC_DRIVER);
         Connection connection = DriverManager.getConnection(
               SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

         Statement statement = connection.createStatement();
         resultSet = statement.executeQuery(query);

      } catch (Exception e) {
         logger.error("Error in getting ResultSet, Message: {}, Error: {}", e.getMessage(), e);
      }
      return resultSet;
   }

   public static List<String> getStringListFromArray(JSONArray array) {

      if (array == null || array.isEmpty()) {
         return Collections.emptyList();
      }

      List<String> list = new ArrayList<>();

      for (int i = 0; i < array.length(); i++) {

         try {
            list.add(array.getString(i));
         } catch (Exception e) {
            logger.error("Error Parsing String from JSONArray At index {} | Message: {} | Error: {}", i,
                  e.getMessage(), e);
         }

      }

      return list;
   }

   private static Map<String, String> getReportWidgetDetailsMap(Row row, JobContext jobContext) {
      Map<String, String> reportWidgetDetailsMap = new LinkedHashMap<>();
      reportWidgetDetailsMap.put("REPORT_WIDGET_ID_PK",
            row.getAs("REPORT_WIDGET_ID_PK") != null ? row.getAs("REPORT_WIDGET_ID_PK").toString() : "");
      reportWidgetDetailsMap.put("GENERATED_REPORT_ID",
            row.getAs("GENERATED_REPORT_ID") != null ? row.getAs("GENERATED_REPORT_ID").toString() : "");
      reportWidgetDetailsMap.put("REPORT_MEASURE",
            row.getAs("REPORT_MEASURE") != null ? row.getAs("REPORT_MEASURE").toString() : "");
      reportWidgetDetailsMap.put("REPORT_NAME",
            row.getAs("REPORT_NAME") != null ? row.getAs("REPORT_NAME").toString() : "");
      reportWidgetDetailsMap.put("DOMAIN", row.getAs("DOMAIN") != null ? row.getAs("DOMAIN").toString() : "");
      reportWidgetDetailsMap.put("VENDOR", row.getAs("VENDOR") != null ? row.getAs("VENDOR").toString() : "");
      reportWidgetDetailsMap.put("TECHNOLOGY",
            row.getAs("TECHNOLOGY") != null ? row.getAs("TECHNOLOGY").toString() : "");
      reportWidgetDetailsMap.put("FREQUENCY", row.getAs("FREQUENCY") != null ? row.getAs("FREQUENCY").toString() : "");
      reportWidgetDetailsMap.put("NODE", row.getAs("NODE") != null ? row.getAs("NODE").toString() : "");
      reportWidgetDetailsMap.put("CONFIGURATION",
            row.getAs("CONFIGURATION") != null ? row.getAs("CONFIGURATION").toString() : "");

      jobContext.setParameters("DOMAIN", reportWidgetDetailsMap.get("DOMAIN"));
      jobContext.setParameters("VENDOR", reportWidgetDetailsMap.get("VENDOR"));
      jobContext.setParameters("TECHNOLOGY", reportWidgetDetailsMap.get("TECHNOLOGY"));

      return reportWidgetDetailsMap;
   }

}
