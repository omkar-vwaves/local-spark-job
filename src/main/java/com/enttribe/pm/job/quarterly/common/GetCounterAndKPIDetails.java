package com.enttribe.pm.job.quarterly.common;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import gnu.trove.map.hash.THashMap;

public class GetCounterAndKPIDetails extends Processor {

	private static final long serialVersionUID = 1L;
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
			.getLogger(GetCounterAndKPIDetails.class);

	private static final ObjectMapper mapper = new ObjectMapper();
	private static Map<String, Map<String, Object>> kpiFormulaFinalMap = null;
	private static final Pattern kpiPattern = Pattern.compile("KPI#(G?[0-9]{0,5})");

	private static String jdbcDriver;
	private static String jdbcUrl;
	private static String jdbcUsername;
	private static String jdbcPassword;

	public GetCounterAndKPIDetails() {
		super();
		logger.info("Initialized GetCounterAndKPIDetails with Default Constructor");
	}

	public GetCounterAndKPIDetails(Integer id, String processorName) {
		super(id, processorName);
		logger.info("Initialized GetCounterAndKPIDetails with ID: {}, ProcessorName: {}", id, processorName);
	}

	static long startTime;
	static {
		startTime = System.currentTimeMillis();
	}

	@Override
	public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

		logger.info("GetCounterAndKPIDetails Execution Started :: ");

		try {
			Map<String, String> contextMap = jobContext.getParameters();
			jdbcDriver = contextMap.get("SPARK_PM_JDBC_DRIVER");
			jdbcUrl = contextMap.get("SPARK_PM_JDBC_URL");
			jdbcUsername = contextMap.get("SPARK_PM_JDBC_USERNAME");
			jdbcPassword = contextMap.get("SPARK_PM_JDBC_PASSWORD");

			logger.info("JDBC Connection Details: Driver={}, URL={}, Username={}, Password={}",
					jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);

			Map<String, String> counterAggrMap = getCounterAggrMap(jobContext, contextMap);

			String kpiDataValue = getAllKpiCode(jobContext, contextMap);

			initializeKPIFormulaMap(jobContext, contextMap);
			String metaDataValue = getKPIDataMetaColumns(jobContext, contextMap);

			initializePolygonNEMap(jobContext);
			initializeCounterVariableAggQuery(jobContext, counterAggrMap);
			initializeCounterVariableSelectQuery(jobContext, counterAggrMap);
			initializeCounterMapQuery(jobContext, counterAggrMap);
			initializeCounterVariableIndex(jobContext, counterAggrMap, contextMap);
			initializeNeTypeRowKeyAppenderMap(jobContext, contextMap);
			// initializeEnbIdMap(jobContext, contextMap);

			String kpis = kpiDataValue + "," + metaDataValue;

			logger.info("Generated ALL_KPI : {}", kpis);
			jobContext.setParameters("ALL_KPI", kpis);

			if (this.dataFrame != null) {
				long count = this.dataFrame.count();
				logger.info("GetCounterAndKPIDetails - Result Row Count Size : {}", count);
			}

			long endTime = System.currentTimeMillis();
			logger.info("GetCounterAndKPIDetails Execution Completed : {} Milliseconds", endTime - startTime);

		} catch (Exception e) {
			logger.error("Exception Occurred During GetCounterAndKPIDetails Execution: {}", e.getMessage(),
					e);
		}

		return this.dataFrame;
	}

	
	private Map<String, String> getCounterAggrMap(JobContext jobcontext, Map<String, String> contextMap) {

		Map<String, String> counterMap = new THashMap<>();

		try {
			String nodes = contextMap.get("NODES");
			String technology = contextMap.get("TECHNOLOGY");
			String vendor = contextMap.get("VENDOR");
			String domain = contextMap.get("DOMAIN");

			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("SELECT pc.PM_COUNTER_VARIABLE_ID_PK, ")
					.append("CONCAT(CONCAT(pc.NODE_AGGREGATION,'#'), pc.TIME_AGGREGATION) AS aggrDetails ")
					.append("FROM PM_COUNTER_VARIABLE pc, PM_CATEGORY c, PM_NODE_VENDOR pn ")
					.append("WHERE pc.PM_Category_Id_FK = c.PM_Category_Id_Pk ")
					.append("AND c.PM_NODE_VENDOR_ID_FK = pn.PM_Node_Vendor_Id_Pk ")
					.append("AND pn.VENDOR = '").append(vendor).append("' ")
					.append("AND pn.DOMAIN = '").append(domain).append("' ")
					.append("AND pc.NODE_AGGREGATION IS NOT NULL AND pc.NODE_AGGREGATION != '' ")
					.append("AND pc.TIME_AGGREGATION IS NOT NULL AND pc.TIME_AGGREGATION != '' ");

			if ("MOTYPE".equalsIgnoreCase(contextMap.get("moType"))) {
				sqlBuilder.append("AND UPPER(pc.UNIQUE_STRING) LIKE '%ECGI_INDIVIDUAL%' ");
			}

			if (nodes != null && !nodes.trim().isEmpty()) {
				if (!nodes.contains("'")) {
					String formattedNodes = Arrays.stream(nodes.split(","))
							.map(String::trim)
							.map(node -> "'" + node + "'")
							.collect(Collectors.joining(","));
					nodes = formattedNodes;
				}
				sqlBuilder.append("AND pn.NODE IN (").append(nodes).append(") ");
			}

			if (technology != null && !technology.isEmpty()) {
				sqlBuilder.append("AND pn.TECHNOLOGY = '").append(technology).append("' ");
			}

			String sqlQuery = sqlBuilder.toString();
			logger.info("COUNTER_AGGREGATION_MAPJSON Query : {}", sqlQuery);

			ResultSet rs = getResultFromPMDatabase(sqlQuery, contextMap);
			if (rs != null) {
				while (rs.next()) {
					counterMap.put(String.valueOf(rs.getInt(1)), rs.getString(2));
				}
			}

			logger.info("COUNTER_AGGREGATION_MAPJSON Size : {}", counterMap.size());

			String counterMapJson = mapper.writeValueAsString(counterMap);
			jobcontext.setParameters("COUNTER_AGGREGATION_MAPJSON", counterMapJson);

		} catch (Exception e) {
			logger.error("Exception While Getting COUNTER_AGGREGATION_MAPJSON : {}", e.getMessage(), e);
		}

		return counterMap;
	}

	private void initializeCounterMapQuery(JobContext jobcontext, Map<String, String> counterAggrMap)
			throws Exception {

		String counterQueryMap = null;

		try {
			StringBuilder mapQuery = new StringBuilder();

			for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
				String key = entry.getKey();
				if (key != null) {
					mapQuery.append("'").append(key).append("', `").append(key).append("`, ");
				}
			}

			if (mapQuery.length() > 0) {
				mapQuery.setLength(mapQuery.length() - 2);
			}

			counterQueryMap = "Map(" + mapQuery + ") as rawcounters";
		} catch (Exception e) {
			logger.error("Exception while getting PMCounterVariable Map Query: {}", e.getMessage(), e);
		}

		logger.info("COUNTER_MAP_QUERY : {}", counterQueryMap);
		jobcontext.setParameters("COUNTER_MAP_QUERY", counterQueryMap);
	}

	private void initializeCounterVariableSelectQuery(JobContext jobcontext, Map<String, String> counterAggrMap)
			throws Exception {

		String mapSelect = null;
		String counterSelect = null;

		try {
			StringBuilder mapQuery = new StringBuilder();
			StringBuilder counterQuery = new StringBuilder();

			for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
				String key = entry.getKey();
				if (key != null) {
					String[] split = entry.getValue().split("#");
					String operation = split[0];

					if ("AVG".equalsIgnoreCase(operation) || "exclude_zero_avg".equalsIgnoreCase(operation)) {
						mapQuery.append("`").append(key).append("`, S").append(key).append(", C").append(key)
								.append(", ");
					} else {
						mapQuery.append("`").append(key).append("`, ");
					}

					counterQuery.append("`").append(key).append("`, ");
				}
			}

			if (mapQuery.length() > 0) {
				mapSelect = mapQuery.substring(0, mapQuery.length() - 2);
			}
			if (counterQuery.length() > 0) {
				counterSelect = counterQuery.substring(0, counterQuery.length() - 2);
			}
		} catch (Exception e) {
			logger.error("Exception while getting PMCounterVariable Query : {}", e.getMessage(), e);
		}

		logger.info("COUNTER_DATA_SELECT_QUERY : {}", counterSelect);
		logger.info("COUNTER_SELECT_QUERY : {}", mapSelect);

		jobcontext.setParameters("COUNTER_DATA_SELECT_QUERY", counterSelect);
		jobcontext.setParameters("COUNTER_SELECT_QUERY", mapSelect);
	}

	private void initializeCounterVariableIndex(JobContext jobcontext, Map<String, String> counterAggrMap,
			Map<String, String> contextMap) {

		LinkedHashMap<String, String> indexMap = new LinkedHashMap<>();
		try {
			int index = "MOTYPE".equalsIgnoreCase(contextMap.get("moType")) ? 9 : 2;

			for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
				String key = entry.getKey();
				if (key != null) {
					indexMap.put(key, String.valueOf(index++));
				}
			}
		} catch (Exception e) {
			logger.error("Exception While Getting Counter Variable Index : {}", e.getMessage(), e);
		}

		logger.info("COUNTER_VARIABLE_INDEX Size : {}", indexMap.size());

		try {
			String json = mapper.writeValueAsString(indexMap);
			jobcontext.setParameters("COUNTER_VARIABLE_INDEX", json);
		} catch (Exception e) {
		}
	}

	private void initializeKPIFormulaMap(JobContext jobcontext, Map<String, String> contextMap) throws Exception {
		Map<String, Map<String, String>> tempKpiFormulaFinalMap = new THashMap<>();
		try {
			String technology = contextMap.get("TECHNOLOGY");
			String domain = contextMap.get("DOMAIN");
			String vendor = contextMap.get("VENDOR");
			String moType = contextMap.get("moType");

			StringBuilder sqlBuilder = new StringBuilder();

			sqlBuilder.append(
					"SELECT CONCAT(KF.KPI_CODE, CASE WHEN GK.CODE IS NOT NULL THEN CONCAT('##', COALESCE(GK.CODE, 'null')) ELSE '' END, '##', KF.KPI_FORMULA_DESC) AS FORMULA, ")
					.append("CAST(COALESCE(PMC.PM_COUNTER_VARIABLE_ID_PK, 'null') AS BINARY) AS PM_COUNTER_VARIABLE_ID_PK, ")
					.append("PMC.UNIQUE_STRING ")
					.append("FROM KPI_FORMULA KF ")
					.append("LEFT JOIN (FORMULA_COUNTER_MAPPING FCM JOIN PM_COUNTER_VARIABLE PMC ON FCM.PM_COUNTER_VARIABLE_ID_FK = PMC.PM_COUNTER_VARIABLE_ID_PK) ")
					.append("ON FCM.KPI_FORMULA_ID_FK = KF.KPI_FORMULA_ID_PK ")
					.append("LEFT JOIN GENERIC_KPI_MAPPING GKM ON KF.KPI_FORMULA_ID_PK = GKM.KPI_FORMULA_ID_FK ")
					.append("LEFT JOIN PM_GENERIC_KPI GK ON GKM.PM_GENERIC_KPI_ID_FK = GK.PM_GENERIC_KPI_ID_PK ")
					.append("WHERE KF.DOMAIN = '").append(domain).append("' ")
					.append("AND KF.VENDOR = '").append(vendor).append("' ")
					.append("AND KF.DELETED = 0 ")
					.append("AND KF.KPI_TYPE = 'REGULAR' ")
					.append("AND KF.KPI_FORMULA_DESC NOT LIKE '%timeshift%' ");

			if (moType != null && moType.equalsIgnoreCase("MOTYPE")) {
				sqlBuilder.append("AND UPPER(PMC.UNIQUE_STRING) LIKE '%ECGI_INDIVIDUAL%' ");
			}

			if (technology != null && !technology.isEmpty()) {
				sqlBuilder.append("AND KF.TECHNOLOGY = '").append(technology).append("' ");
			}

			sqlBuilder.append("UNION ")
					.append("SELECT CONCAT(GKPI.CODE, '##', GKPI.KPI_FORMULA_DESC) AS FORMULA, ")
					.append("'null' AS PM_COUNTER_VARIABLE_ID_PK, ")
					.append("NULL AS UNIQUE_STRING ")
					.append("FROM PM_GENERIC_KPI GKPI ")
					.append("LEFT JOIN GENERIC_KPI_MAPPING GKM ON GKPI.PM_GENERIC_KPI_ID_PK = GKM.PM_GENERIC_KPI_ID_FK ")
					.append("WHERE GKM.PM_GENERIC_KPI_ID_FK IS NULL ")
					.append("AND GKPI.CODE IS NOT NULL ")
					.append("AND GKPI.KPI_FORMULA_DESC IS NOT NULL ")
					.append("AND GKPI.KPI_FORMULA_DESC NOT LIKE '%timeshift%' ")
					.append("AND GKPI.KPI_TYPE = 'REGULAR' ")
					.append("AND GKPI.DOMAIN = '").append(domain).append("' ")
					.append("AND GKPI.TECHNOLOGY IN ('COMMON', '").append(technology).append("') ")
					.append("AND GKPI.DELETED = 0");

			String sql = sqlBuilder.toString();

			logger.info("KPIFORMULA_MAPJSON SQL query: {}", sql);

			ResultSet rs = getResultFromPMDatabase(sql, contextMap);
			while (rs.next()) {
				String formulaKey = rs.getString(1);
				String counterId = rs.getString(2);
				String uniqueString = rs.getString(3);

				Map<String, String> counterUniqueStringMap = tempKpiFormulaFinalMap
						.getOrDefault(formulaKey, new HashMap<>());

				counterUniqueStringMap.put(counterId, uniqueString);
				tempKpiFormulaFinalMap.put(formulaKey, counterUniqueStringMap);
			}
		} catch (Exception e) {
			logger.error("Exception While Getting KPI Formula Map : {}", e.getMessage(), e);
		}

		kpiFormulaFinalMap = updateKpiFormulaMap(tempKpiFormulaFinalMap);

		for (String kpi : kpiFormulaFinalMap.keySet()) {
			Map<String, Object> kpiformulaMap = kpiFormulaFinalMap.get(kpi);
			if (kpiformulaMap != null) {
				String formulaString = (String) kpiformulaMap.get("formulaString");
				Map<String, String> formulaCounterMap = (Map<String, String>) kpiformulaMap.get("formulaCounterMap");

				formulaString = getInnerKPIFormula(formulaString, formulaCounterMap);
				formulaString = formulaString.replace("{", "(")
						.replace("}", ")")
						.replace("\\", "");
				formulaCounterMap.remove("null");

				kpiformulaMap.put("formulaString", formulaString);
				kpiformulaMap.put("formulaCounterMap", formulaCounterMap);
			}
		}

		logger.info("KPIFORMULA_MAPJSON Size : {}", kpiFormulaFinalMap.size());

		try {
			String json = mapper.writeValueAsString(kpiFormulaFinalMap);
			jobcontext.setParameters("KPIFORMULA_MAPJSON", json);
		} catch (Exception e) {
		}
	}

	@SuppressWarnings("unchecked")
	public Map<String, Map<String, Object>> updateKpiFormulaMap(Map<String, Map<String, String>> tempKpiFormulaFinalMap)
			throws Exception {

		Map<String, Map<String, Object>> kpiFormulaFinalMap = new HashMap<>();

		for (Map.Entry<String, Map<String, String>> formulaEntry : tempKpiFormulaFinalMap.entrySet()) {
			String formulaKey = formulaEntry.getKey();

			if (formulaKey != null && formulaKey.contains("##")) {
				String formulaId = formulaKey.substring(0, formulaKey.lastIndexOf("##"));
				String formulaString = formulaKey
						.substring(formulaKey.lastIndexOf("##") + "##".length());
				Map<String, Object> tempMap1 = new HashMap<>();

				Map<String, String> formulaCounterMap = formulaEntry.getValue();

				for (String counterVariable : formulaCounterMap.keySet()) {
					String regex = "(" + formulaCounterMap.get(counterVariable) + ")";
					formulaString = formulaString.replaceAll(regex, "CK" + counterVariable);
				}

				tempMap1.put("formulaString", formulaString);
				tempMap1.put("formulaCounterMap", formulaCounterMap);

				Set<String> genericKpi = new HashSet<>();

				String baseFormulaId = formulaId.contains("##") ? formulaId.substring(0, formulaId.indexOf("##"))
						: formulaId;
				if (kpiFormulaFinalMap.containsKey(baseFormulaId)) {
					Map<String, Object> existingKpi = kpiFormulaFinalMap.get(baseFormulaId);
					Object existingGenericKpi = existingKpi.get("generickpi");

					if (existingGenericKpi != null) {
						genericKpi.addAll((Set<String>) existingGenericKpi);
					}
				}

				if (formulaId.contains("##")) {
					String[] newGenericKpiArray = formulaId.substring(formulaId.indexOf("##") + 2).split("##");
					genericKpi.addAll(Arrays.asList(newGenericKpiArray));
					tempMap1.put("generickpi", genericKpi);
				}

				kpiFormulaFinalMap.put(baseFormulaId, tempMap1);
			}
		}

		return kpiFormulaFinalMap;
	}

	private String getInnerKPIFormula(String formulaString, Map<String, String> formulaCounterMap) {
		try {
			Matcher kpiMatcher = kpiPattern.matcher(formulaString);
			while (kpiMatcher.find()) {
				String kpi = kpiMatcher.group(1);
				Map<String, Object> kpiMap = kpiFormulaFinalMap.get(kpi);
				if (kpiMap == null || !kpiMap.containsKey("formulaString")) {
					continue;
				}

				String formulaStringReplacement = (String) kpiMap.get("formulaString");
				formulaString = formulaString.replace("KPI#" + kpi, formulaStringReplacement);

				if (!formulaString.contains(kpi) &&
						!formulaString.contains("NVL((KPI#" + kpi + "))") &&
						!formulaString.contains("NVL(((KPI#" + kpi + ")))")) {

					Map<String, String> counterMap = (Map<String, String>) kpiMap.get("formulaCounterMap");
					if (counterMap != null) {
						formulaCounterMap.putAll(counterMap);
					}

					return getInnerKPIFormula(formulaString, formulaCounterMap);
				}
			}
		} catch (Exception e) {
			logger.error("Exception While Getting Inner KPI Formula : {}", e.getMessage(), e);
		}
		return formulaString;
	}

	private void initializeNeTypeRowKeyAppenderMap(JobContext jobcontext, Map<String, String> contextMap) {

		Map<String, String> netypeRowKeyAppenderMap = new HashMap<>();
		String parameter = contextMap.get("rowKeyAppender");
		try {
			if (parameter != null && !parameter.equals("null")) {
				for (String netypeEntry : parameter.split("LINE")) {
					String[] entryParts = netypeEntry.split("##");
					if (entryParts.length == 2) {
						String netypes = entryParts[0];
						String appender = entryParts[1];

						for (String netype : netypes.split(",")) {
							netypeRowKeyAppenderMap.put(netype, appender);
						}
					}
				}
				String json = new ObjectMapper().writeValueAsString(netypeRowKeyAppenderMap);

				logger.info("NETYPE_ROW_KEY_APPENDER_MAP Size : {}", netypeRowKeyAppenderMap.size());
				jobcontext.setParameters("netypeRowKeyAppenderMap", json);
			}
		} catch (Exception e) {
			logger.error("Error While Processing rowKeyAppender Parameter: {}", e.getMessage(), e);
		}
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

	private String getAllKpiCode(JobContext jobcontext, Map<String, String> contextMap) {
		String kpiCodeList = "";
		String nodes = contextMap.get("NODES");
		String domain = contextMap.get("DOMAIN");
		String vendor = contextMap.get("VENDOR");
		String technology = contextMap.get("TECHNOLOGY");

		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT GROUP_CONCAT(kpiCode) FROM (")
				.append("SELECT kf.KPI_CODE AS kpiCode FROM KPI_FORMULA kf ")
				.append("WHERE kf.DOMAIN = '").append(domain).append("' ")
				.append("AND kf.VENDOR = '").append(vendor).append("' ")
				.append("AND kf.DELETED = 0");

		if (nodes != null && !nodes.isEmpty()) {
			if (!nodes.contains("'")) {
				String formattedNodes = Arrays.stream(nodes.split(","))
						.map(String::trim)
						.map(node -> "'" + node + "'")
						.collect(Collectors.joining(","));
				sqlBuilder.append(" AND UPPER(kf.NODE) IN (").append(formattedNodes).append(")");
			}
		}

		if (technology != null && !technology.equalsIgnoreCase("null") && !technology.isEmpty()) {
			sqlBuilder.append(" AND kf.TECHNOLOGY = '").append(technology).append("'");
		}

		sqlBuilder.append(" UNION ")
				.append("SELECT gkpi.CODE AS kpiCode FROM PM_GENERIC_KPI gkpi ")
				.append("WHERE gkpi.CODE IS NOT NULL ")
				.append("AND gkpi.DOMAIN = '").append(domain).append("' ")
				.append("AND DELETED = 0) u");

		String sqlQuery = sqlBuilder.toString();
		logger.info("Executing ALL_KPI_CODE Query: {}", sqlQuery);

		try (ResultSet resultSet = getResultFromPMDatabase(sqlQuery, contextMap)) {
			if (resultSet != null && resultSet.next()) {
				kpiCodeList = resultSet.getString(1);
			}
			jobcontext.setParameters("ALL_KPI_CODE", kpiCodeList);
		} catch (SQLException e) {
			logger.error("Error While Retrieving All KPI Codes: {}", e.getMessage(), e);
		}

		logger.info("ALL_KPI_CODE Size : {}", kpiCodeList != null ? kpiCodeList.length() : 0);
		return kpiCodeList;
	}

	private String getKPIDataMetaColumns(JobContext jobcontext, Map<String, String> contextMap) {
		String metaColumns = "";
		String sql = "SELECT VALUE FROM PM_CONFIGURATION WHERE NAME = 'KPI_DATA_META_COLUMNS' AND TYPE = 'JOB'";
		try (ResultSet resultSet = getResultFromPMDatabase(sql, contextMap)) {
			if (resultSet != null && resultSet.next()) {
				metaColumns = resultSet.getString("VALUE");
				jobcontext.setParameters("KPI_DATA_META_COLUMNS", metaColumns);
			}
		} catch (SQLException e) {
			logger.error("Error Retrieving KPI_DATA_META_COLUMNS From PM_CONFIGURATION: {}", e.getMessage(), e);
		}
		logger.info("KPI_DATA_META_COLUMNS: {}", metaColumns);
		return metaColumns;
	}

	private void initializePolygonNEMap(JobContext jobcontext) {
		try {
			Map<String, String> polygonNEMap = new HashMap<>();
			String polygonNEmapJson = new ObjectMapper().writeValueAsString(polygonNEMap);
			jobcontext.setParameters("POLYGON_NE_MAPJSON", polygonNEmapJson);
		} catch (Exception e) {
			logger.error("Unexpected Error While Getting Polygon NE Details: {}", e.getMessage(), e);
		}
	}

	private void initializeCounterVariableAggQuery(JobContext jobContext,
			Map<String, String> counterAggrMap) {

		StringBuilder nodeAggrQuery = new StringBuilder();
		StringBuilder timeAggrQuery = new StringBuilder();
		StringBuilder rawFileNodeAggrQuery = new StringBuilder();

		try {
			for (Map.Entry<String, String> entry : counterAggrMap.entrySet()) {
				String counterKey = entry.getKey();
				String aggrValue = entry.getValue();

				if (counterKey != null && aggrValue != null && aggrValue.contains("#")) {
					String[] aggrParts = aggrValue.split("#");
					String nodeAggr = aggrParts[0];
					String timeAggr = aggrParts[1];

					switch (nodeAggr.toUpperCase()) {
						case "AVG" -> {
							rawFileNodeAggrQuery.append(String.format(
									"CAST(AVG(`%s`) AS Double) AS `%s`, CAST(SUM(`%s`) AS Double) AS `S%s`, CAST(COUNT(`%s`) AS Double) AS `C%s`,",
									counterKey, counterKey, counterKey, counterKey, counterKey, counterKey));
							nodeAggrQuery.append(String.format(
									"CAST(SUM(S%s)/SUM(C%s) AS Double) AS `%s`, CAST(SUM(S%s) AS Double) AS `S%s`, CAST(SUM(C%s) AS Double) AS `C%s`,",
									counterKey, counterKey, counterKey, counterKey, counterKey, counterKey,
									counterKey));
						}

						case "COUNT" -> {
							rawFileNodeAggrQuery.append(String.format(
									"CAST(COUNT(`%s`) AS Double) AS `%s`,", counterKey, counterKey));
							nodeAggrQuery.append(String.format(
									"CAST(SUM(`%s`) AS Double) AS `%s`,", counterKey, counterKey));
						}

						case "EXCLUDE_ZERO_AVG" -> {
							rawFileNodeAggrQuery.append(String.format(
									"CAST(AVG(CASE WHEN `%s`=0 THEN NULL ELSE `%s` END) AS Double) AS `%s`, " +
											"CAST(SUM(`%s`) AS Double) AS `S%s`, " +
											"CAST(COUNT(CASE WHEN `%s`=0 THEN NULL ELSE `%s` END) AS Double) AS `C%s`,",
									counterKey, counterKey, counterKey, counterKey, counterKey, counterKey, counterKey,
									counterKey));
							nodeAggrQuery.append(String.format(
									"CAST(SUM(S%s)/SUM(C%s) AS Double) AS `%s`, " +
											"CAST(SUM(S%s) AS Double) AS `S%s`, " +
											"CAST(SUM(C%s) AS Double) AS `C%s`,",
									counterKey, counterKey, counterKey, counterKey, counterKey, counterKey,
									counterKey));
						}

						default -> {
							rawFileNodeAggrQuery.append(String.format(
									"CAST(%s(`%s`) AS Double) AS `%s`,", nodeAggr, counterKey, counterKey));
							nodeAggrQuery.append(String.format(
									"CAST(%s(`%s`) AS Double) AS `%s`,", nodeAggr, counterKey, counterKey));
						}
					}

					if ("EXCLUDE_ZERO_AVG".equalsIgnoreCase(timeAggr)) {
						timeAggrQuery.append(String.format(
								"CAST(AVG(CASE WHEN `%s`=0 THEN NULL ELSE `%s` END) AS Double) AS `%s`,",
								counterKey, counterKey, counterKey));
					} else {
						timeAggrQuery.append(String.format(
								"CAST(%s(`%s`) AS Double) AS `%s`,", timeAggr, counterKey, counterKey));
					}
				}
			}

			String nodeAggrQueryFinal = removeTrailingComma(nodeAggrQuery.toString());
			String timeAggrQueryFinal = removeTrailingComma(timeAggrQuery.toString());
			String rawFileNodeAggrQueryFinal = removeTrailingComma(rawFileNodeAggrQuery.toString());

			jobContext.setParameters("COUNTER_NODE_AGGR_QUERY", nodeAggrQueryFinal);
			jobContext.setParameters("COUNTER_TIME_AGGR_QUERY", timeAggrQueryFinal);
			jobContext.setParameters("RAW_FILE_COUNTER_NODE_AGGR_QUERY", rawFileNodeAggrQueryFinal);
		} catch (Exception e) {
			logger.error("Failed to Build PMCounterVariable Aggregation Queries : {}", e.getMessage(), e);
		}
	}

	private String removeTrailingComma(String input) {
		return (input != null && input.endsWith(",")) ? input.substring(0, input.length() - 1) : input;
	}

	private ResultSet getResultFromPMDatabase(String sqlQuery, Map<String, String> contextMap) {

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
