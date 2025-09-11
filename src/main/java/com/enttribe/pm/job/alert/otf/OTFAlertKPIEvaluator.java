package com.enttribe.pm.job.alert.otf;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.enttribe.commons.Symbol;
import com.enttribe.commons.lang.MapUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

import gnu.trove.map.hash.THashMap;

import scala.jdk.CollectionConverters;

public class OTFAlertKPIEvaluator implements
		UDF4<String, scala.collection.immutable.Map<String, String>, scala.collection.immutable.Map<String, String>, String, Row>,
		AbstractUDF {

	private static Logger logger = LoggerFactory.getLogger(OTFAlertKPIEvaluator.class);

	private static final long serialVersionUID = 1L;
	private static Map<String, Map<String, String>> kpiFormulaMap = null;
	public JobContext jobContext;

	public OTFAlertKPIEvaluator() {
		super();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Row call(String finalKey, scala.collection.immutable.Map<String, String> rawCounter,
			scala.collection.immutable.Map<String, String> metaData, String frequency) throws Exception {

		logger.info("KPI Evaluator Execution Started!");

		java.util.Map<String, String> rawCountersMap = CollectionConverters.MapHasAsJava(rawCounter).asJava();
		java.util.Map<String, String> metaDataMap = new THashMap<>();
		if (metaData != null) {
			metaDataMap = new THashMap<>(CollectionConverters.MapHasAsJava(metaData).asJava());
		}

		logger.info("Received Raw Counters Map: {} And Meta Data Map: {}", rawCountersMap, metaDataMap);

		Map<String, String> kpiMap = new THashMap<>();
		Map<String, String> exceptionalKpiMap = new THashMap<>();

		String KPI_FORMULA_MAP_JSON = jobContext.getParameter("KPI_FORMULA_MAP");
		kpiFormulaMap = new ObjectMapper().readValue(KPI_FORMULA_MAP_JSON,
				new TypeReference<Map<String, Map<String, String>>>() {
				});

		logger.info("KPI Formula Final Map: {}", kpiFormulaMap);

		if (kpiFormulaMap != null) {
			Iterator<Map.Entry<String, Map<String, String>>> kpiFormula = kpiFormulaMap.entrySet().iterator();
			while (kpiFormula.hasNext()) {
				int count = 0;
				Map.Entry<String, Map<String, String>> formula = kpiFormula.next();
				logger.info("Formula: {}", formula);
				if (formula.getKey() != null && formula.getKey().contains("##")) {

					String formulaKey = formula.getKey();
					Map<String, String> formulaValue = formula.getValue();
					logger.info("Formula Key: {} And Formula Value: {}", formulaKey, formulaValue);

					String formulaId = StringUtils.substringBeforeLast(formulaKey, "##");
					String formulaString = StringUtils.substringAfterLast(formulaKey, "##");

					logger.info("Formula ID: {} And Formula String: {}", formulaId, formulaString);

					String timeShiftKey = StringUtils
							.substringBefore(StringUtils.substringAfter(formulaId, ").TimeShift("), ")");
					if (!timeShiftKey.equalsIgnoreCase("")) {
						timeShiftKey = "ts" + timeShiftKey;
					}

					logger.info("Time Shift Key: {}", timeShiftKey);

					Map<String, String> formulaCounterMap = formulaValue;
					logger.info("Formula Counter Map: {}", formulaCounterMap);

					for (Entry<String, String> formulaCounterEntry : formulaCounterMap.entrySet()) {
						String counterId = formulaCounterEntry.getKey();
						logger.info("Counter ID: {}", counterId);
						try {
							if (!counterId.equalsIgnoreCase("")) {
								String counterKey = formulaCounterEntry.getValue();
								logger.info("Counter Key: {}", counterKey);

								String counterIdWithTimeShift = counterId + timeShiftKey;
								logger.info("Counter ID With Time Shift: {}", counterIdWithTimeShift);

								if (rawCountersMap.get(counterIdWithTimeShift) != null) {

									logger.info("Counter ID With Time Shift: {}", counterIdWithTimeShift);

									String value = String.valueOf(rawCountersMap.get(counterIdWithTimeShift));
									logger.info("Value: {}", value);
									if (!StringUtils.isEmpty(value)) {

										logger.info("Creating Expression: {}", formulaString);
										try {
											formulaString = createExpression(formulaString, counterKey, value);
											logger.info("Formula String: {}", formulaString);
										} catch (Exception e) {
											logger.error("Exception: {}", e.getMessage());
										}
									} else {
										logger.info("Value is Empty: {}", value);
									}
								} else {
									logger.info("Counter ID With Time Shift Not Found: {}", counterIdWithTimeShift);
									formulaString = createNVLExpression(formulaString, counterKey);
									logger.info("Formula String: {}", formulaString);
								}
							}
						} catch (Exception e) {
							logger.error(
									"Exception Occured While Evaluating KPI For Counter ID: {}, Formula String: {}, Raw Counters Map: {}",
									counterId, formulaString, rawCountersMap);
						}
					}
					if (count < 1) {

						logger.info("Replacing Meta Detail: {}", formulaString);
						logger.info("Meta Data Map: {}", metaDataMap);
						try {
							formulaString = replaceMetaDetail(formulaString, metaDataMap);
							logger.info("Replaced Meta Detail: {}", formulaString);
							com.enttribe.sparkrunner.util.Expression expression = new com.enttribe.sparkrunner.util.Expression(
									formulaString, true);
							String kpiValue = expression.eval();
							logger.info("Calculated KPI Value: {}", kpiValue);

							if (formulaId.contains(",")) {
								for (String kpiId : formulaId.split(",")) {
									kpiMap.put(kpiId, kpiValue);
								}
							} else {
								kpiMap.put(formulaId, kpiValue);
							}

						} catch (Exception e) {
							logger.error("Exception Occured While Replacing Meta Detail: {}", e.getMessage());
							if ((e.getMessage() != null || e.getMessage().equalsIgnoreCase("Division by zero!"))
									&& !e.getMessage().contains("Unknown operator")) {
								for (String kpiId : formulaId.split(",")) {
									kpiMap.put(String.valueOf(kpiId), String.valueOf(Double.NaN));
								}
							} else {
								if (formulaString.contains("KPI#") || formulaString.contains("IF")
										|| formulaString.contains("MIN") || formulaString.contains("MAX")
										|| formulaString.contains("FLOOR") || formulaString.contains("DECTOHEX")
										|| formulaString.contains("HEXTODEC") || formulaString.contains("ROUND")
										|| formulaString.contains("CEILING") || formulaString.contains("LEN")
										|| formulaString.contains("RIGHT") || formulaString.contains("LEFT")
										|| formulaString.contains("TimeShift")) {
									for (String kpiId : formulaId.split(",")) {
										exceptionalKpiMap.put(String.valueOf(kpiId), formulaString);
									}
								} else {
									kpiMap.put(String.valueOf(formulaId), formulaString);
								}
							}

						}
					} else {
						logger.info("Count is not less than 1: {}", count);
						logger.info("Formula String: {}", formulaString);
					}
				}
			}
		}
		if (finalKey != null && !finalKey.isEmpty()) {
			logger.info("Setting Date Time For Final Key: {}", finalKey);
			setDateTime(finalKey, metaDataMap, frequency);
		}
		List<String> kpiList = new ArrayList<>();
		List<String> counterIdList = new ArrayList<>();

		String KPI_CODES = jobContext.getParameter("KPI_CODES");
		String COUNTER_IDS = jobContext.getParameter("COUNTER_IDS");

		if (KPI_CODES != null && !KPI_CODES.isEmpty()) {
			kpiList = Arrays.asList(KPI_CODES.split(","));
		}

		if (COUNTER_IDS != null && !COUNTER_IDS.isEmpty()) {
			counterIdList = Arrays.asList(COUNTER_IDS.split(","));
		}

		List<String> columnsToSet = Arrays.asList("DATE", "TIME", "DL1", "DL2", "DL3", "DL4", "H1", "H1_NEID", "NET",
				"NS", "NEID",
				"NAM");

		kpiMap = getUpdateValueByKpi(exceptionalKpiMap, kpiMap);
		String genericKPIWiseFormulaDesc = jobContext.getParameter("genericKPIWiseFormulaDesc");
		logger.info("Generic KPI Wise Formula Desc: {}", genericKPIWiseFormulaDesc);
		Map<String, String> genericKPIWiseFormulaDescs = new Gson().fromJson(genericKPIWiseFormulaDesc, Map.class);
		if (MapUtils.isNotEmpty(genericKPIWiseFormulaDescs)) {
			kpiMap = genericKpiEvaluator(genericKPIWiseFormulaDescs, kpiMap, metaDataMap);
		}

		// Initialize the result array
		Object[] resultArray = initializeObjectArray(kpiList, counterIdList, columnsToSet);
		logger.info("Array Size: {}, Columns To Set Size: {}, Kpi List Size: {}, Counter Id List Size: {}",
				resultArray.length, columnsToSet.size(), kpiList.size(), counterIdList.size());

		// Initialize the result array index
		int arrayIndex = 0;
		initializeResultArray(metaDataMap, kpiMap, columnsToSet, resultArray, arrayIndex, rawCountersMap, kpiList,
				counterIdList);

		logger.info("KPI Evaluator Result: {}", Arrays.toString(resultArray));

		return RowFactory.create(resultArray);
	}

	/**
	 * Replaces NVL expressions or direct counter keys with an opening parenthesis
	 * in the given formula string for further expression preparation.
	 *
	 * @param formulaString The formula string containing NVL or counter
	 *                      placeholders
	 * @param counterKey    The specific counter key to search and replace
	 * @return Updated formula string with NVL patterns replaced
	 */
	private String createNVLExpression(String formulaString, String counterKey) {
		String replaceString = "NVL((" + counterKey + "),";
		String replaceString3Bracket = "NVL(((" + counterKey + ")),";

		if (formulaString.contains(replaceString3Bracket)) {
			formulaString = StringUtils.replace(formulaString, replaceString3Bracket, "(");
		}
		if (formulaString.contains(replaceString)) {
			formulaString = StringUtils.replace(formulaString, replaceString, "(");
		} else {
			formulaString = StringUtils.replace(formulaString, counterKey, "(");
		}

		return formulaString;
	}

	/**
	 * Replaces metadata placeholders in a formula string with actual values from
	 * the metaMap.
	 *
	 * @param formulaeString Formula string containing metadata placeholders
	 * @param metaMap        Map containing metadata key-value pairs
	 * @return Formula string with placeholders replaced by actual values
	 */
	private String replaceMetaDetail(String formulaeString, Map<String, String> metaMap) {
		if (formulaeString.contains("#$") || formulaeString.contains("(EQUALS(#")
				|| formulaeString.contains("(NOT_EQUALS(#)") || formulaeString.contains("LEFT(")
				|| formulaeString.contains("RIGHT(")) {

			Pattern pattern = Pattern.compile("EQUALS\\((#.*?#)", Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(formulaeString);
			while (matcher.find()) {
				String match = matcher.group(1);
				String key = StringUtils.substringBetween(match, "#", "#");
				String value = Symbol.QUOTE_STRING + metaMap.getOrDefault(key, "") + Symbol.QUOTE_STRING;
				formulaeString = StringUtils.replace(formulaeString, match, value);
			}

			if (formulaeString.contains("#$") && formulaeString.contains("$#")) {
				Pattern operatorPattern = Pattern.compile("#\\$(.*?)\\$#");
				Matcher operatorMatcher = operatorPattern.matcher(formulaeString);
				while (operatorMatcher.find()) {
					String key = operatorMatcher.group(1).replace("(", "").replace(")", "");
					String value = metaMap.getOrDefault(key, "");
					formulaeString = StringUtils.replace(formulaeString, "#$" + key + "$#", value);
				}
			}

			formulaeString = formulaeString.replace("\\\"", "'").replace("'", "\"");
		}

		return formulaeString;
	}

	/**
	 * Evaluates generic KPI expressions, replaces placeholders with actual values,
	 * and computes final KPI values. Handles exceptions and retries failed ones
	 * separately.
	 *
	 * @param genericKPIWiseFormulaDesc Map of KPI code to formula expression string
	 * @param kpiValues                 Map of KPI ID to already computed values
	 * @param metaDataMap               Map of metadata used for variable
	 *                                  substitution in expressions
	 * @return Updated KPI values with newly evaluated KPI results
	 */
	private Map<String, String> genericKpiEvaluator(Map<String, String> genericKPIWiseFormulaDesc,
			Map<String, String> kpiValues,
			Map<String, String> metaDataMap) {
		Map<String, String> exceptionalKpiMap = new HashMap<>();

		for (Map.Entry<String, String> entry : genericKPIWiseFormulaDesc.entrySet()) {
			String kpiCode = entry.getKey();
			String formulaDesc = replaceFormulaDescWithValue(kpiValues, entry.getValue());

			try {
				formulaDesc = replaceMetaDetail(formulaDesc, metaDataMap);
				com.enttribe.sparkrunner.util.Expression expression = new com.enttribe.sparkrunner.util.Expression(
						formulaDesc, true);
				double kpiValue = Double.parseDouble(expression.eval());
				updateValueMap(kpiValues, kpiCode, kpiValue);
			} catch (Exception e) {
				String message = e.getMessage();
				if ((message != null && message.equalsIgnoreCase("Division by zero!"))
						&& !message.contains("Unknown operator")) {
					updateValueMap(kpiValues, kpiCode, Double.NaN);
				} else {
					exceptionalKpiMap.put(kpiCode, formulaDesc);
				}
			}
		}

		if (!exceptionalKpiMap.isEmpty()) {
			kpiValues = getUpdateValueByKpi(kpiValues, exceptionalKpiMap);
		}

		return kpiValues;
	}

	/**
	 * Replaces KPI placeholders in the formula description with actual KPI values.
	 *
	 * @param kpiValues   Map of KPI IDs to their corresponding values
	 * @param formulaDesc Formula string containing KPI#ID placeholders
	 * @return Updated formula string with KPI values substituted
	 */
	private String replaceFormulaDescWithValue(Map<String, String> kpiValues, String formulaDesc) {
		for (Map.Entry<String, String> kpiValue : kpiValues.entrySet()) {
			String placeholder = "KPI#" + kpiValue.getKey();
			if (formulaDesc.contains(placeholder)) {
				formulaDesc = StringUtils.replace(formulaDesc, placeholder, kpiValue.getValue());
			}
		}
		return formulaDesc.replace("{", "(").replace("}", ")");
	}

	/**
	 * Updates the kpiMap by assigning the specified kpiValue to each KPI ID in the
	 * comma-separated formulaId string.
	 *
	 * @param kpiMap    Map of KPI IDs and their corresponding values
	 * @param formulaId Comma-separated string of KPI IDs to update
	 * @param kpiValue  The value to assign to each KPI ID
	 * @return Updated KPI map with new values for the specified KPI IDs
	 */
	private Map<String, String> updateValueMap(Map<String, String> kpiMap, String formulaId, double kpiValue) {
		for (String kpiId : formulaId.split(",")) {
			kpiMap.put(kpiId, String.format("%f", kpiValue));
		}
		return kpiMap;
	}

	/**
	 * Sets BI-related metadata columns (DATE, TIME, DL1) in the given metaInfo map
	 * based on frequency and key.
	 *
	 * @param finalKey  Unique key containing the timestamp
	 * @param metaInfo  Map to populate with metadata
	 * @param frequency Data frequency (e.g., "DAILY", "HOURLY", etc.)
	 * @return Updated metaInfo map with BI columns
	 * @throws ParseException if date parsing fails
	 */
	private Map<String, String> setDateTime(String finalKey, Map<String, String> metaInfo,
			String frequency) throws ParseException {

		String timeKey = StringUtils.substringAfter(finalKey,
				"##").trim();

		SimpleDateFormat inputFormat;
		SimpleDateFormat outputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
		SimpleDateFormat outputHourFormat = new SimpleDateFormat("HH:mm");

		if ("5 MIN".equalsIgnoreCase(frequency) ||
				"FIVEMIN".equalsIgnoreCase(frequency)) {
			inputFormat = new SimpleDateFormat("yyyyMMddHHmm");
		} else if ("15 Min".equalsIgnoreCase(frequency) ||
				"QUARTERLY".equalsIgnoreCase(frequency)) {
			inputFormat = new SimpleDateFormat("yyyyMMddHHmm");
		} else if ("HOURLY".equalsIgnoreCase(frequency) ||
				"PERHOUR".equalsIgnoreCase(frequency)) {
			timeKey = timeKey.substring(0, 10);
			inputFormat = new SimpleDateFormat("yyyyMMddHH");

		} else {
			timeKey = timeKey.substring(0, 8);
			inputFormat = new SimpleDateFormat("yyyyMMdd");
		}

		Date date = inputFormat.parse(timeKey);
		metaInfo.put("DATE", outputDateFormat.format(date));
		metaInfo.put("TIME", outputHourFormat.format(date));

		if ("MONTHLY".equalsIgnoreCase(frequency) ||
				"PERMONTH".equalsIgnoreCase(frequency)) {
			Calendar calendar = new GregorianCalendar();
			calendar.setTime(date);
			calendar.set(Calendar.DATE, 1);
			String dateKey = new DateTime(calendar.getTime()).toString("MMM-yyyy");
			metaInfo.put("DATE", dateKey);
		}

		if ("WEEKLY".equalsIgnoreCase(frequency) ||
				"PERWEEK".equalsIgnoreCase(frequency)) {
			metaInfo.put("DATE", parseWeekWithDate(date, "ww-yyyy"));
		}

		if ("YEARLY".equalsIgnoreCase(frequency) || "PERYEAR".equalsIgnoreCase(frequency)) {
			String year = new SimpleDateFormat("yyyy").format(date);
			metaInfo.put("DATE", year);
		}

		return metaInfo;
	}

	/**
	 * Parses the given date to generate a week-based key in the specified format
	 * starting from Monday.
	 *
	 * @param startDate The reference date
	 * @param format    The desired date format (e.g., "yyyyMMdd")
	 * @return A string like "W20250701" representing the week starting date
	 */
	public static String parseWeekWithDate(Date startDate, String format) {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

		DateTime weekStart = new DateTime(calendar.getTime());
		return "W" + weekStart.toString(format);
	}

	/**
	 * Initializes an Object array sized to hold metadata, KPI, and counter values.
	 *
	 * @param kpiIds        List of KPI IDs
	 * @param counterIdList List of counter IDs
	 * @param columnsToSet  List of metadata column names
	 * @return An empty Object array with calculated size
	 */
	private Object[] initializeObjectArray(List<String> kpiIds, List<String> counterIdList, List<String> columnsToSet) {
		int size = columnsToSet.size() + kpiIds.size() + counterIdList.size();
		return new Object[size];
	}

	/**
	 * Extracts and formats the counter value from the given map using supported
	 * split markers.
	 *
	 * @param kpiMap    Map of KPI/counter values
	 * @param counterId The counter ID to fetch and parse
	 * @return Formatted numeric value as string or "-" if invalid
	 */
	private static String getCounterData(Map<String, String> kpiMap, String counterId) {
		try {
			String raw = String.valueOf(kpiMap.get(counterId));

			if (StringUtils.isBlank(raw) || "null".equalsIgnoreCase(raw)) {
				return Symbol.HYPHEN_STRING;
			}

			logger.info("Raw counter value for ID {}: {}", counterId, raw);

			double parsedValue;

			if (raw.contains("@@")) {
				parsedValue = Double.parseDouble(raw.split("@@")[1]);
			} else if (raw.contains("COUNT")) {
				parsedValue = Double.parseDouble(StringUtils.substringAfter(raw,
						"COUNT"));
			} else {
				parsedValue = Double.parseDouble(raw);
			}

			if (Double.isNaN(parsedValue) || Double.isInfinite(parsedValue)) {
				return Symbol.HYPHEN_STRING;
			}

			return new DecimalFormat("##.####").format(parsedValue);

		} catch (Exception e) {
			logger.error("Exception while parsing counter [{}]: {}", counterId,
					ExceptionUtils.getStackTrace(e));
			return Symbol.HYPHEN_STRING;
		}
	}

	/**
	 * Populates the result array with metadata, KPI, and counter values based on
	 * the input mappings and columns.
	 *
	 * @param metaMap        Map of metadata values
	 * @param kpiMap         Map of KPI values
	 * @param columnsToSet   List of metadata columns to include
	 * @param resultArray    Target array to populate
	 * @param arrayIndex     Starting index for inserting values
	 * @param rawCountersMap Map of raw counter values
	 * @param kpiIds         List of KPI IDs to retrieve
	 * @param counterIdList  List of counter IDs to retrieve
	 * @return Updated result array with all values populated
	 */
	private Object[] initializeResultArray(
			Map<String, String> metaMap,
			Map<String, String> kpiMap,
			List<String> columnsToSet,
			Object[] resultArray,
			int arrayIndex,
			Map<String, String> rawCountersMap,
			List<String> kpiIds,
			List<String> counterIdList) {

		logger.info("Starting initializeResultArray with arrayIndex: {}", arrayIndex);

		String startIndex = jobContext.getParameter("START_INDEX");
		logger.info("Start Index: {}", startIndex);
		String filterLevel = jobContext.getParameter("FILTER_LEVEL" + startIndex);
		logger.info("Filter Level: {}", filterLevel);

		for (String column : columnsToSet) {
			String value = getValue(metaMap, column);

			List<String> l0DashList = Arrays.asList("DL1", "DL2", "DL3", "DL4", "H1", "H1_NEID", "NEID", "NAM");
			if (filterLevel.equalsIgnoreCase("L0") && l0DashList.contains(column)) {
				value = "-";
			}
			if (filterLevel.equalsIgnoreCase("L0") && column.equalsIgnoreCase("NEID")) {
				value = "Custom";
			}
			if (filterLevel.equalsIgnoreCase("L0") && column.equalsIgnoreCase("NAM")) {
				value = "Custom";
			}

			logger.info("Processing column: {}, Value: {}", column, value);
			resultArray[arrayIndex++] = value;
		}

		logger.info("After meta columns, arrayIndex: {}", arrayIndex);

		for (String kpiId : kpiIds) {
			String kpiValue = getKPIValueAsDouble(kpiMap, kpiId);
			logger.info("Adding KPI column: {}, Value: {}", kpiId, kpiValue);
			resultArray[arrayIndex++] = kpiValue;
		}

		logger.info("After KPI columns, arrayIndex: {}", arrayIndex);

		for (String counterId : counterIdList) {
			String counterValue = getCounterData(rawCountersMap, counterId);
			logger.info("Adding counter column: {}, Value: {}", counterId, counterValue);
			resultArray[arrayIndex++] = counterValue;
		}

		logger.info("After counter columns, arrayIndex: {}", arrayIndex);

		return resultArray;
	}

	/**
	 * Returns a formatted double string for a KPI value if it's numeric and valid.
	 *
	 * @param kpiMap Map containing KPI values
	 * @param kpiId  The KPI ID to retrieve
	 * @return Formatted KPI value as string or "-" if invalid
	 */
	public static String getKPIValueAsDouble(Map<String, String> kpiMap, String kpiId) {
		String kpiValueString = getValue(kpiMap, kpiId);

		if (kpiValueString != null && isNumeric(kpiValueString)) {
			double value = Double.parseDouble(kpiValueString);
			if (!Double.isNaN(value) && !Double.isInfinite(value)) {
				return new DecimalFormat("##.####").format(value);
			}
		}

		return Symbol.HYPHEN_STRING;
	}

	/**
	 * Checks if the given string is a valid numeric value.
	 *
	 * @param str Input string
	 * @return true if numeric, false otherwise
	 */
	public static boolean isNumeric(String str) {
		try {
			Double.parseDouble(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * Retrieves the value for the given column from the metaMap, or returns "-" if
	 * not found.
	 *
	 * @param metaMap Map containing metadata values
	 * @param column  The key to look up
	 * @return The value from the map or "-" if null
	 */
	private static String getValue(Map<String, String> metaMap, String column) {
		String value = metaMap.get(column);
		return value != null ? value : Symbol.HYPHEN_STRING;
	}

	/**
	 * Replaces NVL-wrapped and plain occurrences of a counterKey in the formula
	 * with its actual value.
	 *
	 * @param formulaString The formula containing NVL expressions or direct counter
	 *                      references
	 * @param counterKey    The KPI/counter key to be replaced
	 * @param value         The value to replace with
	 * @return Updated formula string with substitutions
	 */
	private String createExpression(String formulaString, String counterKey, String value) {
		String replaceStr3Bracket = "NVL(((" + counterKey + ")),";
		String replaceStr2Bracket = "NVL((" + counterKey + "),";

		if (formulaString.contains(replaceStr3Bracket)) {
			String nvlValue = StringUtils.substringBefore(
					StringUtils.substringAfter(formulaString, replaceStr3Bracket), ")");
			String fullMatch = replaceStr3Bracket + nvlValue;
			String replacement = "(" + value;
			formulaString = StringUtils.replace(formulaString, fullMatch, replacement);
		}

		if (formulaString.contains(replaceStr2Bracket)) {
			String nvlValue = StringUtils.substringBefore(
					StringUtils.substringAfter(formulaString, replaceStr2Bracket), ")");
			String fullMatch = replaceStr2Bracket + nvlValue;
			String replacement = "(" + value;
			formulaString = StringUtils.replace(formulaString, fullMatch, replacement);
		}

		formulaString = StringUtils.replace(formulaString, counterKey, value);
		return formulaString;
	}

	/**
	 * Evaluates and updates KPI values in kpiMap using formulas from
	 * exceptionalKpiMap.
	 * Handles HEXTODEC/DECTOHEX functions and catches division errors.
	 *
	 * @param exceptionalKpiMap Map of KPI IDs and their formula expressions
	 * @param kpiMap            Map of KPI IDs and their current values (updated
	 *                          in-place)
	 * @return Updated KPI map with computed values
	 */
	public Map<String, String> getUpdateValueByKpi(Map<String, String> exceptionalKpiMap, Map<String, String> kpiMap) {
		Map<String, String> deferredEvaluationMap = new THashMap<>();

		for (Map.Entry<String, String> counterEntry : exceptionalKpiMap.entrySet()) {
			String counterKey = counterEntry.getKey();
			String formulaeString = counterEntry.getValue();

			try {
				String[] parts = formulaeString.split("KPI#");
				int count = 0;

				for (String part : parts) {
					count++;
					if (count > 1) {
						formulaeString = getUpdateFormulaString(kpiMap, formulaeString, part);
					}
				}

				if (formulaeString.contains("HEXTODEC")) {
					formulaeString = getFunctionsVal(formulaeString, "HEXTODEC");
				}
				if (formulaeString.contains("DECTOHEX")) {
					formulaeString = getFunctionsVal(formulaeString, "DECTOHEX");
				}

				formulaeString = StringUtils.replace(formulaeString, "{", "(");
				formulaeString = StringUtils.replace(formulaeString, "}", ")");

				com.enttribe.sparkrunner.util.Expression expression = new com.enttribe.sparkrunner.util.Expression(
						formulaeString, true);
				String evaluatedValue = expression.eval();
				kpiMap.put(counterKey, evaluatedValue);

			} catch (Exception e) {
				String message = e.getMessage();
				if (message != null && (message.equalsIgnoreCase("Division by zero!") ||
						message.equalsIgnoreCase("Division undefined"))) {
					kpiMap.put(counterKey, String.valueOf(Double.NaN));
				} else {
					deferredEvaluationMap.put(counterKey, formulaeString);
				}
			}
		}

		return getUpdateValueByKpi1(deferredEvaluationMap, kpiMap);
	}

	/**
	 * Replaces the first occurrence of a KPI reference in the formula string with
	 * its evaluated value from kpiMap.
	 *
	 * @param kpiMap        Map of KPI IDs and their evaluated values
	 * @param formulaString The formula string containing KPI references
	 * @param arr           A substring containing the KPI reference
	 * @return Updated formula string with KPI value substitution
	 */
	private String getUpdateFormulaString(Map<String, String> kpiMap, String formulaString, String arr) {
		String kpiId;
		String kpiPrefix;

		if (arr.contains("TimeShift")) {
			String[] split = arr.split("TimeShift");
			kpiId = split[0] + "TimeShift" + StringUtils.substringBefore(split[1], ")");
			kpiPrefix = Symbol.PARENTHESIS_OPEN_STRING + "KPI#";
		} else {
			kpiId = StringUtils.substringBefore(arr, Symbol.PARENTHESIS_CLOSE_STRING);
			kpiPrefix = "KPI#";
		}

		String value = kpiMap.get(kpiId);
		if (value != null) {
			try {
				String kpiValue = String.format("%f", Double.valueOf(value));
				formulaString = StringUtils.replaceOnce(formulaString, kpiPrefix + kpiId, kpiValue);
			} catch (NumberFormatException e) {
				// Optionally log or handle parse error
			}
		}

		return formulaString;
	}

	/**
	 * Replaces the first occurrence of a KPI reference in the formula string with
	 * its evaluated value from kpiMap.
	 *
	 * @param kpiMap        Map of KPI IDs and their evaluated values
	 * @param formulaString The original formula string
	 * @param arr           A segment containing the KPI ID or time-shifted KPI
	 *                      reference
	 * @return The updated formula string with the KPI reference replaced by its
	 *         value
	 */
	private static String getUpdateFormulaString1(Map<String, String> kpiMap, String formulaString, String arr) {
		String kpiValue;
		String kpiId;
		String kpiPrefix;

		try {
			if (arr.contains("TimeShift")) {
				String[] split = arr.split("TimeShift");
				kpiId = split[0] + "TimeShift" + StringUtils.substringBefore(split[1], ")");
				kpiPrefix = Symbol.PARENTHESIS_OPEN_STRING + "KPI#";

				String value = kpiMap.get(kpiId);
				kpiValue = value != null ? String.format("%f", Double.valueOf(value)) : "NaN";

			} else {
				kpiId = StringUtils.substringBefore(arr, Symbol.PARENTHESIS_CLOSE_STRING);
				kpiPrefix = "KPI#";

				String value = kpiMap.get(String.valueOf(Integer.parseInt(kpiId)));
				kpiValue = value != null ? String.format("%f", Double.valueOf(value)) : "NaN";
			}

			if (!"null".equalsIgnoreCase(kpiValue)) {
				formulaString = StringUtils.replaceOnce(formulaString, kpiPrefix + kpiId, kpiValue);
			}

		} catch (Exception e) {
			logger.error("Invalid KPI Format or Value: {}", arr, e);
		}

		return formulaString;
	}

	/**
	 * Extracts the innermost expression between the brackets of a given function
	 * within the formula string.
	 *
	 * @param formulaeString The full formula string containing the function
	 * @param function       The function name to locate (e.g., "AVG", "SUM")
	 * @return The extracted expression inside the function's parentheses
	 */
	public String getPreComputeExpBtwBrackets(String formulaeString, String function) {
		String preComputeExp = StringUtils.substringAfter(formulaeString, function + "(");

		if (preComputeExp.indexOf('(') > 0) {
			while (preComputeExp.indexOf('(') < preComputeExp.indexOf(')')) {
				preComputeExp = preComputeExp
						.replaceFirst("\\(", "\\{")
						.replaceFirst("\\)", "\\}");

				if (preComputeExp.indexOf('(') < 0) {
					break;
				}
			}
		}

		preComputeExp = StringUtils.substringBefore(preComputeExp, ")");
		preComputeExp = preComputeExp.replaceAll("\\{", "(").replaceAll("\\}", ")");

		return preComputeExp;
	}

	/**
	 * Evaluates all occurrences of a specific function within a formula string by
	 * computing
	 * the innermost expressions and replacing them with evaluated values.
	 *
	 * @param formulaeString The full formula string containing function calls
	 * @param function       The function name to evaluate (e.g., "AVG", "SUM")
	 * @return Updated formula string with evaluated function values
	 */
	public String getFunctionsVal(String formulaeString, String function) {
		while (formulaeString.contains(function + "(")) {
			// Extract and evaluate the innermost expression
			String preComputeExp = getPreComputeExpBtwBrackets(formulaeString, function);
			com.enttribe.sparkrunner.util.Expression preComputeExpression = new com.enttribe.sparkrunner.util.Expression(
					preComputeExp, true);
			String preComputedValue = preComputeExpression.eval();

			// Replace the original sub-expression with computed value
			String fullPreComputeCall = function + "(" + preComputeExp + ")";
			formulaeString = StringUtils.replace(formulaeString, fullPreComputeCall,
					function + "(" + preComputedValue + ")");

			// Extract the full function expression to be evaluated
			String funcParam = StringUtils.substringBefore(
					StringUtils.substringAfter(formulaeString, function + "("), ")");

			String formulaeFuncStr = function + "(" + funcParam + ")";
			String funcParamBeforeDot = StringUtils.substringBefore(funcParam, ".");
			String formulaeFunc = function + "(" + funcParamBeforeDot + ")";

			// Evaluate the final function call
			com.enttribe.sparkrunner.util.Expression finalExpression = new com.enttribe.sparkrunner.util.Expression(
					formulaeFunc, true);
			String evaluatedFuncValue = finalExpression.eval();

			// Replace the function call with its evaluated result
			formulaeString = StringUtils.replace(formulaeString, formulaeFuncStr, evaluatedFuncValue);
		}

		return formulaeString;
	}

	/**
	 * Evaluates and updates KPI values in the given kpiMap using formulae defined
	 * in exceptionalKpiMap.
	 *
	 * @param exceptionalKpiMap Map containing KPI keys and their formula
	 *                          expressions
	 * @param kpiMap            Map containing existing KPI values, updated in-place
	 * @return Updated kpiMap with computed KPI values
	 */
	public static Map<String, String> getUpdateValueByKpi1(
			Map<String, String> exceptionalKpiMap,
			Map<String, String> kpiMap) {

		for (Map.Entry<String, String> counterEntry : exceptionalKpiMap.entrySet()) {
			String counterKey = counterEntry.getKey();
			String formulaeString = counterEntry.getValue();

			String[] array = formulaeString.split("KPI#");
			int count = 0;

			try {
				for (String arr : array) {
					count++;
					if (count > 1) {
						formulaeString = getUpdateFormulaString1(kpiMap, formulaeString, arr);
					}
				}

				com.enttribe.sparkrunner.util.Expression expression = new com.enttribe.sparkrunner.util.Expression(
						formulaeString, true);

				String kpiValue = expression.eval();
				kpiMap.put(counterKey, kpiValue);

			} catch (Exception e) {
				kpiMap.put(counterKey, String.valueOf(Double.NaN));
			}
		}

		return kpiMap;
	}

	/**
	 * Returns the name of the UDF as recognized by Spark SQL.
	 * 
	 * @return the name of the UDF, which is "OTFAlertKPIEvaluator"
	 */
	@Override
	public String getName() {
		return "OTFAlertKPIEvaluator";
	}

	/**
	 * Returns the StructType consisting of meta, KPI, and counter fields.
	 *
	 * @return StructType containing all schema fields
	 */
	@Override
	public DataType getReturnType() {
		List<StructField> fields = new ArrayList<>();

		String finalMetaColumns = "DATE,TIME,L1,L2,L3,L4,PARENT_ENTITY_NAME,PARENT_ENTITY_ID,PARENT_ENTITY_TYPE,ENTITY_STATUS,ENTITY_ID,ENTITY_NAME";

		fields.addAll(addStructMetaColumnField(finalMetaColumns));

		List<String> kpiList = new ArrayList<>();
		List<String> counterIdList = new ArrayList<>();

		String KPI_CODES = jobContext.getParameter("KPI_CODES");
		String COUNTER_IDS = jobContext.getParameter("COUNTER_IDS");

		if (KPI_CODES != null && !KPI_CODES.isEmpty()) {
			kpiList = Arrays.asList(KPI_CODES.split(","));
		}

		if (COUNTER_IDS != null && !COUNTER_IDS.isEmpty()) {
			counterIdList = Arrays.asList(COUNTER_IDS.split(","));
		}

		fields.addAll(addStructKPIField(kpiList));
		fields.addAll(addStructKPIField(counterIdList));

		return DataTypes.createStructType(fields);
	}

	/**
	 * Creates a list of StructField (StringType) from a comma-separated string of
	 * column names.
	 *
	 * @param columns Comma-separated string of column names
	 * @return List of StructField objects
	 */
	public List<StructField> addStructMetaColumnField(String columns) {
		List<StructField> fields = new ArrayList<>();
		if (columns == null || columns.trim().isEmpty()) {
			return fields;
		}

		for (String column : columns.split(",")) {
			String trimmedColumn = column.trim();
			if (!trimmedColumn.isEmpty()) {
				fields.add(DataTypes.createStructField(trimmedColumn, DataTypes.StringType, true));
			}
		}

		return fields;
	}

	/**
	 * Creates a list of StructField with StringType for each non-null and non-empty
	 * column name.
	 *
	 * @param columns list of column names
	 * @return list of StructField objects
	 */
	public List<StructField> addStructKPIField(List<String> columns) {
		List<StructField> fields = new ArrayList<>();
		if (columns == null || columns.isEmpty()) {
			return fields;
		}

		for (String column : columns) {
			if (column != null && !column.trim().isEmpty()) {
				fields.add(DataTypes.createStructField(column.trim(), DataTypes.StringType, true));
			}
		}

		return fields;
	}
}