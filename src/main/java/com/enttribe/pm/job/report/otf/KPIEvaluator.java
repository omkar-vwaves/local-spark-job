package com.enttribe.pm.job.report.otf;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
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
import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

import gnu.trove.map.hash.THashMap;

import scala.jdk.CollectionConverters;

public class KPIEvaluator implements
		UDF4<String, scala.collection.immutable.Map<String, String>, scala.collection.immutable.Map<String, String>, String, Row>,
		AbstractUDF {

	private static Logger logger = LoggerFactory.getLogger(KPIEvaluator.class);

	private static final long serialVersionUID = 1L;
	private static Map<String, Map<String, String>> kpiFormulaMap = null;
	public JobContext jobContext;

	public KPIEvaluator() {
		super();
	}

	@Override
	public Row call(String finalKey, scala.collection.immutable.Map<String, String> rawCounter,
			scala.collection.immutable.Map<String, String> metaData, String frequency) throws Exception {

		logger.debug("KPI Evaluator Execution Started With Frequency: {}", frequency);

		java.util.Map<String, String> rawCountersMap = CollectionConverters.MapHasAsJava(rawCounter).asJava();
		java.util.Map<String, String> metaDataMap = new THashMap<>();
		if (metaData != null) {
			metaDataMap = new THashMap<>(CollectionConverters.MapHasAsJava(metaData).asJava());
		}

		logger.debug("Raw Counters Map: {}", rawCountersMap);
		logger.debug("Meta Data Map: {}", metaDataMap);

		Map<String, String> kpiMap = new THashMap<>();
		Map<String, String> exceptionalKpiMap = new THashMap<>();

		String KPI_FORMULA_MAP_JSON = jobContext.getParameter("KPI_FORMULA_MAP");
		kpiFormulaMap = new ObjectMapper().readValue(KPI_FORMULA_MAP_JSON,
				new TypeReference<Map<String, Map<String, String>>>() {
				});

		logger.debug("KPI Formula Final Map: {}", kpiFormulaMap);

		if (kpiFormulaMap != null) {
			Iterator<Map.Entry<String, Map<String, String>>> kpiFormula = kpiFormulaMap.entrySet().iterator();
			while (kpiFormula.hasNext()) {
				int count = 0;
				Map.Entry<String, Map<String, String>> formula = kpiFormula.next();
				logger.debug("Formula: {}", formula);
				if (formula.getKey() != null && formula.getKey().contains("##")) {

					String formulaKey = formula.getKey();
					Map<String, String> formulaValue = formula.getValue();
					logger.debug("Formula Key: {}", formulaKey);
					logger.debug("Formula Value: {}", formulaValue);

					String formulaId = StringUtils.substringBeforeLast(formulaKey, "##");
					String formulaString = StringUtils.substringAfterLast(formulaKey, "##");

					logger.debug("Formula ID: {}", formulaId);
					logger.debug("Formula String: {}", formulaString);

					String timeShiftKey = StringUtils
							.substringBefore(StringUtils.substringAfter(formulaId, ").TimeShift("), ")");
					if (!timeShiftKey.equalsIgnoreCase("")) {
						timeShiftKey = "ts" + timeShiftKey;
					}

					logger.debug("Time Shift Key: {}", timeShiftKey);

					Map<String, String> formulaCounterMap = formulaValue;
					logger.debug("Formula Counter Map: {}", formulaCounterMap);

					for (Entry<String, String> formulaCounterEntry : formulaCounterMap.entrySet()) {
						String counterId = formulaCounterEntry.getKey();
						logger.debug("Counter ID: {}", counterId);
						try {
							if (!counterId.equalsIgnoreCase("")) {
								String counterKey = formulaCounterEntry.getValue();
								logger.debug("Counter Key: {}", counterKey);

								String counterIdWithTimeShift = counterId + timeShiftKey;
								logger.debug("Counter ID With Time Shift: {}", counterIdWithTimeShift);

								if (rawCountersMap.get(counterIdWithTimeShift) != null) {

									logger.debug("Counter ID With Time Shift: {}", counterIdWithTimeShift);

									String value = String.valueOf(rawCountersMap.get(counterIdWithTimeShift));
									logger.debug("Value: {}", value);
									if (!StringUtils.isEmpty(value)) {

										logger.debug("Creating Expression: {}", formulaString);
										try {
											formulaString = createExpression(formulaString, counterKey, value);
											logger.debug("Formula String: {}", formulaString);
										} catch (Exception e) {
											logger.error("Exception: {}", e.getMessage());
										}
									} else {
										logger.debug("Value is Empty: {}", value);
									}
								} else {
									logger.debug("Counter ID With Time Shift Not Found: {}", counterIdWithTimeShift);
									formulaString = createNVLExpression(formulaString, counterKey);
									logger.debug("Formula String: {}", formulaString);
								}
							}
						} catch (Exception e) {
							logger.error(
									"Exception Occured While Evaluating KPI For Counter ID: {}, Formula String: {}, Raw Counters Map: {}",
									counterId, formulaString, rawCountersMap);
						}
					}
					if (count < 1) {

						logger.debug("Replacing Meta Detail: {}", formulaString);
						logger.debug("Meta Data Map: {}", metaDataMap);
						try {
							formulaString = replaceMetaDetail(formulaString, metaDataMap);
							logger.debug("Replaced Meta Detail: {}", formulaString);
							com.enttribe.sparkrunner.util.Expression expression = new com.enttribe.sparkrunner.util.Expression(
									formulaString, true);
							String kpiValue = expression.eval();
							logger.debug("Calculated KPI Value: {}", kpiValue);

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
						logger.debug("Count is not less than 1: {}", count);
						logger.debug("Formula String: {}", formulaString);
					}
				}
			}
		}
		if (finalKey != null && !finalKey.isEmpty()) {
			logger.debug("Setting Date Time For Final Key: {}", finalKey);
			String utcOffsetInMinute = jobContext.getParameter("UTC_OFFSET_IN_MINUTE");
			setDateTime(finalKey, metaDataMap, frequency, utcOffsetInMinute);
		}

		updateMetaDataForAggregation(metaDataMap, jobContext);

		List<String> kpiList = new ArrayList<>();
		List<String> counterIdList = new ArrayList<>();
		String kpiCodeCommaSeparated = jobContext.getParameter("KPI_CODE_COMMA_SEPARATED");
		String counterIdCommaSeparated = jobContext.getParameter("COUNTER_ID_COMMA_SEPARATED");

		logger.debug("KPI Code Comma Separated: {}", kpiCodeCommaSeparated);
		logger.debug("Counter Id Comma Separated: {}", counterIdCommaSeparated);

		String[] kpiCodeArray = kpiCodeCommaSeparated.split(",");
		String[] counterIdArray = counterIdCommaSeparated.split(",");
		for (String kpiCode : kpiCodeArray) {

			if (!kpiCode.trim().equalsIgnoreCase("")) {
				kpiList.add(kpiCode.trim());
			}
		}
		for (String counterId : counterIdArray) {
			if (!counterId.trim().equalsIgnoreCase("")) {
				counterIdList.add(counterId.trim());
			}
		}

		logger.debug("KPI List Size: {}", kpiList.size());
		logger.debug("Counter Id List Size: {}", counterIdList.size());

		List<String> columnsToSet = Arrays.asList("DATE", "TIME", "DL1", "DL2", "DL3", "DL4", "H1", "H1_NEID", "NET",
				"NS", "ENB_NEID",
				"NAM");

		kpiMap = getUpdateValueByKpi(exceptionalKpiMap, kpiMap);

		// String genericKPIWiseFormulaDesc =
		// jobContext.getParameter("GENERIC_KPI_WISE_FORMULA_DESC");
		// logger.debug("🚀 Generic KPI Wise Formula Desc: {}",
		// genericKPIWiseFormulaDesc);
		// Map<String, String> genericKPIWiseFormulaDescs = new
		// Gson().fromJson(genericKPIWiseFormulaDesc, Map.class);
		// if (MapUtils.isNotEmpty(genericKPIWiseFormulaDescs)) {
		// kpiMap = genericKpiEvaluator(genericKPIWiseFormulaDescs, kpiMap,
		// metaDataMap);
		// }

		// Initialize the result array
		Object[] resultArray = initializeObjectArray(kpiList, counterIdList, columnsToSet);
		logger.debug("Array Size: {}, Columns To Set Size: {}, Kpi List Size: {}, Counter Id List Size: {}",
				resultArray.length, columnsToSet.size(), kpiList.size(), counterIdList.size());

		// Initialize the result array index
		int arrayIndex = 0;
		initializeResultArray(metaDataMap, kpiMap, columnsToSet, resultArray, arrayIndex, rawCountersMap, kpiList,
				counterIdList);

		logger.debug("KPI Evaluator Result: {}", Arrays.toString(resultArray));

		return RowFactory.create(resultArray);
	}

	private static Map<String, String> updateMetadebugMap(Map<String, String> metadebug, String removeKey) {
		String[] removeKeyArray = removeKey.split(Symbol.HASH_STRING);
		for (String key : removeKeyArray) {
			metadebug.remove(key);
		}
		return metadebug;
	}

	private Map<String, String> updateMetaDataForAggregation(Map<String, String> metaDataMap, JobContext jobContext) {
		String aggregationLevel = jobContext.getParameter("AGGREGATION_LEVEL");
		String nodeValue = "INDIVIDUAL";
		String reportLevel = "NOT_NEEDED";
		String technology = jobContext.getParameter("TECHNOLOGY");
		metaDataMap.put("T", technology);
		try {
			switch (aggregationLevel) {
				/*
				 * CELL LEVEL
				 */
				case "ENB_NEID":
				case "ENB":

					updateMetadebugMap(metaDataMap, "CEL#NEID#NET#EGID");
					metaDataMap.put("NAM", metaDataMap.get("ENB"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * NODE LEVEL 02
				 */

				case "H2_NEID":
				case "H2":

					updateMetadebugMap(metaDataMap, "CEL#NEID#ENB#ENB_NEID#NET#EGID#SC#NEL#SFID");
					metaDataMap.put("NAM", metaDataMap.get("H2"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * NODE LEVEL 01
				 */

				case "H1_NEID":
				case "H1":

					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#EGID#SC#NEL#SFID#OG");
					metaDataMap.put("NAM", metaDataMap.get("H1"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * GROUP CENTER LEVEL
				 */

				case "GC":
					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#SC#NEL#SFID");
					break;

				/*
				 * L4 LEVEL
				 */

				case "L4":
					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#GC#SC#OG#DOG#NEL#SFID");
					metaDataMap.put("NAM", metaDataMap.get("L4"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * L3 LEVEL
				 */

				case "L3":
					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#GC#L4#DL4#SC#OG#DOG#NEL#SFID");
					metaDataMap.put("NAM", metaDataMap.get("L3"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * L2 LEVEL
				 */

				case "L2":
					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#GC#L4#DL4#L3#DL3#SC#OG#DOG#NEL#SFID");
					metaDataMap.put("NAM", metaDataMap.get("L2"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * L1 LEVEL
				 */

				case "L1":
					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#GC#L4#DL4#L3#DL3#DL2#L2#SC#OG#DOG#NEL#SFID#NEID#CEL#PMEMSID#ENB#VNF#VNFNEID");
					metaDataMap.put("NAM", metaDataMap.get("L1"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");
					break;

				/*
				 * L0 LEVEL
				 */

				case "L0":

					updateMetadebugMap(metaDataMap,
							"CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#GC#L4#DL4#L3#DL3#DL2#L2#DL1#L1#SC#OG#DOG#NEL#SFID#NEID#CEL#PMEMSID#ENB#VNF#VNFNEID");
					metaDataMap.put("NAM", metaDataMap.get("L0"));
					metaDataMap.put("L0", metaDataMap.remove("L0"));
					metaDataMap.put("DL0", metaDataMap.remove("L0"));
					metaDataMap.put("BND", "ALL");
					metaDataMap.put("M33", "ALL");
					metaDataMap.put("NS", "ALL");

					/*
					 * CUSTOM LEVEL
					 */

					if (reportLevel.equalsIgnoreCase("CUSTOM") && !nodeValue.equalsIgnoreCase("INDIVIDUAL")) {
						metaDataMap.put("NAM", "CUSTOM");
						metaDataMap.put("L0", "CUSTOM");
						metaDataMap.put("L1", "CUSTOM");
						metaDataMap.put("DL1", "CUSTOM");
					}
					break;
			}
		} catch (Exception e) {
			logger.error("Exception Occured While Removing Columns From Meta Data Map: {}", e.getMessage());
		}
		return metaDataMap;
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
	// private Map<String, String> genericKpiEvaluator(Map<String, String>
	// genericKPIWiseFormulaDesc,
	// Map<String, String> kpiValues,
	// Map<String, String> metaDataMap) {
	// Map<String, String> exceptionalKpiMap = new HashMap<>();

	// for (Map.Entry<String, String> entry : genericKPIWiseFormulaDesc.entrySet())
	// {
	// String kpiCode = entry.getKey();
	// String formulaDesc = replaceFormulaDescWithValue(kpiValues,
	// entry.getValue());

	// try {
	// formulaDesc = replaceMetaDetail(formulaDesc, metaDataMap);
	// com.enttribe.sparkrunner.util.Expression expression = new
	// com.enttribe.sparkrunner.util.Expression(
	// formulaDesc, true);
	// double kpiValue = Double.parseDouble(expression.eval());
	// updateValueMap(kpiValues, kpiCode, kpiValue);
	// } catch (Exception e) {
	// String message = e.getMessage();
	// if ((message != null && message.equalsIgnoreCase("Division by zero!"))
	// && !message.contains("Unknown operator")) {
	// updateValueMap(kpiValues, kpiCode, Double.NaN);
	// } else {
	// exceptionalKpiMap.put(kpiCode, formulaDesc);
	// }
	// }
	// }

	// if (!exceptionalKpiMap.isEmpty()) {
	// kpiValues = getUpdateValueByKpi(kpiValues, exceptionalKpiMap);
	// }

	// return kpiValues;
	// }

	/**
	 * Replaces KPI placeholders in the formula description with actual KPI values.
	 *
	 * @param kpiValues   Map of KPI IDs to their corresponding values
	 * @param formulaDesc Formula string containing KPI#ID placeholders
	 * @return Updated formula string with KPI values substituted
	 */
	// private String replaceFormulaDescWithValue(Map<String, String> kpiValues,
	// String formulaDesc) {
	// for (Map.Entry<String, String> kpiValue : kpiValues.entrySet()) {
	// String placeholder = "KPI#" + kpiValue.getKey();
	// if (formulaDesc.contains(placeholder)) {
	// formulaDesc = StringUtils.replace(formulaDesc, placeholder,
	// kpiValue.getValue());
	// }
	// }
	// return formulaDesc.replace("{", "(").replace("}", ")");
	// }

	/**
	 * Updates the kpiMap by assigning the specified kpiValue to each KPI ID in the
	 * comma-separated formulaId string.
	 *
	 * @param kpiMap    Map of KPI IDs and their corresponding values
	 * @param formulaId Comma-separated string of KPI IDs to update
	 * @param kpiValue  The value to assign to each KPI ID
	 * @return Updated KPI map with new values for the specified KPI IDs
	 */
	// private Map<String, String> updateValueMap(Map<String, String> kpiMap, String
	// formulaId, double kpiValue) {
	// for (String kpiId : formulaId.split(",")) {
	// kpiMap.put(kpiId, String.format("%f", kpiValue));
	// }
	// return kpiMap;
	// }

	/**
	 * Sets BI-related metadata columns (DATE, TIME, DL1) in the given metadebug map
	 * based on frequency and key.
	 *
	 * @param finalKey  Unique key containing the timestamp
	 * @param metadebug  Map to populate with metadata
	 * @param frequency Data frequency (e.g., "DAILY", "HOURLY", etc.)
	 * @return Updated metadebug map with BI columns
	 * @throws ParseException if date parsing fails
	 */
	private Map<String, String> setDateTime(String finalKey, Map<String, String> metadebug,
			String frequency, String utcOffsetInMinute) throws ParseException {

		frequency = frequency != null ? frequency.toUpperCase().trim() : "15 MIN";

		logger.debug("Setting Date Time, With Final Key={}, Frequency={}, UTC Offset={}", finalKey, frequency, utcOffsetInMinute);

		List<String> fiveMinFrequencyList = Arrays.asList("5 MIN", "FIVEMIN");
		List<String> quarterFrequencyList = Arrays.asList("15 MIN", "QUARTERLY");
		List<String> hourlyFrequencyList = Arrays.asList("HOURLY", "PERHOUR");
		List<String> dailyFrequencyList = Arrays.asList("DAILY", "PERDAY");

		try {
			String timeKey = StringUtils.substringAfter(finalKey,
					"##").trim();

			logger.debug("Time Key={}", timeKey);

			SimpleDateFormat inputFormat;
			SimpleDateFormat outputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
			SimpleDateFormat outputHourFormat = new SimpleDateFormat("HH:mm");

			Date date = null;

			if (fiveMinFrequencyList.contains(frequency)) {
				inputFormat = new SimpleDateFormat("yyyyMMddHHmm");
				date = inputFormat.parse(timeKey);

			} else if (quarterFrequencyList.contains(frequency)) {
				inputFormat = new SimpleDateFormat("yyyyMMddHHmm");
				date = inputFormat.parse(timeKey);

			} else if (hourlyFrequencyList.contains(frequency)) {
				timeKey = timeKey.substring(0, 10);
				inputFormat = new SimpleDateFormat("yyyyMMddHH");
				date = inputFormat.parse(timeKey);

			} else if (dailyFrequencyList.contains(frequency)) {
				timeKey = timeKey.substring(0, 8);
				inputFormat = new SimpleDateFormat("yyyyMMdd");
				date = inputFormat.parse(timeKey);

			} else {
				inputFormat = new SimpleDateFormat("yyyyMMdd");
				// date = inputFormat.parse(timeKey);
			}

			// Apply UTC offset if date is parsed and offset is provided
			if (date != null && utcOffsetInMinute != null && !utcOffsetInMinute.trim().isEmpty()) {
				try {
					int offsetMinutes = Integer.parseInt(utcOffsetInMinute.trim());
					if (offsetMinutes != 0) {
						Calendar calendar = Calendar.getInstance();
						calendar.setTime(date);
						calendar.add(Calendar.MINUTE, offsetMinutes);
						date = calendar.getTime();
						logger.debug("Applied UTC offset of {} minutes to date", offsetMinutes);
					}
				} catch (NumberFormatException e) {
					logger.error("Invalid UTC Offset Value: {}. Using Original Date.", utcOffsetInMinute);
				}
			}

			// Set metadebug based on frequency after applying offset
			if (fiveMinFrequencyList.contains(frequency) && date != null) {
				metadebug.put("DATE", outputDateFormat.format(date));
				metadebug.put("TIME", outputHourFormat.format(date));

			} else if (quarterFrequencyList.contains(frequency) && date != null) {
				metadebug.put("DATE", outputDateFormat.format(date));
				metadebug.put("TIME", outputHourFormat.format(date));

			} else if (hourlyFrequencyList.contains(frequency) && date != null) {
				metadebug.put("DATE", outputDateFormat.format(date));
				metadebug.put("TIME", outputHourFormat.format(date));

			} else if (dailyFrequencyList.contains(frequency) && date != null) {
				metadebug.put("DATE", outputDateFormat.format(date));

			}

			if ("MONTHLY".equalsIgnoreCase(frequency) ||
					"PERMONTH".equalsIgnoreCase(frequency)) {

				// Date date = inputFormat.parse(timeKey);

				// Calendar calendar = new GregorianCalendar();
				// calendar.setTime(date);
				// calendar.set(Calendar.DATE, 1);
				// String dateKey = new DateTime(calendar.getTime()).toString("MMM-yyyy");
				metadebug.put("DATE", timeKey);
			}

			if ("WEEKLY".equalsIgnoreCase(frequency) ||
					"PERWEEK".equalsIgnoreCase(frequency)) {
				// Date date = inputFormat.parse(timeKey);
				// metadebug.put("DATE", parseWeekWithDate(date, "ww-yyyy"));
				// metadebug.put("TIME", getWeekRange(date));
				metadebug.put("DATE", timeKey);
				metadebug.put("TIME", getWeekRange(timeKey));
			}

			if ("YEARLY".equalsIgnoreCase(frequency) || "PERYEAR".equalsIgnoreCase(frequency)) {
				if (date != null) {
					String year = new SimpleDateFormat("yyyy").format(date);
					metadebug.put("DATE", year);
				} else {
					Date yearDate = inputFormat.parse(timeKey);
					String year = new SimpleDateFormat("yyyy").format(yearDate);
					metadebug.put("DATE", year);
				}
			}

		} catch (Exception e) {
			logger.error("Exception while setting Date Time, With Final Key={}, Frequency={}", finalKey, frequency,
					e);
		}

		logger.debug("Meta debug: {}", metadebug);

		return metadebug;
	}

	// public static String getWeekRange(Date date) {
	// Calendar calendar = new GregorianCalendar();
	// calendar.setTime(date);
	// calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

	// Date weekStart = calendar.getTime();

	// calendar.add(Calendar.DATE, 6); // Add 6 days to get Sunday
	// Date weekEnd = calendar.getTime();

	// SimpleDateFormat format = new SimpleDateFormat("dd MMM yyyy");
	// return format.format(weekStart) + " - " + format.format(weekEnd);
	// }

	public static String getWeekRange(String weekFormat) {
		try {
			// Input format: W27-2025
			String[] parts = weekFormat.replace("W", "").split("-");
			int week = Integer.parseInt(parts[0]);
			int year = Integer.parseInt(parts[1]);

			Calendar calendar = Calendar.getInstance();
			calendar.clear(); // Clear all fields
			calendar.set(Calendar.YEAR, year);
			calendar.set(Calendar.WEEK_OF_YEAR, week);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY); // Week starts on Monday

			Date weekStart = calendar.getTime();
			calendar.add(Calendar.DATE, 6); // Add 6 days for Sunday
			Date weekEnd = calendar.getTime();

			SimpleDateFormat format = new SimpleDateFormat("dd MMM yyyy");
			return format.format(weekStart) + " - " + format.format(weekEnd);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid week format: " + weekFormat, e);
		}
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

			logger.debug("Raw counter value for ID {}: {}", counterId, raw);

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
			logger.error("Exception while parsing counter [{}]: {}", counterId, e.getMessage());
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

		logger.debug("Starting initializeResultArray with arrayIndex: {}", arrayIndex);

		for (String column : columnsToSet) {
			String value = getValue(metaMap, column);

			logger.debug("Processing column: {}, Value: {}", column, value);
			resultArray[arrayIndex++] = value;
		}

		logger.debug("After meta columns, arrayIndex: {}", arrayIndex);

		for (String kpiId : kpiIds) {
			String kpiValue = getKPIValueAsDouble(kpiMap, kpiId);
			logger.debug("Adding KPI column: {}, Value: {}", kpiId, kpiValue);
			resultArray[arrayIndex++] = kpiValue;
		}

		logger.debug("After KPI columns, arrayIndex: {}", arrayIndex);

		for (String counterId : counterIdList) {
			String counterValue = getCounterData(rawCountersMap, counterId);
			logger.debug("Adding counter column: {}, Value: {}", counterId, counterValue);
			resultArray[arrayIndex++] = counterValue;
		}

		logger.debug("After counter columns, arrayIndex: {}", arrayIndex);

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
	 * @return the name of the UDF, which is "KPIEvaluator"
	 */
	@Override
	public String getName() {
		return "KPIEvaluator";
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
		List<String> counterList = new ArrayList<>();

		String kpiCodeCommaSeparated = jobContext.getParameter("KPI_CODE_COMMA_SEPARATED");
		String counterIdCommaSeparated = jobContext.getParameter("COUNTER_ID_COMMA_SEPARATED");

		String[] kpiCodeArray = kpiCodeCommaSeparated.split(",");
		String[] counterIdArray = counterIdCommaSeparated.split(",");

		for (String kpiCode : kpiCodeArray) {
			if (!kpiCode.trim().equalsIgnoreCase("")) {
				kpiList.add(kpiCode.trim());
			}
		}
		for (String counterId : counterIdArray) {
			if (!counterId.trim().equalsIgnoreCase("")) {
				counterList.add(counterId.trim());
			}
		}

		fields.addAll(addStructKPIField(kpiList));
		fields.addAll(addStructKPIField(counterList));

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