
package com.enttribe.pm.job.hourly;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.lang.DateUtils;
import com.enttribe.commons.lang.StringUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gnu.trove.map.hash.THashMap;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TimeZone;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF9;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.time.*;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.CollectionConverters;

public class RegisterUDF_KPI extends Processor {

    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(RegisterUDF_KPI.class);
    public String tempTable;
    private static Object SYNCHRONIZER = new Object();
    private static final String DOMAIN = "domain";
    private static final String VENDOR = "vendor";
    private static final String TECHNOLOGY = "technology";
    private static final String NETWORK_TYPE = "networkType";
    private static final String LEVEL = "Level";
    private static final String DATA_LEVEL = "datalevel";
    private static final String CTIME = "ctime";
    private static final String NODENAME = "nodeName";
    private static final String QUARTERLY_DATE_FORMATE = "yyMMddHHmmss";
    private static final String HOURLY_DATE_FORMATE = "yyMMddHHmm";
    private static final String QUARTERLY = "QUARTERLY";
    private static final String DAILY_STRING = "DAILY";
    private static final String NBH_STRING = "NBH";
    private static final String HOURLY_STRING = "HOURLY";
    private static final String BBH_STRING = "BBH";
    private static final String QUARTERLY_STRING = "QUARTERLY";
    private static final String WEEKLY_STRING = "WEEKLY";
    private static final String MONTHLY_STRING = "MONTHLY";
    private static final String TIME_STRING = "Time";
    private static final String META_DATE = "Meta_Date";
    private static final String PT_STRING = "PT";
    private static final String HR_STRING = "HR";
    private static final String DT_STRING = "DT";
    private static final String FOURZERO_STRING = "0000";
    private static final String KPI_NAMESPLIT = "KPI#";
    private static final String DIVISION_BY_ZERO = "Division by zero!";
    private static final String DIVISION_UNDEFINED = "Division undefined";
    private static final String UNKNOWN_FUNCTION = "Unknown";
    private static final String MISMATCHED_PARENTHESES = "Mismatched parentheses";
    private static final String FUNCTION_IF_EXPECTED_3_PARAMETERS = "Function IF expected 3 parameters";
    private static final String NODE = "Node";
    private static final String ROW_KEY_APPENDER = "RowKeyAppender";
    private static final String SAV = "SAV";
    private static final String ALL = "ALL";
    private static final String METADATA = "metaData";
    private static final String COUNTRY = "India";
    private static final String DATE_SMALL = "date";
    private static final String TIMESTAMP = "timestamp";
    private static String PADDING_STRING = "0";
    private static Integer CRC_LENGTH = 10;
    private static final String ROWKEY = "rowKey";

    private static ConcurrentHashMap<String, Map<String, Object>> kpiFormulaFinalMap = null;
    private static Map<String, Map<String, String>> neVsL3Map = null;
    private static Map<String, String> netypeRowKeyAppenderMap = null;
    private static String colRemove;

    private static final ObjectMapper mapper = new ObjectMapper();

    public RegisterUDF_KPI() {
        super();
    }

    public RegisterUDF_KPI(Dataset<Row> dataFrame, Integer id, String processorName, String tempTable) {
        super(id, processorName);
        this.tempTable = tempTable;
        this.dataFrame = dataFrame;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        // REGISTER KPI EVALUATOR UDF
        KPIEvaluatorBntv kpiEvaluator = new KPIEvaluatorBntv(jobContext);
        UserDefinedFunction kpiEvaluatorUdf = org.apache.spark.sql.functions
                .udf(kpiEvaluator, kpiEvaluator.getReturnType())
                .asNondeterministic();
        jobContext.sqlctx().udf().register("KPIEvaluatorBntv", kpiEvaluatorUdf);

        // REGISTER NODE AGGR ROW KEY CONVERTER UDF
        NodeAggrRowKeyConverter nodeKeyConverter = new NodeAggrRowKeyConverter(jobContext);
        UserDefinedFunction nodeKeyConverterUdf = org.apache.spark.sql.functions
                .udf(nodeKeyConverter, nodeKeyConverter.getReturnType())
                .asNondeterministic();
        jobContext.sqlctx().udf().register("NodeAggrRowKeyConverter", nodeKeyConverterUdf);

        // REGISTER TIME AGGR ROW KEY CONVERTER UDF
        TimeAggrRowKeyConverter timeKeyConverter = new TimeAggrRowKeyConverter();
        UserDefinedFunction timeKeyConverterUdf = org.apache.spark.sql.functions
                .udf(timeKeyConverter, timeKeyConverter.getReturnType())
                .asNondeterministic();
        jobContext.sqlctx().udf().register("TimeAggrRowKeyConverter", timeKeyConverterUdf);

        // REGISTER CREATE META DATA UDF
        CreateMetaData metaDataCreator = new CreateMetaData(jobContext);
        UserDefinedFunction metaDataCreatorUdf = org.apache.spark.sql.functions
                .udf(metaDataCreator, metaDataCreator.getReturnType())
                .asNondeterministic();
        jobContext.sqlctx().udf().register("CreateMetaData", metaDataCreatorUdf);

        logger.info("🚀 UDFs Registered Successfully! ✅");
        return this.dataFrame;
    }
    // ===================== KPI EVALUATOR UDF STARTED ===================== //

    public class KPIEvaluatorBntv implements
            UDF9<String, String, String, String, String, String, Long, scala.collection.immutable.Map<String, Double>, scala.collection.immutable.Map<String, String>, Row>,
            AbstractUDF {

        private static final long serialVersionUID = 1L;
        private Logger logger = LoggerFactory.getLogger(KPIEvaluatorBntv.class);
        private JobContext jobcontext;

        public KPIEvaluatorBntv() {
            super();
        }

        public KPIEvaluatorBntv(JobContext jobcontext) {
            this.jobcontext = jobcontext;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Row call(String domain, String vendor, String technology, String networkType, String level,
                String nodeName, Long ctime, scala.collection.immutable.Map<String, Double> rawCounter,
                scala.collection.immutable.Map<String, String> metaData) throws Exception {

            java.util.Map<String, Double> rawCounterMap = CollectionConverters.MapHasAsJava(rawCounter).asJava();
            java.util.Map<String, String> metaJsonMap = new THashMap<>();

            if (metaData != null) {
                metaJsonMap = new THashMap<>(CollectionConverters.MapHasAsJava(metaData).asJava());
            }

            initializeKpiFormulaMap();

            Map<String, String> kpiMap = new THashMap<String, String>();
            Map<String, String> exceptionalKpiMap = new THashMap<>();
            Map<String, String> NvlKpiMap = new THashMap<>();

            logger.error("MetaJsonMap: {}", metaJsonMap.toString());
            String date = metaJsonMap.get("date");
            Iterator<Map.Entry<String, Map<String, Object>>> kpiFormula = kpiFormulaFinalMap.entrySet().iterator();

            while (kpiFormula.hasNext()) {

                int count = 0;
                Entry<String, Map<String, Object>> formula = kpiFormula.next();

                if (formula.getKey() != null) {

                    String formulaId = formula.getKey();

                    Map<String, Object> formulaValues = formula.getValue();

                    String formulaString = (String) formulaValues.get("formulaString");

                    List<String> generickpi = (List<String>) formulaValues.get("generickpi");

                    Map<String, String> formulaCounterMap = (Map<String, String>) formulaValues
                            .get("formulaCounterMap");

                    for (Entry<String, String> formulaCounterEntry : formulaCounterMap.entrySet()) {

                        String counterId = formulaCounterEntry.getKey();

                        try {

                            String counterKey = "CK" + counterId;
                            Double value = rawCounterMap.get(counterId);

                            if (value != null) {
                                formulaString = createExpression(formulaString, counterKey, value);
                            } else {
                                formulaString = createNVLExpression(formulaString, counterKey);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    if (count < 1) {
                        try {
                            if (!hasFunction(formulaString)) {

                                String kpiValue = String
                                        .valueOf(new ExpressionBuilder(formulaString).build().evaluate());

                                if (formulaId != null && formulaId != "null") {
                                    kpiMap.put(formulaId, kpiValue);
                                }
                                updateValueToGenericKpi(kpiMap, generickpi, kpiValue);
                            } else {
                                updateExceptionalKpiMap(exceptionalKpiMap, formulaId, formulaString, generickpi);
                            }
                        } catch (Exception e) {
                            if (e.getMessage() != null && (e.getMessage().equalsIgnoreCase(DIVISION_BY_ZERO)
                                    || e.getMessage().equalsIgnoreCase(DIVISION_UNDEFINED))) {
                            }
                        }
                    }
                }
            }
            int computedKpi = kpiMap.size();
            logger.error("Initial Computed KPI Count: {}", computedKpi);

            int updatedComputedKpi = 0;
            while (computedKpi != updatedComputedKpi) {
                computedKpi = kpiMap.size();
                kpiMap = evaluateFunction(exceptionalKpiMap, kpiMap, metaJsonMap, NvlKpiMap);

                updatedComputedKpi = kpiMap.size();
            }

            logger.error("Final Computed KPI Count: {}", kpiMap.size());

            kpiMap = evaluateFunction(NvlKpiMap, kpiMap, metaJsonMap, null);

            metaJsonMap = updateMetaInfoMap(metaJsonMap, colRemove);

            Row row = null;
            String kpiMapString = Symbol.EMPTY_STRING;
            String metaMapString = Symbol.EMPTY_STRING;

            if (StringUtils.isNotEmpty(domain) && !StringUtils.isEmpty(vendor) && !StringUtils.isEmpty(technology)
                    && !StringUtils.isEmpty(level) && !StringUtils.isEmpty(date) && !StringUtils.isEmpty(nodeName)
                    && ctime != null && !StringUtils.isEmpty(networkType)) {

                kpiMapString = new ObjectMapper().writeValueAsString(kpiMap);
                metaMapString = new ObjectMapper().writeValueAsString(metaJsonMap);

                if (kpiMap != null) {
                    for (Entry<String, String> entry : kpiMap.entrySet()) {
                        if (entry.getValue() == null || "null".equalsIgnoreCase(entry.getValue())) {
                            entry.setValue("");
                        }
                    }
                }
                if (metaJsonMap != null) {
                    for (Entry<String, String> entry : metaJsonMap.entrySet()) {
                        if (entry.getValue() == null || "null".equalsIgnoreCase(entry.getValue())) {
                            entry.setValue("");
                        }
                    }
                }

                String dataLevel = generateDataLevel(level, technology, networkType);

                List<String> CHILD_NE_TYPE_LIST = Arrays.asList(
                        "BACKPLANE", "CHASSIS", "CONTAINER", "CPM_MODULE", "CPU", "FABRIC_MODULE", "FAN",
                        "FLASH_DISK_MODULE", "INTERFACE", "MDA_MODULE", "MODULE", "NOT_FOUND", "OTHER", "PORT",
                        "POWER_SUPPLY", "SENSOR", "SERVER");

                if (level != null) {
                    String upperLevel = level.toUpperCase();

                    if (upperLevel.contains("L0") || upperLevel.contains("L1") || upperLevel.contains("L2")
                            || upperLevel.contains("L3") || upperLevel.contains("L4")) {

                        if (metaJsonMap != null) {
                            metaJsonMap.put("NEID", nodeName);
                            metaJsonMap.put("NAM", nodeName);
                            metaJsonMap.put("NET", networkType);
                            metaJsonMap.put("ENTITY_ID", nodeName);
                            metaJsonMap.put("ENTITY_NAME", nodeName);
                            metaJsonMap.put("ENTITY_TYPE", networkType);
                        }

                    } else if (CHILD_NE_TYPE_LIST.stream().anyMatch(upperLevel::startsWith)) {

                        Optional<String> matchedChildNEType = CHILD_NE_TYPE_LIST.stream()
                                .filter(upperLevel::startsWith)
                                .findFirst();

                        if (matchedChildNEType.isPresent()) {

                            if (metaJsonMap != null) {

                                String neId = metaJsonMap.get("NEID") != null ? metaJsonMap.get("NEID") : nodeName;
                                String nename = metaJsonMap.get("NAM") != null ? metaJsonMap.get("NAM") : nodeName;
                                metaJsonMap.put("ENTITY_ID", neId);
                                metaJsonMap.put("ENTITY_NAME", nename);
                                metaJsonMap.put("ENTITY_TYPE", matchedChildNEType.get());

                                String childNeId = metaJsonMap.get("NEID");
                                if (childNeId != null && !childNeId.isEmpty()) {
                                    String parentNeId = childNeId.split("_")[0];
                                    metaJsonMap.put("PARENT_ENTITY_ID", parentNeId);
                                }

                                String parentNeName = metaJsonMap.get("NAM");
                                if (parentNeName != null && !parentNeName.isEmpty()) {
                                    parentNeName = parentNeName.split("_")[0];
                                    metaJsonMap.put("PARENT_ENTITY_NAME", parentNeName);
                                }

                                String parentNeType = metaJsonMap.get("NODE_TYPE");
                                if (parentNeType != null) {
                                    metaJsonMap.put("PARENT_ENTITY_TYPE", parentNeType);
                                }

                                // metaJsonMap.put("ENTITY_TYPE", metaJsonMap.get("NODE_TYPE"));
                            }
                        } else {
                        }
                    } else {
                        if (metaJsonMap != null && metaJsonMap.containsKey("ENTITY_NAME")) {
                            metaJsonMap.put("NAM", metaJsonMap.get("ENTITY_NAME"));
                        }
                        if (metaJsonMap != null && metaJsonMap.containsKey("NODE_TYPE")) {
                            metaJsonMap.put("NET", metaJsonMap.get("NODE_TYPE"));
                        }
                    }
                }
                /* =================== Final CQL Row Returns Here =================== */

                // logger.error("Generated Row: {}", row);

                // row = (Row) RowFactory.create(
                // new Object[] { domain, vendor, technology, dataLevel, date, nodeName, ctime,
                // kpiMap,
                // metaJsonMap, networkType });

                if (metaJsonMap != null) {
                    metaJsonMap.entrySet().removeIf(entry -> {
                        Object value = entry.getValue();
                        return value == null || (value instanceof String && ((String) value).trim().isEmpty());
                    });
                } else {
                    metaJsonMap = new THashMap<>();
                }

                logger.error("Generated Row: {}", row);

                if (domain != null && vendor != null && technology != null && dataLevel != null && date != null
                        && nodeName != null && ctime != null && networkType != null) {

                    row = (Row) RowFactory.create(
                            new Object[] { domain, vendor, technology, dataLevel, date, nodeName, ctime, kpiMap,
                                    metaJsonMap, networkType });

                }
            } else {
                logger.error(
                        "Invalid Input KPIEvaluatorBntv: domain={} vendor={} technology={} level={} date={} nodeName={} ctime={} networkType={}",
                        domain, vendor, technology, level, date, nodeName, ctime, networkType);
            }

            return row;

        }

        private String createNVLExpression(String formulaString, String counterKey) {
            if (formulaString == null || counterKey == null) {
                return formulaString;
            }

            String key3Brackets = "NVL(((" + counterKey + ")),";
            String key2Brackets = "NVL((" + counterKey + "),";

            formulaString = StringUtils.replace(formulaString, key3Brackets, "(");
            formulaString = StringUtils.replace(formulaString, key2Brackets, "(");

            return formulaString;
        }

        private void updateExceptionalKpiMap(Map<String, String> exceptionalKpiMap, String formulaId,
                String formulaString, List<String> genericKpiList) {
            if (exceptionalKpiMap == null || formulaId == null || formulaString == null) {
                return;
            }

            exceptionalKpiMap.put(formulaId, formulaString);

            if (genericKpiList != null) {
                for (String genericKpiId : genericKpiList) {
                    if (genericKpiId != null && !"null".equalsIgnoreCase(genericKpiId)) {
                        exceptionalKpiMap.put(genericKpiId, formulaString);
                    }
                }
            }
        }

        private void updateValueToGenericKpi(Map<String, String> kpiMap, List<String> genericKpiList, String kpiValue) {
            if (genericKpiList == null || kpiMap == null) {
                return;
            }

            for (String genericKpiId : genericKpiList) {
                if (genericKpiId != null && !"null".equalsIgnoreCase(genericKpiId)) {
                    kpiMap.put(genericKpiId, kpiValue);
                }
            }
        }

        private static final Set<String> FUNCTION_KEYWORDS = Set.of(
                "KPI#", "IF(", "MIN", "MAX", "FLOOR", "DECTOHEX", "HEXTODEC",
                "ROUND", "CEILING", "LEN", "RIGHT", "LEFT", "#");

        private boolean hasFunction(String formula) {
            if (formula == null || formula.isEmpty())
                return false;

            String upperFormula = formula.toUpperCase();
            return FUNCTION_KEYWORDS.stream()
                    .filter(Objects::nonNull)
                    .map(String::toUpperCase)
                    .anyMatch(upperFormula::contains);
        }

        private boolean hasKpiWithinKpi(String formula) {
            if (formula.contains("KPI#")) {
                return true;
            }
            return false;
        }

        private static final Set<String> SPECIAL_FUNCTIONS = Set.of(
                "IF", "MIN", "MAX", "FLOOR", "DECTOHEX", "HEXTODEC",
                "ROUND", "CEILING", "LEN", "RIGHT", "LEFT", "||");

        private boolean hasSpecialFunction(String formula) {
            if (formula == null || formula.isEmpty())
                return false;

            String upperFormula = formula.toUpperCase();
            return SPECIAL_FUNCTIONS.stream().anyMatch(upperFormula::contains);
        }

        private String createExpression(String formulaString, String counterKey, Double value) {
            if (formulaString == null || counterKey == null || value == null) {
                return formulaString;
            }

            String valueStr = String.valueOf(value);
            String baseKey1 = "NVL(((" + counterKey + ")),";
            String baseKey2 = "NVL((" + counterKey + "),";

            if (formulaString.contains(baseKey1)) {
                String nvlValue = StringUtils.substringBefore(
                        StringUtils.substringAfter(formulaString, baseKey1), ")");
                formulaString = StringUtils.replace(formulaString, baseKey1 + nvlValue, "(" + valueStr);
            }

            if (formulaString.contains(baseKey2)) {
                String nvlValue = StringUtils.substringBefore(
                        StringUtils.substringAfter(formulaString, baseKey2), ")");
                formulaString = StringUtils.replace(formulaString, baseKey2 + nvlValue, "(" + valueStr);
            }

            // Replace remaining instances of the raw counterKey with the value
            formulaString = StringUtils.replace(formulaString, counterKey, valueStr);

            return formulaString;
        }

        public String generateDataLevel(String level, String technology, String networkType) {
            if (level == null || technology == null || networkType == null) {
                return null;
            }

            int underscoreIndex = level.indexOf('_');
            if (underscoreIndex > -1) {
                level = level.substring(0, underscoreIndex);
            }

            return level + "_" + technology + "_" + networkType;
        }

        private Map<String, String> updateMetaInfoMap(Map<String, String> metaInfo, String removeKey) {
            if (metaInfo != null && removeKey != null && !removeKey.isEmpty()) {
                String[] keysToRemove = removeKey.split(Symbol.HASH_STRING);
                for (String key : keysToRemove) {
                    if (!key.isEmpty()) {
                        metaInfo.remove(key);
                    }
                }
            }
            return metaInfo;
        }

        public Map<String, String> evaluateFunction(Map<String, String> exceptionalKpiMap, Map<String, String> kpiMap,
                Map<String, String> metaMap, Map<String, String> nvlKpiMap) {

            Map<String, String> tempMap = new HashMap<>(exceptionalKpiMap);

            for (Entry<String, String> counterEntry : tempMap.entrySet()) {
                String counterKey = counterEntry.getKey();
                String formulaeString = counterEntry.getValue();

                if (nvlKpiMap != null && formulaeString.contains("NVL(")) {
                    updateExceptionalKpiMap(nvlKpiMap, counterKey, formulaeString, null);
                    continue;
                }
                String[] array = formulaeString.split(KPI_NAMESPLIT);
                try {
                    for (int count = 1; count < array.length; count++) {

                        String kpiId = StringUtils.substringBefore(array[count], ")");
                        String kpiIdWithSuffix = KPI_NAMESPLIT + kpiId;
                        String value = (String) kpiMap.get(kpiId);

                        if (value != null) {
                            formulaeString = createExpression(formulaeString, kpiIdWithSuffix, Double.valueOf(value));
                        } else {
                            formulaeString = createNVLExpression(formulaeString, kpiIdWithSuffix);
                        }
                    }
                    if (formulaeString.contains("HEXTODEC")) {
                        formulaeString = getFunctionsVal(formulaeString, "HEXTODEC");
                    }
                    if (formulaeString.contains("DECTOHEX")) {
                        formulaeString = getFunctionsVal(formulaeString, "DECTOHEX");
                    }
                    formulaeString = replaceMetaDetail(formulaeString, metaMap);

                    if (!hasKpiWithinKpi(formulaeString)) {
                        if (hasSpecialFunction(formulaeString)) {

                            com.enttribe.sparkrunner.util.Expression expression1 = new com.enttribe.sparkrunner.util.Expression(
                                    formulaeString, true);

                            String kpiValue = expression1.eval();

                            if (counterKey != null && counterKey != "null" && !kpiMap.containsKey(counterKey)) {
                                kpiMap.put(counterKey, kpiValue);
                            }

                        } else {
                            String kpiValue = String.valueOf(new ExpressionBuilder(formulaeString).build().evaluate());

                            if (counterKey != null && counterKey != "null" && !kpiMap.containsKey(counterKey)) {
                                kpiMap.put(counterKey, kpiValue);
                            }
                        }
                        exceptionalKpiMap.remove(counterKey);
                    }
                } catch (Exception e) {

                    if (e.getMessage() != null && (e.getMessage().equalsIgnoreCase(DIVISION_BY_ZERO)
                            || e.getMessage().equalsIgnoreCase(DIVISION_UNDEFINED)
                            || e.getMessage().contains(UNKNOWN_FUNCTION)
                            || e.getMessage().contains(MISMATCHED_PARENTHESES)
                            || e.getMessage().contains(FUNCTION_IF_EXPECTED_3_PARAMETERS))) {
                        exceptionalKpiMap.remove(counterKey);
                    } else {
                        exceptionalKpiMap.put(counterKey, formulaeString);
                    }
                }
            }
            return kpiMap;
        }

        private String replaceMetaDetail(String formulaeString, Map<String, String> metaMap) {
            if (formulaeString.contains("#$") || formulaeString.contains("(EQUALS(#")
                    || formulaeString.contains("(NOT_EQUALS(#)") || formulaeString.contains("LEFT(")
                    || formulaeString.contains("RIGHT(")) {
                final Pattern pattern = Pattern.compile("EQUALS(.*?),");
                final Matcher matcher = pattern.matcher(formulaeString);
                while (matcher.find()) {
                    String match = matcher.group(1).replace("(", "").replace(")", "");
                    String matchkey = StringUtils.substringBetween(match, "#", "#");
                    formulaeString = StringUtils.replace(formulaeString, "#" + matchkey + "#",
                            Symbol.QUOTE_STRING + metaMap.get(matchkey) + Symbol.QUOTE_STRING);
                }
                if (formulaeString.contains("#$") && formulaeString.contains("$#")) {
                    final Pattern operatorPattern = Pattern.compile("#\\$(.*?)\\$#");
                    final Matcher operatorMatcher = operatorPattern.matcher(formulaeString);
                    while (operatorMatcher.find()) {
                        String matchkey = operatorMatcher.group(1).replace("(", "").replace(")", "");
                        formulaeString = StringUtils.replace(formulaeString, "#$" + matchkey + "$#",
                                metaMap.get(matchkey));
                    }
                }
            }
            return formulaeString;
        }

        public String getPreComputeExpBtwBrackets(String formulaeString, String function) {
            String preComputeExp = StringUtils.substringAfter(formulaeString, function + "(");
            if (preComputeExp.indexOf("(") > 0) {
                while (preComputeExp.indexOf("(") < preComputeExp.indexOf(")")) {
                    preComputeExp = preComputeExp.replaceFirst("\\(", "\\{").replaceFirst("\\)", "\\}");
                    if (preComputeExp.indexOf("(") < 0)
                        break;
                }
            }
            preComputeExp = StringUtils.substringBefore(preComputeExp, ")");
            preComputeExp = preComputeExp.replaceAll("\\{", "(").replaceAll("\\}", ")");
            return preComputeExp;
        }

        public String getFunctionsVal(String formulaeString, String function) {
            while (formulaeString.contains(function)) {
                String preComputeExp = getPreComputeExpBtwBrackets(formulaeString, function);
                com.enttribe.sparkrunner.util.Expression preComputeExpression = new com.enttribe.sparkrunner.util.Expression(
                        preComputeExp, true);
                String kpiValuePreComputeExp = preComputeExpression.eval();

                formulaeString = StringUtils.replace(formulaeString, function + "(" + preComputeExp + ")",
                        function + "(" + kpiValuePreComputeExp + ")");
                String formulaeFuncStr = function + "("
                        + StringUtils.substringBefore(StringUtils.substringAfter(formulaeString, function + "("), ")")
                        + ")";
                String formulaeFunc = function + "("
                        + StringUtils.substringBefore(StringUtils
                                .substringBefore(StringUtils.substringAfter(formulaeString, function + "("), ")"), ".")
                        + ")";
                com.enttribe.sparkrunner.util.Expression expression1 = new com.enttribe.sparkrunner.util.Expression(
                        formulaeFunc,
                        true);
                String kpiValueFunc = expression1.eval();
                formulaeString = StringUtils.replace(formulaeString, formulaeFuncStr, kpiValueFunc);
            }
            return formulaeString;
        }

        private void initializeKpiFormulaMap() {
            if (kpiFormulaFinalMap == null) {
                synchronized (SYNCHRONIZER) {
                    if (kpiFormulaFinalMap == null) {

                        colRemove = jobcontext.getParameter("columnsRemove");

                        ObjectMapper mapper = new ObjectMapper();
                        String json = jobcontext.getParameter("KPIFORMULA_MAPJSON");

                        try {
                            kpiFormulaFinalMap = mapper.readValue(json,
                                    new TypeReference<ConcurrentHashMap<String, Map<String, Object>>>() {
                                    });

                        } catch (Exception e) {
                            logger.error("Exception Occurred While Parsing KPI Formula Map JSON. Error: {}",
                                    e.getMessage());
                        }
                    }
                }
            }
            logger.error("KPI Formula Map Size: {}", kpiFormulaFinalMap.size());
        }

        @Override
        public String getName() {
            return "KPIEvaluatorBntv";
        }

        @Override
        public DataType getReturnType() {
            List<StructField> fields = new ArrayList<>();

            fields.add(DataTypes.createStructField(DOMAIN, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(VENDOR, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(TECHNOLOGY, DataTypes.StringType, true));

            if (jobcontext.getParameters().containsKey("moType")
                    && jobcontext.getParameter("moType").equalsIgnoreCase("MOTYPE")) {
                fields.add(DataTypes.createStructField("motype", DataTypes.StringType, true));
                fields.add(DataTypes.createStructField("moname", DataTypes.StringType, true));
            } else {

                fields.add(DataTypes.createStructField(DATA_LEVEL, DataTypes.StringType, true));
                fields.add(DataTypes.createStructField(DATE_SMALL, DataTypes.StringType, true));
            }

            fields.add(DataTypes.createStructField("nodename", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(TIMESTAMP, DataTypes.LongType, true));

            if (jobcontext.getParameters().containsKey("jobType")
                    && jobcontext.getParameter("jobType").equalsIgnoreCase("BBH")) {
                fields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
            }

            fields.add(DataTypes.createStructField("kpijson",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
            fields.add(DataTypes.createStructField("metajson",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
            fields.add(DataTypes.createStructField("networktype", DataTypes.StringType, true));

            return DataTypes.createStructType(fields);
        }

    }

    // ================= NodeAggrRowKeyConverter Started ===================== //

    public class NodeAggrRowKeyConverter implements
            UDF5<String, String, Long, scala.collection.immutable.Map<String, String>, String, List<Row>>, AbstractUDF {

        private static final long serialVersionUID = 1L;
        private final Logger logger = LoggerFactory.getLogger(NodeAggrRowKeyConverter.class);
        private JobContext jobContext;

        public NodeAggrRowKeyConverter() {
            super();
        }

        public NodeAggrRowKeyConverter(JobContext jobContext) {
            this.jobContext = jobContext;
        }

        @Override
        public List<Row> call(String aggregationLevel, String nename, Long ctime,
                scala.collection.immutable.Map<String, String> metaDataMap, String subAggregation) throws Exception {

            Map<String, String> metaInfoMapAsJavaMap = new HashMap<>(
                    CollectionConverters.MapHasAsJava(metaDataMap).asJava());

            return getCRCForPMGeoMapper(aggregationLevel, nename, metaInfoMapAsJavaMap, subAggregation, ctime);
        }

        private Map<String, String> updateMetaMap(Map<String, String> metaInfoMapAsJavaMap, String aggregationLevel) {
            String aggrValue = metaInfoMapAsJavaMap.get(aggregationLevel);
            if (neVsL3Map.containsKey(aggrValue)) {
                Map<String, String> map = neVsL3Map.get(aggrValue);
                map.entrySet().forEach(e -> metaInfoMapAsJavaMap.put(e.getKey(), e.getValue()));
            }
            return metaInfoMapAsJavaMap;
        }

        private List<Row> getCRCForPMGeoMapper(String aggregationLevel, String nename, Map<String, String> metaMap,
                String subAggregation, Long time) {

            List<Row> rowList = new ArrayList<>();
            try {

                logger.error("[DEBUG] Aggregation Level: {}, Meta Info={}", aggregationLevel, metaMap);

                boolean bandAggr = false;
                boolean nestatusAggr = false;
                boolean subAggrEnable = false;
                if (subAggregation != null && !subAggregation.isEmpty()) {
                    subAggrEnable = true;
                    if (subAggregation.contains("BAND") && !StringUtils.isEmpty(metaMap.get("BND"))) {
                        bandAggr = true;
                    }
                    if (subAggregation.contains("NESTATUS") && !StringUtils.isEmpty(metaMap.get("NS"))
                            && metaMap.get("NS").equalsIgnoreCase("ONAIR")) {
                        nestatusAggr = true;
                    }
                }

                logger.error(
                        "Aggregation Level: {}, SubAggregation: {}, BandAggr: {}, NestatusAggr: {}, SubAggrEnable: {}",
                        aggregationLevel, subAggregation, bandAggr, nestatusAggr, subAggrEnable);

                switch (aggregationLevel) {
                    case "Node_Core":
                        metaMap.put("datalevel", "NODE");
                        Map<String, String> metaMapCore = updateMetaInfoMap(new HashMap<>(metaMap), "NEID#CEL#PMEMSID");
                        metaMapCore.put("ENB", metaMap.get("VNF"));
                        metaMapCore.put("NAM", metaMap.get("VNF"));
                        metaMapCore.put("displaynodename", metaMapCore.get("ENB"));
                        metaMapCore.put("parquetLevel", "VNF");
                        convertRowKey("VNFNEID", metaMapCore, time, rowList, subAggregation, subAggrEnable,
                                Symbol.EMPTY_STRING, true, bandAggr, nestatusAggr, false);
                        break;
                    case "Node":

                        // =========================== Executed Node ===========================

                        initializedNeVsL3Map();
                        metaMap.put("datalevel", "NODE");
                        Map<String, String> metaMapENB = updateMetaInfoMap(new HashMap<>(metaMap), "CEL#NEID#NET#EGID");
                        Map<String, String> metaMapH2 = updateMetaInfoMap(new HashMap<>(metaMapENB),
                                "ENB#ENB_NEID#NEL#SC#SFID");
                        Map<String, String> metaMapRIM = updateMetaInfoMap(new HashMap<>(metaMap),
                                "CEL#NEID#NET#EGID#SFID");
                        Map<String, String> metaMapH1 = updateMetaInfoMap(new HashMap<>(metaMapH2), "H2#H2_NEID");

                        // =========================== Executed ===========================

                        metaMapENB.put("NAM", metaMapENB.get("ENB"));
                        metaMapENB.put("displaynodename", metaMapENB.get("ENB"));

                        if (metaMapENB != null) {
                            String enbNeid = metaMapENB.getOrDefault("ENB_NEID", null);

                            if (enbNeid == null) {
                                String childNeid = metaMapENB.getOrDefault("NEID", null);
                                if (childNeid != null) {
                                    enbNeid = childNeid.split("_")[0];
                                }
                            }
                            metaMapENB.put("NEID", enbNeid);
                        }

                        if (metaMapENB.containsKey("parquetLevel")) {
                            metaMapENB.put("parquetLevel", metaMapENB.get("NODE_TYPE"));
                        }

                        // =========================== Executed ===========================

                        convertRowKey("ENB_NEID", metaMapENB, time, rowList, subAggregation, subAggrEnable,
                                Symbol.EMPTY_STRING, true, bandAggr, nestatusAggr, false);

                        // =========================== Executed ===========================

                        if (metaMapRIM.containsKey("RIM")) {
                            metaMapRIM.put("NAM", metaMapRIM.get("RIM"));
                            metaMapRIM.put("displaynodename", metaMapRIM.get("RIM"));
                            metaMapRIM.put("parquetLevel", "RIM");

                            convertRowKey("RIM_NEID", metaMapRIM, time, rowList, subAggregation, subAggrEnable,
                                    Symbol.EMPTY_STRING, true, bandAggr, nestatusAggr, false);

                        }

                        if (metaMapH2.containsKey("H2")) {
                            metaMapH2.put("NAM", metaMapH2.get("H2"));
                            metaMapH2.put("displaynodename", metaMapH2.get("H2"));
                            metaMapH2.put("parquetLevel", "H2");
                            convertRowKey("H2_NEID", metaMapH2, time, rowList, subAggregation, subAggrEnable,
                                    Symbol.EMPTY_STRING, true, bandAggr, nestatusAggr, false);
                        }

                        if (metaMapH2.containsKey("CUUP")) {
                            metaMapH2.put("NAM", metaMapH2.get("NEID"));
                            metaMapH2.put("displaynodename", metaMapH2.get("CUUP"));
                            metaMapH2.put("parquetLevel", "CUUP");
                            convertRowKey("NEID", metaMapH2, time, rowList, subAggregation, subAggrEnable,
                                    Symbol.EMPTY_STRING, true, bandAggr, nestatusAggr, false);
                        }

                        if (neVsL3Map != null) {
                            metaMapH1 = updateMetaMap(metaMapH1, "H1");
                        }
                        metaMapH1.put("NAM", metaMapH1.get("H1"));
                        metaMapH1.put("displaynodename", metaMapH1.get("H1"));
                        metaMapH1.put("parquetLevel", "H1");

                        convertRowKey("H1_NEID", metaMapH1, time, rowList, subAggregation, subAggrEnable,
                                Symbol.EMPTY_STRING, true, bandAggr, nestatusAggr, false);
                        break;
                    /********************************************************************************************************/

                    case "Geography_Core":
                        metaMap.put("datalevel", "GEOGRAPHY");
                        Map<String, String> metaMapGeoCore = updateMetaInfoMap(new HashMap<>(metaMap),
                                "CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#SC#SFID");
                        Map<String, String> metaMapL1Core = updateMetaInfoMap(new HashMap<>(metaMapGeoCore),
                                "NEID#CEL#PMEMSID#ENB#VNF#VNFNEID");
                        Map<String, String> metaMapL0Core = new HashMap<>(metaMapL1Core);

                        String rowKeyAppenderCore = getRowKeyAppender(metaMap, "RowKeyAppender");
                        /********************************************************************************************************/
                        /**
                         * ------------------------------------L1
                         * level---------------------------------------------------------
                         */
                        metaMapL1Core.put("NAM", metaMapL1Core.get("L1"));
                        metaMapL1Core.put("displaynodename", metaMapL1Core.get("DL1"));
                        metaMapL1Core.put("parquetLevel", "L1");
                        convertRowKey("L1", metaMapL1Core, time, rowList, subAggregation, subAggrEnable,
                                rowKeyAppenderCore, true, bandAggr, nestatusAggr, false);
                        /**
                         * -----------------------------------------------------------------------------------------------------
                         */
                        /********************************************************************************************************/
                        /**
                         * ------------------------------------L0
                         * level---------------------------------------------------------
                         */
                        metaMapL0Core.put("NAM", COUNTRY);
                        metaMapL0Core.put("displaynodename", COUNTRY);
                        metaMapL0Core.put("L1", COUNTRY);
                        metaMapL0Core.put("DL1", COUNTRY);
                        metaMapL0Core.put("parquetLevel", "L0");
                        convertRowKey("L1", metaMapL0Core, time, rowList, subAggregation, subAggrEnable,
                                rowKeyAppenderCore, true, bandAggr, nestatusAggr, false);
                        break;
                    /********************************************************************************************************/
                    case "H1/H2": {
                        logger.error("inside H1/H2 : {}", metaMap);
                        Map<String, String> metaMap_H2 = updateMetaInfoMap(new HashMap<>(metaMap),
                                "CEL#NEID#NET#ENB#ENB_NEID#NEL");
                        Map<String, String> metaMap_H1 = updateMetaInfoMap(new HashMap<>(metaMap_H2), "H2#H2_NEID");
                        logger.error("metaMap_H1 H1 : {}", metaMap_H1.toString());
                        logger.error("metaMap_H2 H2 : {}", metaMap_H2.toString());

                        if (metaMap_H2.containsKey("H2")) {
                            String rowKeyAppender = getRowKeyAppender(metaMap_H2, "RowKeyAppender");
                            logger.error("H2 rowKeyAppender: {}", rowKeyAppender);

                            metaMap_H2.put("NAM", metaMap_H2.get("H2"));
                            metaMap_H2.put("DNAM", metaMap_H2.get("H2"));
                            metaMap_H2.put("parquetLevel", "H2");
                            convertRowKey("H2_NEID", metaMap_H2, time, rowList, subAggregation, subAggrEnable,
                                    rowKeyAppender,
                                    true, bandAggr, nestatusAggr, false);
                        }

                        if (metaMap_H1.containsKey("H1")) {
                            if (neVsL3Map != null) {
                                metaMap_H1 = updateMetaMap(metaMap_H1, "H1");
                            }

                            String rowKeyAppender = getRowKeyAppender(metaMap_H1, "RowKeyAppender");
                            logger.error("H1 rowKeyAppender : {}", rowKeyAppender);

                            metaMap_H1.put("NAM", metaMap_H1.get("H1"));
                            metaMap_H1.put("DNAM", metaMap_H1.get("H1"));
                            metaMap_H1.put("parquetLevel", "H1");

                            convertRowKey("H1_NEID", metaMap_H1, time, rowList, subAggregation, subAggrEnable,
                                    rowKeyAppender,
                                    true, bandAggr, nestatusAggr, false);
                        } else {
                            logger.error("metamap H1 and H2 Both Does Not Contains Key H1 and H2");
                        }
                    }
                        break;
                    case "Geography":

                        // =================== Executed Geography Level Aggregation ===================

                        metaMap.put("datalevel", "GEOGRAPHY");
                        Map<String, String> metaMapGeo = updateMetaInfoMap(new HashMap<>(metaMap),
                                "CEL#NEID#H2#H2_NEID#ENB#ENB_NEID#NET#H1#H1_NEID#EID#EGID#SC#SFID");
                        Map<String, String> metaMapNEL = updateMetaInfoMap(new HashMap<>(metaMapGeo), "GC#OG#DOG");
                        Map<String, String> metaMapGC = updateMetaInfoMap(new HashMap<>(metaMapGeo), "NEL");
                        Map<String, String> metaMapL4 = updateMetaInfoMap(new HashMap<>(metaMapGC), "GC");
                        Map<String, String> metaMapL3 = updateMetaInfoMap(new HashMap<>(metaMapL4), "L4#DL4");
                        Map<String, String> metaMapL2 = updateMetaInfoMap(new HashMap<>(metaMapL3), "L3#DL3");
                        Map<String, String> metaMapL1 = updateMetaInfoMap(new HashMap<>(metaMapL2), "L2#DL2");
                        Map<String, String> metaMapL0 = new HashMap<>(metaMapL1);
                        Map<String, String> metaMapPoly = updateMetaInfoMap(new HashMap<>(metaMapL1), "L1#DL1");
                        Map<String, String> metaMapOG = updateMetaInfoMap(metaMapGC, "L4#DL4#L3#DL3#DL2#L2#DL1#L1#GC");

                        String rowKeyAppender = getRowKeyAppender(metaMap, "RowKeyAppender");

                        /*
                         * =========================== L4 Level Aggregation ===========================
                         */

                        metaMapL4.put("NAM", metaMapL4.get("L4"));
                        metaMapL4.put("displaynodename", metaMapL4.get("DL4"));
                        metaMapL4.put("parquetLevel", "L4");
                        convertRowKey("L4", metaMapL4, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, true);

                        /*
                         * =========================== L3 Level Aggregation ===========================
                         */

                        metaMapL3.put("NAM", metaMapL3.get("L3"));
                        metaMapL3.put("displaynodename", metaMapL3.get("DL3"));
                        metaMapL3.put("parquetLevel", "L3");
                        convertRowKey("L3", metaMapL3, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, false);

                        /*
                         * =========================== L2 Level Aggregation ===========================
                         */

                        metaMapL2.put("NAM", metaMapL2.get("L2"));
                        metaMapL2.put("displaynodename", metaMapL2.get("DL2"));
                        metaMapL2.put("parquetLevel", "L2");
                        convertRowKey("L2", metaMapL2, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, false);

                        /*
                         * =========================== L1 Level Aggregation ===========================
                         */

                        metaMapL1.put("NAM", metaMapL1.get("L1"));
                        metaMapL1.put("displaynodename", metaMapL1.get("DL1"));
                        metaMapL1.put("parquetLevel", "L1");
                        convertRowKey("L1", metaMapL1, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, false);

                        /*
                         * =========================== L0 Level Aggregation ===========================
                         */

                        metaMapL0.put("NAM", COUNTRY);
                        metaMapL0.put("displaynodename", COUNTRY);
                        metaMapL0.put("L1", COUNTRY);
                        metaMapL0.put("DL1", COUNTRY);
                        metaMapL0.put("parquetLevel", "L0");
                        convertRowKey("L1", metaMapL0, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, false);

                        /*
                         * =========================== OG Level Aggregation ===========================
                         */

                        metaMapOG.put("NAM", metaMapOG.get("OG"));
                        metaMapOG.put("displaynodename", metaMapOG.get("DOG"));
                        metaMapOG.put("parquetLevel", "OG");
                        convertRowKey("OG", metaMapOG, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, false);

                        /*
                         * =========================== NEL Level Aggregation ===========================
                         */

                        metaMapNEL.put("RowKeyAppender", null);
                        metaMapNEL.put("NAM", metaMapNEL.get("NEL"));
                        metaMapNEL.put("displaynodename", metaMapNEL.get("NEL"));
                        metaMapNEL.put("parquetLevel", "NEL");
                        convertRowKey("NEL", metaMapNEL, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, false);

                        /*
                         * =========================== GC Level Aggregation ===========================
                         */

                        metaMapGC.put("NAM", metaMapGC.get("GC"));
                        metaMapGC.put("displaynodename", metaMapGC.get("GC"));
                        metaMapGC.put("parquetLevel", "GC");
                        convertRowKey("GC", metaMapGC, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                                true, bandAggr, nestatusAggr, true);

                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                logger.error("Exception In NodeAggrRowKeyConverter: {}", e.getMessage());
            }

            return rowList;
        }

        private List<Row> convertRowKey(String aggregationLevel, Map<String, String> metaMap, Long time,
                List<Row> rowList,
                String subAggregation, boolean subAggrEnable, String rowKeyAppender, boolean isAllEnabledInSiteStatus,
                boolean bandAggr, boolean nestatusAggr, boolean architectLevel) {

            logger.error(
                    "Converting RowKey For AggrLevel: {}, MetaMap: {}, Time: {}, RowList: {}, SubAggregation: {}, SubAggrEnable: {}, RowKeyAppender: {}, IsAllEnabledInSiteStatus: {}, BandAggr: {}, NestatusAggr: {}, ArchitectLevel: {}",
                    aggregationLevel, metaMap, time, rowList, subAggregation, subAggrEnable, rowKeyAppender,
                    isAllEnabledInSiteStatus, bandAggr, nestatusAggr, architectLevel);

            String aggrVal = metaMap.get(aggregationLevel);

            if (bandAggr) {
                Map<String, String> tempMetaMap = new HashMap<>(metaMap);
                tempMetaMap.put("SAT", "Band");
                tempMetaMap.put("SAV", metaMap.get("BND"));
                tempMetaMap.put("NS", "ALL");
                rowList.add(createRow(aggrVal, time, tempMetaMap, false));
                if (Boolean.TRUE.equals(architectLevel))
                    rowList.add(createRow(aggrVal, time, tempMetaMap, architectLevel));
            }

            if (nestatusAggr) {
                Map<String, String> tempMetaMap = new HashMap<>(metaMap);
                tempMetaMap.put("SAT", "Status");
                tempMetaMap.put("SAV", metaMap.get("NS"));
                tempMetaMap.put("BND", "ALL");
                rowList.add(createRow(aggrVal, time, tempMetaMap, false));
                if (Boolean.TRUE.equals(architectLevel))
                    rowList.add(createRow(aggrVal, time, tempMetaMap, architectLevel));

                if (Arrays.asList("GC,OG,NEL").contains(aggregationLevel) && tempMetaMap.get("CR") != null
                        && tempMetaMap.get("NS").equalsIgnoreCase("ONAIR")) {
                    tempMetaMap.put("SAT", "Status_NCR");
                    tempMetaMap.put("SAV", tempMetaMap.get("NS") + Symbol.UNDERSCORE_STRING + tempMetaMap.get("CR"));
                    rowList.add(createRow(aggrVal, time, new HashMap<>(tempMetaMap), false));
                    if (Boolean.TRUE.equals(architectLevel))
                        rowList.add(createRow(aggrVal, time, new HashMap<>(tempMetaMap), architectLevel));
                }
            }

            if (bandAggr && nestatusAggr) {
                Map<String, String> tempMetaMap = new HashMap<>(metaMap);
                tempMetaMap.put("SAT", "Status_Band");
                tempMetaMap.put("SAV", metaMap.get("NS") + "_" + metaMap.get("BND"));
                rowList.add(createRow(aggrVal, time, tempMetaMap, architectLevel));
                if (Boolean.TRUE.equals(architectLevel))
                    rowList.add(createRow(aggrVal, time, tempMetaMap, architectLevel));
                if (Arrays.asList("GC,OG,NEL").contains(aggregationLevel) && tempMetaMap.get("CR") != null
                        && tempMetaMap.get("NS").equalsIgnoreCase("ONAIR")) {
                    tempMetaMap.put("SAT", "Status_Band_NCR");
                    tempMetaMap.put("SAV", tempMetaMap.get("NS") + Symbol.UNDERSCORE_STRING + tempMetaMap.get("BND")
                            + Symbol.UNDERSCORE_STRING + tempMetaMap.get("CR"));
                    rowList.add(createRow(aggrVal, time, new HashMap<>(tempMetaMap), false));
                    if (Boolean.TRUE.equals(architectLevel))
                        rowList.add(createRow(aggrVal, time, new HashMap<>(tempMetaMap), architectLevel));
                }
            }

            Map<String, String> tempaMetaMap = new HashMap<>(metaMap);
            if (subAggrEnable) {
                if (subAggregation.contains("BAND"))
                    tempaMetaMap.put("BND", "ALL");
                if (subAggregation.contains("NESTATUS") && isAllEnabledInSiteStatus)
                    tempaMetaMap.put("NS", "ALL");
            }
            if (tempaMetaMap.get("D") != null && tempaMetaMap.get("D").equalsIgnoreCase("RAN")) {
                tempaMetaMap.put("SAT", "ALL");
                tempaMetaMap.put("SAV", "ALL");
                tempaMetaMap.put("BND", "ALL");
            }

            // =========================== Executed ===========================

            rowList.add(createRow(aggrVal, time, tempaMetaMap, false));

            // =========================== Executed ===========================

            if (Boolean.TRUE.equals(architectLevel)) {
                rowList.add(createRow(aggrVal, time, tempaMetaMap, architectLevel));
            }
            return rowList;
        }

        private String getRowKeyAppender(Map<String, String> metaJsonMap, String inputKey) {
            if (metaJsonMap == null)
                return Symbol.EMPTY_STRING;

            String appendValue = metaJsonMap.get(inputKey);
            if (StringUtils.isNotEmpty(appendValue)) {
                return Symbol.UNDERSCORE_STRING + appendValue;
            }

            return Symbol.EMPTY_STRING;
        }

        private void initializedNeVsL3Map() {
            if (neVsL3Map == null) {
                synchronized (SYNCHRONIZER) {
                    if (neVsL3Map == null) {
                        String json = jobContext.getParameter("NE_VS_L3_MAPJSON");
                        neVsL3Map = strToMap(json);
                    }
                }
            }
            logger.error("NeVsL3Map Size: {}", neVsL3Map.size());
        }

        private <K, V> Map<K, V> strToMap(String stringToConvert) {
            try {
                return mapper.readValue(stringToConvert, new TypeReference<Map<K, V>>() {
                });
            } catch (Exception e) {
                logger.error("Exception In Converting String To Map: {}, Message: {}", stringToConvert, e.getMessage());
            }
            return new HashMap<K, V>();
        }

        private Row createRow(String nodeName, Long time, Map<String, String> metaJsonMap,
                boolean architectLevel) {

            Map<String, String> contextMap = jobContext.getParameters();

            String domain = contextMap.get("DOMAIN");
            String vendor = contextMap.get("VENDOR");
            String technology = contextMap.get("TECHNOLOGY");
            String networktype = contextMap.get(NETWORK_TYPE);

            String level = metaJsonMap.containsKey("parquetLevel")
                    && metaJsonMap.get("parquetLevel") != null ? metaJsonMap.get("parquetLevel")
                            : null;

            logger.error("Input ArchitectLevel: {} And Received Level: {}", architectLevel, level);

            if (level != null && !level.equals(NODE)) {

                logger.error("Recieved Level Is Not Null And Level Not Equal To Node Level: {}, Node: {}", level, NODE);

                // level = level + (metaJsonMap.containsKey(ROW_KEY_APPENDER)
                // && StringUtils.isNotEmpty(metaJsonMap.get(ROW_KEY_APPENDER))
                // ? Symbol.UNDERSCORE_STRING + StringUtils.substringAfter(
                // metaJsonMap.get(ROW_KEY_APPENDER), Symbol.UNDERSCORE_STRING)
                // : Symbol.EMPTY_STRING);

                // level = level + (Boolean.TRUE.equals(architectLevel)
                // && StringUtils.isNotEmpty(metaJsonMap.get("ARCHITECH"))
                // ? Symbol.UNDERSCORE_STRING + metaJsonMap.get("ARCHITECH")
                // : Symbol.EMPTY_STRING);
                // level = level
                // + (metaJsonMap.containsKey(SAV) && !StringUtils.isEmpty(metaJsonMap.get(SAV))
                // && !metaJsonMap.get(SAV).equalsIgnoreCase(ALL)
                // ? Symbol.UNDERSCORE_STRING + metaJsonMap.get(SAV)
                // : Symbol.EMPTY_STRING);

                level = metaJsonMap.get("NODE_TYPE") != null ? metaJsonMap.get("NODE_TYPE") : level;
            } else {
                logger.error("Recieved Level Is Null, So Keeping The Level As It Is: {}", level);
            }

            logger.error(
                    "Returning Row With NodeName: {}, Technology: {}, Domain: {}, Vendor: {}, Networktype: {}, Level: {}, Time: {}, MetaJsonMap: {}",
                    nodeName, technology, domain, vendor, networktype, level, time, metaJsonMap);

            return RowFactory.create(nodeName, technology, domain, vendor, networktype, level, time,
                    metaJsonMap);
        }

        private Map<String, String> updateMetaInfoMap(Map<String, String> metaInfo, String removeKey) {
            if (metaInfo == null || StringUtils.isEmpty(removeKey)) {
                return metaInfo;
            }

            for (String key : removeKey.split(Symbol.HASH_STRING)) {
                metaInfo.remove(key);
            }

            return metaInfo;
        }

        @Override
        public String getName() {
            return "NodeAggrRowKeyConverter";
        }

        @Override
        public DataType getReturnType() {
            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField(NODENAME, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(TECHNOLOGY, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(DOMAIN, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(VENDOR, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(NETWORK_TYPE, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(LEVEL, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(CTIME, DataTypes.LongType, true));
            fields.add(DataTypes.createStructField(METADATA,
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
            return DataTypes.createArrayType(DataTypes.createStructType(fields));
        }

    }

    public class TimeAggrRowKeyConverter
            implements UDF3<Long, String, scala.collection.immutable.Map<String, String>, Row>, AbstractUDF {

        private static final long serialVersionUID = 1L;

        public TimeAggrRowKeyConverter() {
        }

        @Override
        public String getName() {
            return "TimeAggrRowKeyConverter";
        }

        @Override
        public DataType getReturnType() {
            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField(CTIME, DataTypes.LongType, true));
            fields.add(DataTypes.createStructField(METADATA,
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
            return DataTypes.createStructType(fields);
        }

        @Override
        public Row call(Long ctime, String jobType, scala.collection.immutable.Map<String, String> metaData)
                throws Exception {

                    
            logger.error("TimeAggrRowKeyConverter UDF Called With Input: Time: {}, JobType: {}, MetaData: {}", ctime, jobType, metaData.toString());

            Calendar instance = Calendar.getInstance();
            instance.setTimeInMillis(ctime);

            Map<String, String> metaJsonMap = new THashMap<>(
                    scala.jdk.CollectionConverters.MapHasAsJava(metaData).asJava());

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            String date = simpleDateFormat.format(instance.getTime());

            switch (jobType.toUpperCase()) {
                case QUARTERLY_STRING:
                    instance.set(Calendar.SECOND, 0);
                    metaJsonMap.put(DATE_SMALL, date);
                    break;
                case HOURLY_STRING:
                    instance.set(Calendar.SECOND, 0);
                    instance.set(Calendar.MINUTE, 0);
                    metaJsonMap.put(DATE_SMALL, date);
                    metaJsonMap.put(HR_STRING, StringUtils.substring(metaJsonMap.get(HR_STRING), 0, 2));
                    metaJsonMap.put(TIME_STRING, metaJsonMap.get(HR_STRING) + "00");
                    break;
                case DAILY_STRING:
                    instance.set(Calendar.SECOND, 0);
                    instance.set(Calendar.MINUTE, 0);
                    instance.set(Calendar.HOUR_OF_DAY, 0);
                    metaJsonMap.put(DATE_SMALL, StringUtils.substring(date, 0, 6));
                    metaJsonMap.put(TIME_STRING, FOURZERO_STRING);
                    metaJsonMap.put(PT_STRING, FOURZERO_STRING);
                    metaJsonMap.remove(HR_STRING);
                    break;
                case NBH_STRING:
                case BBH_STRING:
                    instance.set(Calendar.SECOND, 0);
                    instance.set(Calendar.MINUTE, 0);
                    instance.set(Calendar.HOUR_OF_DAY, 0);
                    metaJsonMap.put(DATE_SMALL, StringUtils.substring(date, 0, 6));
                    metaJsonMap.remove(HR_STRING);
                    break;
                case WEEKLY_STRING:
                    instance.add(Calendar.DAY_OF_MONTH, -1);
                    instance.set(Calendar.SECOND, 0);
                    instance.set(Calendar.MINUTE, 0);
                    instance.set(Calendar.HOUR_OF_DAY, 0);
                    instance.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                    metaJsonMap.put(DATE_SMALL, StringUtils.substring(date, 0, 4));
                    metaJsonMap.put(TIME_STRING, FOURZERO_STRING);
                    metaJsonMap.put(PT_STRING, FOURZERO_STRING);
                    metaJsonMap.put(META_DATE, FOURZERO_STRING);
                    metaJsonMap.remove(HR_STRING);
                    metaJsonMap.remove(DT_STRING);
                    break;
                case MONTHLY_STRING:
                    instance.set(Calendar.SECOND, 0);
                    instance.set(Calendar.MINUTE, 0);
                    instance.set(Calendar.HOUR_OF_DAY, 0);
                    instance.set(Calendar.DAY_OF_MONTH, 1);
                    metaJsonMap.put(DATE_SMALL, StringUtils.substring(date, 0, 4));
                    break;
                case "FIVEMIN":
                    instance.set(Calendar.SECOND, 0);
                    int unroundedMinutes = instance.get(Calendar.MINUTE);
                    int roundedMinutes = (unroundedMinutes / 5) * 5;
                    instance.set(Calendar.MINUTE, roundedMinutes);

                    // Set meta fields
                    metaJsonMap.put(DATE_SMALL, date); // e.g., "20250722"

                    String hour = String.format("%02d", instance.get(Calendar.HOUR_OF_DAY));
                    String minute = String.format("%02d", instance.get(Calendar.MINUTE));
                    String hhmm = hour + minute;

                    metaJsonMap.put(HR_STRING, hhmm); // ✅ e.g. "1630"
                    metaJsonMap.put(TIME_STRING, hhmm); // ✅ same as above
                    break;

                case "5 MIN":
                    instance.set(Calendar.SECOND, 0);

                    int unroundedMinutes2 = instance.get(Calendar.MINUTE);
                    int roundedMinute2 = (unroundedMinutes2 / 5) * 5;
                    instance.set(Calendar.MINUTE, roundedMinute2);

                    metaJsonMap.put(DATE_SMALL, date); // e.g., "20250722"

                    String hour2 = String.format("%02d", instance.get(Calendar.HOUR_OF_DAY));
                    String minute2 = String.format("%02d", instance.get(Calendar.MINUTE));
                    String hhmm2 = hour2 + minute2;

                    metaJsonMap.put(HR_STRING, hhmm2); // ✅ e.g. "1630"
                    metaJsonMap.put(TIME_STRING, hhmm2); // ✅ same as above
                    break;

                default:
                    break;
            }

            logger.info("Generated meta data: {} for time: {}", metaJsonMap, instance.getTimeInMillis());

            return RowFactory.create(instance.getTimeInMillis(), metaJsonMap);
        }
    }

    public class CreateMetaData implements UDF5<String, String, String, String, String, List<Row>>, AbstractUDF {

        private static final long serialVersionUID = 1L;
        private Logger logger = LoggerFactory.getLogger(CreateMetaData.class);

        private JobContext jobcontext;

        public CreateMetaData(JobContext jobcontext) {
            this.jobcontext = jobcontext;
        }

        @Override
        public List<Row> call(String neTypeRecieved, String finalKey, String geoDetailsRecieved, String jobType,
                String processingTime) throws Exception {

            logger.error("CreateMetaData UDF Called With Input: {}", finalKey);

            initializeNeTypeRowKeyAppenderMap();

            if (netypeRowKeyAppenderMap != null && netypeRowKeyAppenderMap.containsKey(neTypeRecieved)) {
                geoDetailsRecieved = geoDetailsRecieved + "##" + netypeRowKeyAppenderMap.get(neTypeRecieved);
            } else {
                geoDetailsRecieved = geoDetailsRecieved + "##" + "RowKeyAppender@";
            }

            String[] geoDetails = geoDetailsRecieved.split("##");
            Map<String, String> metaInfo = getMetaJsonDetails(geoDetails);
            Map<String, String> contextMap = jobcontext.getParameters();

            Long ctime = null;
            String domain = contextMap.get("DOMAIN");
            String vendor = contextMap.get("VENDOR");
            String technology = contextMap.get("TECHNOLOGY");
            String networktype = contextMap.get("NETWORK_TYPE");

            logger.error("Job Context Parameters - DOMAIN={}, VENDOR={}, TECHNOLOGY={}, NETWORK_TYPE={}", domain,
                    vendor, technology, networktype);

            String nodename = Symbol.EMPTY_STRING;
            String level = StringUtils.isNotEmpty(metaInfo.get("NET")) ? metaInfo.get("NET")
                    : Symbol.EMPTY_STRING; // INTERFACE
            String rowKey = null;

            List<Row> rowList = new ArrayList<>();

            if (finalKey != null) {

                nodename = metaInfo.get("NEID");
                String timeKey = StringUtils.substringAfterLast(finalKey, "##");

                rowKey = StringUtils.rightPad(StringUtils.getCRC(nodename), CRC_LENGTH, PADDING_STRING) + "##"
                        + timeKey;

                ctime = getTimeStampFromRowKey(timeKey, jobType);
                logger.error("Generated Epoch Timestamp: {} For Input TimeKey: {}", ctime, timeKey);

                metaInfo.put("emstype", contextMap.get("EMS_TYPE"));
                metaInfo.put("T", technology);

                metaInfo = setOtherDetails(jobType, timeKey, processingTime, metaInfo);
                metaInfo = setTimeDetails(jobType, timeKey, metaInfo);

                logger.error("Generated MetaInfo: {}", metaInfo.toString());

                Row row = RowFactory.create(
                        new Object[] { rowKey, nodename, technology, domain, vendor, networktype, level, ctime,
                                metaInfo });
                if (row != null) {
                    rowList.add(row);
                }
            } else {

                logger.error("Recieved FinalKey is NULL, So Creating Row With Default Values");

                Row row = RowFactory.create(
                        new Object[] { rowKey, nodename, technology, domain, vendor, networktype, level, ctime,
                                metaInfo });
                if (row != null) {
                    rowList.add(row);
                }
            }

            return rowList;
        }

        private void initializeNeTypeRowKeyAppenderMap() {
            if (netypeRowKeyAppenderMap == null) {
                synchronized (SYNCHRONIZER) {
                    if (netypeRowKeyAppenderMap == null) {
                        String json = jobcontext.getParameter("NETYPE_ROW_KEY_APPENDER_MAP");
                        netypeRowKeyAppenderMap = strToMap(json);
                    }
                }
            }
        }

        private <K, V> Map<K, V> strToMap(String json) {
            try {
                return mapper.readValue(json, new TypeReference<Map<K, V>>() {
                });
            } catch (Exception e) {
                logger.error("Exception In Converting String To Map: {}, Message: {}", json, e.getMessage());
            }
            return new HashMap<K, V>();
        }

        public long getTimeStampFromRowKey(String rowKey, String frequency) {
            try {
                if (rowKey == null || rowKey.trim().isEmpty()) {
                    return getCurrentTimestamp();
                }

                if (frequency == null || frequency.trim().isEmpty()) {
                    return getCurrentTimestamp();
                }

                switch (frequency.toUpperCase()) {
                    case QUARTERLY:
                        try {
                            return DateUtils.parse(QUARTERLY_DATE_FORMATE, rowKey).getTime();
                        } catch (Exception e) {
                            logger.error("Failed to parse quarterly rowKey: {}, error: {}", rowKey, e.getMessage());
                            return getCurrentTimestamp();
                        }

                    case "HOURLY":
                        try {
                            return roundToNextQuarterHour(rowKey);
                        } catch (Exception e) {
                            logger.error("Failed in roundToNextQuarterHour for rowKey: {}, error: {}", rowKey,
                                    e.getMessage());
                            return getCurrentTimestamp();
                        }

                    default:
                        logger.error("Unhandled frequency type: {}", frequency);
                        return getCurrentTimestamp();
                }
            } catch (Exception e) {
                logger.error("Unexpected error in getTimeStampFromRowKey. RowKey: {}, Frequency: {}, Error: {}",
                        rowKey, frequency, e.getMessage());
                return getCurrentTimestamp();
            }
        }

        private long getCurrentTimestamp() {
            return new Date().getTime();
        }

        private Map<String, String> setTimeDetails(String jobType, String timeKey, Map<String, String> metaInfo) {
            if (jobType.equalsIgnoreCase("HOURLY")) {
                timeKey = timeKey + "00";
            }
            final SimpleDateFormat inputFormat = new SimpleDateFormat("yyMMddHHmmss");
            final SimpleDateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");

            try {
                Date date = inputFormat.parse(timeKey);
                metaInfo.put("DT", outputFormat.format(date));

                String hour = StringUtils.substring(timeKey, 6, 8);
                if ("HOURLY".equalsIgnoreCase(jobType)) {
                    metaInfo.put("HR", hour);
                } else if ("QUARTERLY".equalsIgnoreCase(jobType)) {
                    metaInfo.put("HR", hour + StringUtils.substring(timeKey, 8, 10));
                }

            } catch (ParseException e) {
                logger.error("Failed to parse timeKey [{}] using format [yyMMddHHmmss]: {}", timeKey, e.getMessage());
            } catch (Exception e) {
                logger.error("Unexpected Error in SetTimeDetails for JobType [{}] and TimeKey [{}]: {}", jobType,
                        timeKey, e.getMessage());
            }

            return metaInfo;
        }

        private Map<String, String> setOtherDetails(String jobType, String timeKey, String processingTime,
                Map<String, String> metaInfo) {
            if (jobType == null || jobType.isEmpty() || timeKey == null || timeKey.length() < 6 || metaInfo == null) {
                return metaInfo;
            }

            metaInfo.put("parquetLevel", "Cell");
            metaInfo.put("Date", timeKey.substring(0, 6));
            metaInfo.put("Time", getTimeByJobType(jobType, timeKey));
            metaInfo.put("PT", processingTime);

            return metaInfo;
        }

        private String getTimeByJobType(String jobType, String timeKey) {
            if (jobType == null || timeKey == null || timeKey.length() < 10) {
                return null;
            }

            switch (jobType.toUpperCase()) {
                case "QUARTERLY":
                    return getTimeForQuaterly(timeKey);
                case "HOURLY":
                    return timeKey.substring(6, 8) + "00";
                case "DAILY":
                case "WEEKLY":
                case "MONTHLY":
                    return "0000";
                default:
                    return null;
            }
        }

        public static long roundToNextQuarterHour(String rowKey) {

            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmm");
                LocalDateTime localDateTime = LocalDateTime.parse(rowKey, formatter);
                return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (Exception e) {
                System.err.println("Invalid Date Time Format: " + rowKey + " | Error: " + e.getMessage());
                return System.currentTimeMillis();
            }
        }

        private String getTimeForQuaterly(String timeKey) {
            int min = (timeKey.charAt(8) - '0') * 10 + (timeKey.charAt(9) - '0');
            String hour = timeKey.substring(6, 8);

            switch (min / 15) {
                case 0:
                    return hour + "00";
                case 1:
                    return hour + "15";
                case 2:
                    return hour + "30";
                case 3:
                    return hour + "45";
                default:
                    return null;
            }
        }

        private Map<String, String> getMetaJsonDetails(String[] inputGeoDetails) {
            Map<String, String> metaJsonDetailsMap = new HashMap<>();
            for (String geography : inputGeoDetails) {
                String geoKey = StringUtils.substringBefore(geography, "@");
                String geoValue = StringUtils.substringAfter(geography, "@");
                if (!geoValue.equalsIgnoreCase("null"))
                    metaJsonDetailsMap.put(geoKey, geoValue);
            }
            return metaJsonDetailsMap;
        }

        @Override
        public String getName() {
            return "CreateMetaData";
        }

        @Override
        public DataType getReturnType() {
            List<StructField> fields = new ArrayList<>();

            fields.add(DataTypes.createStructField(ROWKEY, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(NODENAME, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(TECHNOLOGY, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(DOMAIN, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(VENDOR, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(NETWORK_TYPE, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(LEVEL, DataTypes.StringType, true));
            fields.add(DataTypes.createStructField(CTIME, DataTypes.LongType, true));
            fields.add(DataTypes.createStructField(METADATA,
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
            return DataTypes.createArrayType(DataTypes.createStructType(fields));
        }

    }

}