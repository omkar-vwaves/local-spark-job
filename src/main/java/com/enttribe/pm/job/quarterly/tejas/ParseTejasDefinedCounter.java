package com.enttribe.pm.job.quarterly.tejas;

//package com.enttribe.pm.udfs;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.substring;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.enttribe.sparkrunner.util.Utils;

import scala.jdk.CollectionConverters;

public class ParseTejasDefinedCounter implements
        UDF2<String, scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>, List<Row>>,
        AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ParseTejasDefinedCounter.class);
    private JobContext jobcontext;
    private static final String CATEGORY_COUNTER_MAPJSON = "CATEGORY_COUNTER_MAPJSON";
    private static Map<String, List<Map<String, String>>> counterCategoryMap = null;
    private static LinkedHashMap<String, String> counterVariableIndexMap = null;
    private static Object SYNCHRONIZER = new Object();

    static long startTime;
    static {
        // ===============
        startTime = System.currentTimeMillis();
    }

    public ParseTejasDefinedCounter(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public ParseTejasDefinedCounter() {
    }

    @Override
    public List<Row> call(String processingTime,
            scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>> parsedCounterMap)
            throws Exception {

        // logger.info("Inside class ParseWDMLTEDefinedCounter : processingTime {},
        // parsedCounterMap {} ", processingTime , parsedCounterMap.size());

        // java.util.Map<String, scala.collection.immutable.Map<String, String>>
        // rowDataFinalMap = scala.collection.JavaConversions
        // .mapAsJavaMap(parsedCounterMap);

        java.util.Map<String, scala.collection.immutable.Map<String, String>> rowDataFinalMap = CollectionConverters
                .MapHasAsJava(parsedCounterMap).asJava();

        getCounterVariableIndexMap();
        getCounterMap();
        List<Row> fileContent = new ArrayList<>();
        try {
            parse(fileContent, rowDataFinalMap, processingTime);

        } catch (Exception e) {
            logger.error("Exception inside ParseDefinedCounter {}", Utils.getStackTrace(e));
        }
        // logger.info("inside ParseWDMLTEDefinedCounter filecontent is
        // {}",fileContent);
        // ================
        long endTime = System.currentTimeMillis();
        logger.info("Time taken by ParseWDMLTEDefinedCounter===> {} milliseconds", endTime - startTime);
        return fileContent;
    }

    public void parse(List<Row> fileContent,
            Map<String, scala.collection.immutable.Map<String, String>> rowDataFinalMap, String processingTime)
            throws IOException {

        for (String key : rowDataFinalMap.keySet()) {

            java.util.Map<String, String> counterValueMap = CollectionConverters
                    .MapHasAsJava(rowDataFinalMap.get(key)).asJava();

            String[] categoryKeyArray = key.split("##");
            String category = categoryKeyArray[0];
            String measObjLdn = categoryKeyArray[1];
            String dateTime = categoryKeyArray[2];
            // measObjLdn = substringAfter(measObjLdn, Symbol.SLASH_FORWARD_STRING);
            // String pmemsId = substringBefore(measObjLdn,
            // Symbol.SLASH_FORWARD_STRING).split(Symbol.HYPHEN_STRING)[1];
            String pmemsId = measObjLdn;
            if (measObjLdn.contains("LNCEL")) {
                pmemsId = pmemsId + Symbol.UNDERSCORE_STRING
                        + substringAfterLast(substringBefore(measObjLdn, Symbol.COMMA_STRING), Symbol.HYPHEN_STRING);
            }
            parseDefinedCounter(fileContent, dateTime, category, pmemsId, processingTime, counterValueMap);
        }

    }

    public void parseDefinedCounter(List<Row> fileContent, String dateTime, String category, String pmemsId,
            String processingTime, Map<String, String> counterValueMap) {
        try {
            String finalKey = pmemsId + "##" + substring(dateTime, 2);
            Object[] normalRow = new Object[counterVariableIndexMap.size() + 2];
            normalRow[0] = finalKey;
            normalRow[1] = processingTime;
            if (counterCategoryMap.containsKey(category)) {
                getValueMapForDefinedCounter(category, counterValueMap, normalRow);
                fileContent.add(RowFactory.create(normalRow));
            }
        } catch (Exception e) {
            logger.error("Execption inside parseDefinedCounter {}", Utils.getStackTrace(e));
        }
    }

    private void getValueMapForDefinedCounter(String category, Map<String, String> counterValueMap,
            Object[] normalRow) {
        List<Map<String, String>> list = counterCategoryMap.get("CELLAVAILABLETIME");
        if (list != null && category.equalsIgnoreCase("LTE_Cell_Load")) {
            for (Map<String, String> item : list) {
                // logger.info("getting the counter header name before if
                // "+item.get("CounterName"));
                if (item.get("CounterHeaderName").equalsIgnoreCase("Granularity")) {
                    Integer index = Integer.valueOf(counterVariableIndexMap.get(item.get("CounterColumnkey")));
                    // logger.info("getting the counter header name after
                    // if"+item.get("CounterName"));

                    // logger.info("getting the counter variable index
                    // "+counterVariableIndexMap.get(item.get("CounterColumnkey")));

                    normalRow[index] = 3600.0;
                }
            }
        }
        List<Map<String, String>> derivedColumnList = counterCategoryMap.get(category);
        for (Map<String, String> wrapper : derivedColumnList) {
            String value = counterValueMap.get(wrapper.get("RawFileCounterName"));
            // logger.info("value is :{}", value);
            if (value != null && isNumeric(value)) {
                Double column = createCustomizedColumns(value, wrapper);
                Integer index = Integer.valueOf(counterVariableIndexMap.get(wrapper.get("CounterColumnkey")));
                normalRow[index] = Double.valueOf(column);
            }
        }
    }

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public Double createCustomizedColumns(String value, Map<String, String> wrapper) {
        if (equalsIgnoreCase(wrapper.get("NodeAgg"), "COUNT")) {
            value = "1";
        }
        return Double.valueOf(value);
    }

    private void getCounterMap() {
        if (counterCategoryMap == null) {
            synchronized (SYNCHRONIZER) {
                if (counterCategoryMap == null) {
                    logger.info("Inside getCounterMap ");
                    ObjectMapper mapper = new ObjectMapper();
                    String json = jobcontext.getParameter(CATEGORY_COUNTER_MAPJSON);
                    logger.info("CATEGORY_COUNTER_MAPJSON : {} ", json);
                    try {
                        counterCategoryMap = mapper.readValue(json,
                                new TypeReference<Map<String, List<Map<String, String>>>>() {
                                });
                    } catch (Exception e) {
                        logger.error("Exception occured in method @getCounterMap in class @Ericsson- {}",
                                ExceptionUtils.getStackTrace(e));
                    }
                    // logger.info("counterCategoryMap Size - {}", counterCategoryMap.size());
                }
            }
        }
    }

    private void getCounterVariableIndexMap() {
        if (counterVariableIndexMap == null) {
            synchronized (SYNCHRONIZER) {
                if (counterVariableIndexMap == null) {
                    // logger.info("Inside getCounterVariableIndexMap ");
                    String parameter = jobcontext.getParameter("COUNTER_VARIABLE_INDEX");

                    logger.info("COUNTER_VARIABLE_INDEX : {} ", parameter);

                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        counterVariableIndexMap = mapper.readValue(parameter,
                                new TypeReference<LinkedHashMap<String, String>>() {
                                });
                    } catch (Exception e) {
                        logger.error("Exception occured in method @getCounterVariableIndexMap in class @Ericsson- {}",
                                ExceptionUtils.getStackTrace(e));
                    }
                    // logger.info("counterVariableIndexMap Size - {}",
                    // counterVariableIndexMap.size());
                }
            }
        }
    }

    @Override
    public String getName() {
        return "ParseDefinedCounter";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("PT", DataTypes.StringType, true));
        getCounterVariableIndexMap();
        for (Map.Entry<String, String> entry : counterVariableIndexMap.entrySet()) {
            fields.add(DataTypes.createStructField(entry.getKey(), DataTypes.DoubleType, true));
        }
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

}