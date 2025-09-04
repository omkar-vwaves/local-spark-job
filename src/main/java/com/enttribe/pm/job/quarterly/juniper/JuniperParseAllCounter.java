package com.enttribe.pm.job.quarterly.juniper;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.substring;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.jdk.CollectionConverters;

public class JuniperParseAllCounter implements
        UDF2<String, scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>, List<Row>>,
        AbstractUDF {

    private static Logger logger = LoggerFactory.getLogger(JuniperParseAllCounter.class);
    public JobContext jobcontext;
    private static final String ALL_CATEGORY_COUNTER_MAPJSON = "ALL_CATEGORY_COUNTER_MAPJSON";
    private static Map<String, List<Map<String, String>>> allCounterCategoryMap = null;
    private static final Object SYNCHRONIZER = new Object();

    public JuniperParseAllCounter(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public JuniperParseAllCounter() {
    }

    @Override
    public List<Row> call(String processingTime,
            scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>> parsedCounterMap)
            throws Exception {
        List<Row> fileContent = new ArrayList<>();
        getAllCategoryCounterMap();
        try {
            java.util.Map<String, scala.collection.immutable.Map<String, String>> rowDataFinalMap = CollectionConverters
                    .MapHasAsJava(parsedCounterMap).asJava();
            parseAll(fileContent, rowDataFinalMap, processingTime, jobcontext);
        } catch (Exception e) {
            logger.error("Exception Occurred While Parsing All Counters In Call Method, Message: {}", e.getMessage(),
                    e);
        }
        return fileContent;
    }

    private void parseAll(List<Row> fileContent,
            Map<String, scala.collection.immutable.Map<String, String>> rowDataFinalMap, String processingTime,
            JobContext jobcontext) {
        try {
            for (String key : rowDataFinalMap.keySet()) {
                java.util.Map<String, String> counterValueMap = CollectionConverters
                        .MapHasAsJava(rowDataFinalMap.get(key)).asJava();
                String[] categoryKeyArray = key.split("##");
                String category = categoryKeyArray[0];
                String pmemsId = categoryKeyArray[1];
                String dateTime = categoryKeyArray[2];
                parseAllCounter(fileContent, dateTime, category, pmemsId, processingTime, counterValueMap, jobcontext);
            }
        } catch (Exception e) {
            logger.error("Exception Occured While Parsing All In ParseAll Method, Message: {}", e.getMessage(), e);
        }
    }

    private void parseAllCounter(List<Row> fileContent, String dateTime, String category, String pmemsId,
            String processingTime, Map<String, String> counterValueMap1, JobContext jobcontext) {
        try {
            String date = substring(dateTime, 2, 8);
            String time = substring(dateTime, 8, 12);
            String dateKey = substring(dateTime, 0, 8);
            String hourkey = substring(dateTime, 0, 10);
            String quarterKey = substring(dateTime, 0, 12);
            String fiveminutekey = quarterKey;
            String frequency = jobcontext.getParameter("frequency");
            frequency = frequency != null ? frequency : jobcontext.getParameter("FREQUENCY");
            if (frequency != null && !frequency.isEmpty()) {
                frequency = frequency.toUpperCase();
                switch (frequency) {
                    case "15 MIN":
                        frequency = "QUARTERLY";
                        break;
                    case "QUARTERLY":
                        frequency = "QUARTERLY";
                        break;
                    case "PER HOUR":
                        frequency = "HOURLY";
                        break;
                    case "HOURLY":
                        frequency = "HOURLY";
                        break;
                    default:
                        frequency = "QUARTERLY";
                        break;
                }
            } else {
                frequency = "QUARTERLY";
            }

            logger.debug("Frequency: {}", frequency);

            String extractedPmemsid = pmemsId != null && pmemsId.contains("_") ? pmemsId.split("_")[0] : pmemsId;
            String finalKey = pmemsId + "##" + date + time;
            if (allCounterCategoryMap.containsKey(category)) {
                Map<String, Map<String, String>> counterValueMap = getValueMapAllCounters(category, counterValueMap1);
                for (Map.Entry<String, Map<String, String>> entry : counterValueMap.entrySet()) {
                    fileContent.add(RowFactory.create(finalKey, date, time, dateKey, hourkey, quarterKey,
                            processingTime, extractedPmemsid, entry.getKey(), entry.getValue(), pmemsId, fiveminutekey,
                            frequency));
                }
            }
        } catch (Exception e) {
            logger.error("Exception Occurred While Parsing Counters In ParseAllCounter Method, Message: {}",
                    e.getMessage(), e);
        }
    }

    private Map<String, Map<String, String>> getValueMapAllCounters(String category,
            Map<String, String> counterValueMap1) {
        Map<String, Map<String, String>> categoryVsCounterValueMap = new HashMap<>();

        List<Map<String, String>> derivedColumnList = allCounterCategoryMap.get(category);
        if (derivedColumnList != null) {
            for (Map<String, String> wrapper : derivedColumnList) {
                String counterheaderName = wrapper.get("CounterName");
                String sequenceNo = wrapper.get("sequenceno");
                String categoryAliasName = wrapper.get("Category");
                Map<String, String> tempMap = categoryVsCounterValueMap.getOrDefault(categoryAliasName,
                        new HashMap<>());
                String value = counterValueMap1.get(counterheaderName);
                if (value != null) {
                    String column = createCustomizedColumns(value, wrapper);
                    tempMap.put(sequenceNo, column);
                }
                categoryVsCounterValueMap.put(categoryAliasName, tempMap);
            }
        }
        return categoryVsCounterValueMap;
    }

    private String createCustomizedColumns(String value, Map<String, String> wrapper) {
        if (equalsIgnoreCase(wrapper.get("NodeAgg"), "COUNT")) {
            value = "1";
        }
        return value;
    }

    private void getAllCategoryCounterMap() {

        if (allCounterCategoryMap == null) {
            synchronized (SYNCHRONIZER) {
                if (allCounterCategoryMap == null) {
                    String json = jobcontext.getParameter(ALL_CATEGORY_COUNTER_MAPJSON);
                    if (json == null || json.isEmpty()) {
                        logger.info("ALL_CATEGORY_COUNTER_MAPJSON is EMPTY/NULL In Job Context, Skipping Process!");
                        return;
                    }
                    try {
                        allCounterCategoryMap = new ObjectMapper().readValue(json,
                                new TypeReference<Map<String, List<Map<String, String>>>>() {
                                });
                        logger.info("Successfully Parsed ALL_CATEGORY_COUNTER_MAP Size: {}",
                                allCounterCategoryMap != null ? allCounterCategoryMap.size() : 0);
                    } catch (Exception e) {
                        logger.error("Exception Occurred While Parsing JSON in ALL_CATEGORY_COUNTER_MAP , Error : {}",
                                e.getMessage(), e);
                    }
                }
            }
        }
    }

    @Override
    public String getName() {
        return "JuniperParseAllCounter";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("Date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("Time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("DateKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("HourKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("QuarterKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("PT", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("pmemsid", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("categoryName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("rawcounters",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
        fields.add(DataTypes.createStructField("interface_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("fiveminutekey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("frequency", DataTypes.StringType, true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

}