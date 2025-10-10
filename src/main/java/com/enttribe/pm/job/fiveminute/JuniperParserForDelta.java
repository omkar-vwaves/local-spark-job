package com.enttribe.pm.job.fiveminute;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.json.JSONObject;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JuniperParserForDelta implements UDF3<String, String, Boolean, List<Row>>, AbstractUDF {

    private static Logger logger = LoggerFactory.getLogger(JuniperParserForDelta.class);

    private static final long serialVersionUID = 1L;
    public JobContext jobcontext;
    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static Map<String, List<String>> categoryVSDetla = null;
    private static Object SYNCHRONIZER = new Object();

    private static final String TIME_ZONE = "UTC";
    private static final String DATE_FORMAT = "yyyyMMddHHmmss";
    private static final int FIVE_MINUTE_DURATION_MINUTES = 5;
    private static final String DEFAULT_CATEGORY = "N/A";
    private static final String DEFAULT_TIME_STRING = "0000";

    public JuniperParserForDelta(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public JuniperParserForDelta() {
    }

    private void getCategoryVsDeltaCounterMap() {

        if (categoryVSDetla == null) {
            synchronized (SYNCHRONIZER) {
                if (categoryVSDetla == null) {

                    String parameter = jobcontext.getParameter(CATEGORY_VS_DELTACOUNTER);
                    ObjectMapper mapper = new ObjectMapper();

                    try {

                        categoryVSDetla = mapper.readValue(parameter, new TypeReference<Map<String, List<String>>>() {
                        });

                        logger.info("Category Vs Delta Counter Map: {}", categoryVSDetla);

                    } catch (Exception e) {
                        System.err.println("Error Initializing Category Vs Delta Counter Map: " + e.getMessage());
                        e.printStackTrace();
                    }

                    logger.info("Category Vs Delta Counter Map Size - {}", categoryVSDetla.size());
                }
            }
        }
    }

    @Override
    public List<Row> call(String rawData, String fileName, Boolean isDelta) throws Exception {

        List<Row> fileContent = new ArrayList<>();

        int index = 1;
        if (isDelta) {
            getCategoryVsDeltaCounterMap();
        }

        try {
            final JSONObject mainJSONObject = new JSONObject(rawData);
            final String processingTime = getProcessedTimeFromZipFilePath(fileName);

            String category = mainJSONObject.optString("name", DEFAULT_CATEGORY).toUpperCase();
            category = category.toUpperCase().trim();
            final JSONObject tagObject = mainJSONObject.getJSONObject("tags");
            String pmemsid = tagObject.getString("agent_host");
            final Map<String, String> counterValueMap = new HashMap<>();

            for (String tagKey : tagObject.keySet()) {
                String tagValue = tagObject.optString(tagKey, null);
                if (tagValue != null && !"null".equals(tagValue)) {
                    String upperTagKey = tagKey.toUpperCase();
                    counterValueMap.put(upperTagKey, tagValue);
                }
            }

            if (pmemsid != null) {
                try {
                    if (tagObject != null && tagObject.has("ifName")) {
                        final String interfaceDesc = tagObject.optString("ifName", "").trim();

                        if (!interfaceDesc.isEmpty()) {
                            counterValueMap.put("IFNAME", interfaceDesc); // Managed Object (MO)
                            final String[] parts = interfaceDesc.split(",");
                            if (parts.length > 0 && parts[0] != null && !parts[0].trim().isEmpty()) {
                                final String extractedInterfaceDesc = parts[0].trim();
                                pmemsid = pmemsid + "_" + extractedInterfaceDesc;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error Getting Interface Description: " + e.getMessage());
                }
            }

            pmemsid = pmemsid != null ? pmemsid.trim() : null;

            final Long timestampFromFile = mainJSONObject.getLong("timestamp");

            String dateTime = getFiveMinuteDateTime(timestampFromFile);
            dateTime = dateTime != null ? dateTime.trim() : null;

            final JSONObject fieldsJSONObject = mainJSONObject.getJSONObject("fields");
            if (isDelta && categoryVSDetla != null && categoryVSDetla.containsKey(category)) {
                final List<String> counterList = categoryVSDetla.get(category);
                for (final String key : fieldsJSONObject.keySet()) {
                    final Object value = fieldsJSONObject.get(key);
                    if (counterList.contains(key.toUpperCase()) && value != null
                            && !"null".equals(value.toString()) && !"".equals(value.toString())) {
                        counterValueMap.put(key.trim().toUpperCase(), String.valueOf(value));
                    }
                }
            } else if (!isDelta) {
                for (final String key : fieldsJSONObject.keySet()) {
                    final Object value = fieldsJSONObject.get(key);
                    counterValueMap.put(key.trim().toUpperCase(), String.valueOf(value));
                }
            }
            final String key = category + "##" + pmemsid + "##" + dateTime + "##" + index;
            fileContent.add(RowFactory.create(processingTime, key, counterValueMap));
            index++;
        } catch (Exception e) {
            System.err.println("Error Processing JSON Line: " + e.getMessage());
        }

        return fileContent;
    }

    private static String getFiveMinuteDateTime(Long epochSeconds) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(TIME_ZONE));
        calendar.setTimeInMillis(epochSeconds * 1000L);

        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);

        if (second >= 30) {
            minute += 1;
        }
        int roundedMinute = (minute / FIVE_MINUTE_DURATION_MINUTES) * FIVE_MINUTE_DURATION_MINUTES;
        calendar.set(Calendar.MINUTE, roundedMinute);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        return formatter.format(calendar.getTime());
    }

    private String getProcessedTimeFromZipFilePath(String zipFilePath) {
        try {
            if (zipFilePath != null && !zipFilePath.isEmpty()) {
                java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("time=(\\d{4})")
                        .matcher(zipFilePath);
                if (matcher.find()) {
                    return matcher.group(1);
                }
            }
        } catch (Exception e) {
            System.err.println("Error Extracting Time From File Path: " + e.getMessage());
        }
        return DEFAULT_TIME_STRING;
    }

    @Override
    public String getName() {
        return "JuniperParser";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("PT", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("mapKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("counterValueMap",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }
}