package com.enttribe.pm.job.quarterly.cisco;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CiscoParserForDelta implements UDF4<String, String, byte[], Boolean, List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CiscoParserForDelta.class);
    private JobContext jobcontext;
    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static Map<String, List<String>> categoryVSDetla = null;
    private static Object SYNCHRONIZER = new Object();

    private static final String TIME_ZONE = "UTC";
    private static final String DATE_FORMAT = "yyyyMMddHHmmss";
    private static final int QUARTER_DURATION_MINUTES = 15;
    private static final String DEFAULT_CATEGORY = "N/A";
    private static final String DEFAULT_TIME_STRING = "0000";

    public CiscoParserForDelta(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public CiscoParserForDelta() {
    }

    private void getCategoryVsDeltaCounterMap() {

        if (categoryVSDetla == null) {
            synchronized (SYNCHRONIZER) {
                if (categoryVSDetla == null) {

                    logger.info("Starting Initialization of Category Vs Delta Counter Map");

                    String parameter = jobcontext.getParameter(CATEGORY_VS_DELTACOUNTER);
                    ObjectMapper mapper = new ObjectMapper();

                    try {

                        categoryVSDetla = mapper.readValue(parameter, new TypeReference<Map<String, List<String>>>() {
                        });

                        logger.info("Category Vs Delta Counter Map Initialized Successfully With Size: {}",
                                categoryVSDetla.size());

                    } catch (Exception e) {
                        logger.error("Error During Initialization of Category Vs Delta Counter Map: {}", e.getMessage(),
                                e);
                    }

                }
            }
        }
    }

    public static void main(String[] args) {

        String zipFilePath = "/path/to/your/03-12-24/0945/file.zip";
        String fileName = "juniper-switch.json";

        try {
            byte[] jsonContent = Files
                    .readAllBytes(Paths.get("/Users/bootnext-mac-111/Downloads/snmp-output.json"));

            List<Row> rowList = new CiscoParserForDelta().call(zipFilePath, fileName, jsonContent, false);
            System.out.println("Size of RowList: " + rowList.size());

        } catch (Exception e) {
            logger.error("Exception Occured In Call Method : {}", e.getMessage());
        }

    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] jsonContent, Boolean isDelta) throws Exception {

        logger.info("Zip File Path: {}, File Name: {}, JSON Content Length : {}", zipFilePath, fileName,
                jsonContent.length);

        List<Row> fileContent = new ArrayList<>();
        String jsonString = new String(jsonContent);
        String[] lines = jsonString.split("\n");

        int index = 1;
        if (isDelta) {
            getCategoryVsDeltaCounterMap();
        }
        for (String line : lines) {

            try {
                JSONObject mainJSONObject = new JSONObject(line);
                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);
                String category = mainJSONObject.optString("name", DEFAULT_CATEGORY).toUpperCase();
                if (category != null) {
                    if (category.equalsIgnoreCase("snmp")) {
                        category = "DEVICE";
                    }
                    category = category.toUpperCase();
                }

                JSONObject tagObject = mainJSONObject.getJSONObject("tags");
                String pmemsid = tagObject.getString("router_ip");

                Map<String, String> counterValueMap = new HashMap<>();
                if (pmemsid != null) {
                    try {

                        String interfaceDesc = tagObject.getString("ifDescr");
                        counterValueMap.put("INTERFACE_DESC", interfaceDesc);

                        String extractedInterfaceDesc = interfaceDesc.split(",")[0];
                        if (extractedInterfaceDesc != null) {
                            pmemsid = pmemsid + "_" + extractedInterfaceDesc;
                        }
                    } catch (Exception e) {
                        logger.error("Exception While Getting Interface Description: {}", e.getMessage());
                    }
                }

                Long timestamp = mainJSONObject.getLong("timestamp");
                String dateTime = getQuarterlyDateTime(timestamp);
                System.out.println("Timestamp: " + String.valueOf(timestamp) + " Date Time: " + dateTime);
                JSONObject fieldsJSONObject = mainJSONObject.getJSONObject("fields");
                if (isDelta) {
                    if (categoryVSDetla != null && categoryVSDetla.containsKey(category)) {
                        List<String> counterList = categoryVSDetla.get(category);
                        for (String key : fieldsJSONObject.keySet()) {
                            Object value = fieldsJSONObject.get(key);
                            if (counterList.contains(key.toUpperCase()) && value != null
                                    && !value.toString().equals("null") && !value.toString().equals("")) {
                                counterValueMap.put(key.trim().toUpperCase(), String.valueOf(value));
                            }
                        }
                    }
                } else {
                    for (String key : fieldsJSONObject.keySet()) {
                        Object value = fieldsJSONObject.get(key);
                        counterValueMap.put(key.trim().toUpperCase(), String.valueOf(value));
                    }
                }

                String key = category + "##" + pmemsid + "##" + dateTime + "##" + index;
                fileContent.add(RowFactory.create(processingTime, key, counterValueMap));
                index++;

            } catch (Exception e) {
                logger.error("Exception While Processing JSON Line: {}, Error Message: {}", line, e.getMessage());
            }
        }

        System.out.println("Result Size : " + index);
        return fileContent;
    }

    private static String getQuarterlyDateTime(Long epochSeconds) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(TIME_ZONE));
        calendar.setTimeInMillis(epochSeconds * 1000L);

        int minute = calendar.get(Calendar.MINUTE);
        int flooredMinute = (minute / QUARTER_DURATION_MINUTES) * QUARTER_DURATION_MINUTES;

        calendar.set(Calendar.MINUTE, flooredMinute);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        return formatter.format(calendar.getTime());
    }

    private String getProcessedTimeFromZipFilePath(String zipFilePath) {
        try {
            String[] parts = zipFilePath.split("/");
            if (parts.length > 2) {
                String processedTime = parts[parts.length - 2];
                if (processedTime.matches("\\d{4}")) {
                    return processedTime;
                }
            }
        } catch (Exception e) {
            logger.error("Error Extracting Processed Time From Zip File Path: {}", zipFilePath);
        }
        return DEFAULT_TIME_STRING;
    }

    @Override
    public String getName() {
        return "CiscoParser";
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