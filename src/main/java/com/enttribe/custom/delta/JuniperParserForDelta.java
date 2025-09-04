package com.enttribe.custom.delta;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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

import com.enttribe.commons.lang.DateUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JuniperParserForDelta implements UDF4<String, String, byte[], Boolean, List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(JuniperParserForDelta.class);
    private JobContext jobcontext;
    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static Map<String, List<String>> categoryVSDetla = null;
    private static Object SYNCHRONIZER = new Object();

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

                        System.out.println("CategoryVSDetla Size: " + categoryVSDetla.size());

                    } catch (Exception e) {
                        System.out.println("Exception While Getting Category Vs Delta Counter Map: " + e.getMessage());
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
                    .readAllBytes(Paths.get(
                            "/Users/ent-00356/Desktop/local-spark-job/src/main/java/com/enttribe/custom/delta/test-juniper-delta.json"));

            List<Row> rowList = new JuniperParserForDelta().call(zipFilePath, fileName, jsonContent, false);
            System.out.println("Size of rowList: " + rowList.size());

        } catch (Exception e) {
            logger.error("Exception Occured In Call Method : {}", e.getMessage());
        }

    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] jsonContent, Boolean isDelta) throws Exception {

        System.out.println("Zip File Path: " + zipFilePath);
        System.out.println("File Name: " + fileName);
        System.out.println("JSON Content Length: " + jsonContent.length);

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

                String category = mainJSONObject.getString("name");
                if (category != null) {
                    category = category.toUpperCase();
                }

                JSONObject tagObject = mainJSONObject.getJSONObject("tags");
                String pmemsid = tagObject.getString("router_ip");

                Map<String, String> counterValueMap = new HashMap<>();
                if (pmemsid != null) {
                    try {
                        String interfaceDesc = tagObject.optString("ifDescr", "").trim();
                        String extractedInterface = interfaceDesc.split(",")[0].trim();
                        pmemsid = pmemsid + "_" + extractedInterface;
                        counterValueMap.put("INTERFACE_DESC", extractedInterface);
                    } catch (Exception e) {

                    }
                }

                String dateTimeFromFile = getDateTime(mainJSONObject.getLong("timestamp"));

                String dateTime = getTimeForQuarterlyJOB(dateTimeFromFile, 00);

                JSONObject fieldsJSONObject = mainJSONObject.getJSONObject("fields");

                if (isDelta) {

                    if (categoryVSDetla != null && categoryVSDetla.containsKey(category)) {
                        List<String> counterList = categoryVSDetla.get(category);
                        for (String key : fieldsJSONObject.keySet()) {
                            Object value = fieldsJSONObject.get(key);
                            if (counterList.contains(key.toUpperCase()) && value != null
                                    && !value.toString().equals("null")) {
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
                logger.error("JuniperParser: Exception While Processing JSON Line: {}, Error Message: {}", line,
                        e.getMessage());
            }
        }

        System.out.println("JuniperParser: Result Size : " + index);
        return fileContent;
    }

    private static String getDateTime(Long dateTime) {

        Instant instant = Instant.ofEpochSecond(dateTime);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(instant.toEpochMilli());

        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        int totalSeconds = minute * 60 + second;
        int roundedSlotSeconds = ((totalSeconds + 899) / 900) * 900;

        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, roundedSlotSeconds);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return formatter.format(calendar.getTime());
    }

    private String getTimeForQuarterlyJOB(String timeKey, Integer mins) throws ParseException {

        String formattedTime = "";

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        Calendar calendar = Calendar.getInstance();

        try {
            Date parsedDate = formatter.parse(timeKey);
            calendar.setTime(DateUtils.addMinutes(parsedDate, -mins));

            int minute = calendar.get(Calendar.MINUTE);
            int roundedMinute = (minute / 15) * 15;

            if (minute % 15 != 0) {
                roundedMinute = ((minute / 15) + 1) * 15;
            }

            calendar.set(Calendar.MINUTE, roundedMinute);
            calendar.set(Calendar.SECOND, 0);

            formattedTime = DateUtils.format("yyyyMMddHHmmss", calendar.getTime());

        } catch (ParseException e) {
            logger.error("Exception Occured While Parsing timeKey: {}, Error: {}", timeKey, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected Error Occured While Parsing timeKey: {}, Error: {}", timeKey, e.getMessage(), e);
        }
        return formattedTime;
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
        return "0000";
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