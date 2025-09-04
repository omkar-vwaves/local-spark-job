package com.enttribe.pm.job.quarterly.juniper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Date;
import java.util.HashSet;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.lang.DateUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

public class JuniperParser implements UDF3<String, String, byte[], List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(JuniperParser.class);
    public JobContext jobcontext;

    Set<String> uniqueTimestamps = new HashSet<>();

    public JuniperParser(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public JuniperParser() {
    }

    public static void main(String[] args) {

        String zipFilePath = "/path/to/your/03-12-24/0945/file.zip";
        String fileName = "juniper-switch.json";

        try {
            byte[] jsonContent = Files
                    .readAllBytes(Paths.get("/Users/bootnext-mac-111/Downloads/snmp-output.json"));

            List<Row> rowList = new JuniperParser().call(zipFilePath, fileName,
                    jsonContent);
            for (int rowIndex = 0; rowIndex < Math.min(5, rowList.size()); rowIndex++) {
                Row row = rowList.get(rowIndex);
                for (int i = 0; i < row.length() && i < 20; i++) {
                    System.out.println("Row : " + rowIndex + " && Column : " + i + " -> " +
                            row.get(i));
                }
            }
            System.out.println("Size of rowList: " + rowList.size());

        } catch (Exception e) {
            logger.error("Exception Occured In Call Method : {}", e.getMessage());
        }

    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] jsonContent) throws Exception {

        logger.info("Processing Zip File Path: {}, File Name: {}, JSON Content Length : {}", zipFilePath, fileName,
                jsonContent.length);

        List<Row> fileContent = new ArrayList<>();
        String jsonString = new String(jsonContent);
        String[] lines = jsonString.split("\n");

        int index = 1;
        for (String line : lines) {

            try {
                JSONObject mainJSONObject = new JSONObject(line);

                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);

                // EXTRACT CATEGORY
                String category = mainJSONObject.getString("name");
                if (category != null) {
                    if (category.equalsIgnoreCase("snmp")) {
                        category = "DEVICE";
                    }
                    category = category.toUpperCase();
                }

                // EXTRACT PMEMSID
                JSONObject tagObject = mainJSONObject.getJSONObject("tags");
                String pmemsid = tagObject.getString("agent_host");

                Map<String, String> counterValueMap = new HashMap<>();
                if (pmemsid != null) {
                    try {
                        counterValueMap.put("INTERFACE_DESC", tagObject.getString("ifDescr"));
                        pmemsid = pmemsid + "_" + tagObject.getString("ifDescr"); // Added
                        // 172.31.31.148_ge-0/1/7
                    } catch (Exception e) {
                        logger.error("Exception While getting interface_desc:");
                    }
                }

                String dateTimeFromFile = getDateTime(mainJSONObject.getLong("timestamp")); // 1738368011 ->
                                                                                            // 202512312300
                uniqueTimestamps.add(String.valueOf(mainJSONObject.getLong("timestamp")));
                String dateTime = getTimeForQuarterlyJOB(dateTimeFromFile, 00); // 202512312300 -> 20251231230000

                JSONObject fieldsJSONObject = mainJSONObject.getJSONObject("fields");
                Iterator<String> keys = fieldsJSONObject.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    Object value = fieldsJSONObject.get(key);
                    try {
                        counterValueMap.put(key.toUpperCase(), String.valueOf(value));
                    } catch (Exception e) {
                    }
                }

                String key = category + "##" + pmemsid + "##" + dateTime + "##" + index;

                Map<String, Map<String, String>> categoryCounterMap = new HashMap<>();
                categoryCounterMap.put(key, counterValueMap);

                fileContent.add(RowFactory.create(processingTime, categoryCounterMap));
                index++;

            } catch (Exception e) {
                logger.error("JuniperParser: Exception While Processing JSON Line: {}, Error Message: {}", line,
                        e.getMessage());
            }
        }

        System.out.println("Result Size : " + index);
        System.out.println("Unique Timestamps : " + uniqueTimestamps.toString());
        return fileContent;
    }

    // private String getDateTime(Long dateTime) {
    // Instant instant = Instant.ofEpochSecond(dateTime);
    // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
    // .withZone(ZoneId.of("UTC")); // Use Desired TimeZone
    // String formattedDate = formatter.format(instant);
    // return formattedDate;
    // }

    // private static String getDateTime(Long dateTime) {

    // Instant instant = Instant.ofEpochSecond(dateTime);
    // Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    // calendar.setTimeInMillis(instant.toEpochMilli());

    // int minute = calendar.get(Calendar.MINUTE);
    // int second = calendar.get(Calendar.SECOND);
    // int totalSeconds = minute * 60 + second;
    // int roundedSlotSeconds = ((totalSeconds + 899) / 900) * 900;

    // calendar.set(Calendar.MINUTE, 0);
    // calendar.set(Calendar.SECOND, 0);
    // calendar.set(Calendar.MILLISECOND, 0);
    // calendar.add(Calendar.SECOND, roundedSlotSeconds);

    // SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
    // formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    // return formatter.format(calendar.getTime());
    // }

    private String getDateTime(Long dateTime) {
        if (dateTime == null) {
            throw new IllegalArgumentException("dateTime cannot be null");
        }

        Instant instant = Instant.ofEpochSecond(dateTime); // for seconds
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
                .withZone(ZoneId.of("UTC"));
        return formatter.format(instant);
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
        fields.add(DataTypes.createStructField("processingTime", DataTypes.StringType, true));
        fields.add(
                DataTypes.createStructField("parsedCounterMap",
                        DataTypes.createMapType(DataTypes.StringType,
                                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true),
                        true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }
}