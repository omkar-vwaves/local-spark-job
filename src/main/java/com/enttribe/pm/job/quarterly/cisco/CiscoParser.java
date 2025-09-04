package com.enttribe.pm.job.quarterly.cisco;

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
import java.util.HashSet;
import java.util.Iterator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.*;

import org.apache.commons.lang.StringUtils;

import com.enttribe.commons.lang.DateUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

public class CiscoParser implements UDF3<String, String, byte[], List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CiscoParser.class);
    private JobContext jobcontext;

    private static List<String> pmemsidList = new ArrayList<>();

    public CiscoParser(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public CiscoParser() {

    }

    public static void main(String[] args) {

        String zipFilePath = "/path/to/your/03-12-24/1715/file.zip";
        String fileName = "02506141715.json";

        try {
            byte[] jsonContent = Files
                    .readAllBytes(Paths.get("/Users/ent-00356/Desktop/202506141715.json"));

            List<Row> rowList = new CiscoParser().call(zipFilePath, fileName, jsonContent);

            for (int rowIndex = 0; rowIndex < Math.min(5, rowList.size()); rowIndex++) {

                Row row = rowList.get(rowIndex);

                for (int i = 0; i < row.length() && i < 20; i++) {

                    System.out.println("Row : " + rowIndex + " && Column : " + i + " -> " +
                            row.get(i));

                }

            }

            System.out.println("Size of rowList: " + rowList.size());
            pmemsidList = new ArrayList<>(new HashSet<>(pmemsidList));
            // System.out.println("PM_EMS_ID List: " + pmemsidList);
            System.out.println("PM_EMS_ID Size: " + pmemsidList.size());

        } catch (Exception e) {
            logger.error("Exception Occured In Call Method : {}", e.getMessage());
        }

    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] jsonContent) throws Exception {

        System.out.println("JSON CONTENT LENGTH : " + jsonContent.length);

        List<Row> fileContent = new ArrayList<>();

        String jsonString = new String(jsonContent);

        String[] lines = jsonString.split("\n");

        int index = 1;

        for (String line : lines) {

            try {
                JSONObject data = new JSONObject(line);
                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);
                String category = data.getString("name");
                if (category != null) {
                    if (category.equalsIgnoreCase("snmp"))
                        category = "DEVICE";
                    category = category.toUpperCase();
                }
                JSONObject tagObject = data.getJSONObject("tags");
                // String measObjLdn = tagObject.getString("agent_host");
                String measObjLdn = tagObject.getString("router_ip");
                Map<String, String> counterValueMap = new HashMap<>();
                if (measObjLdn != null) {
                    try {
                        counterValueMap.put("interface_desc", tagObject.getString("ifDescr"));
                        measObjLdn = measObjLdn + "_" + tagObject.getString("ifDescr"); // Added

                        String str = "'" + measObjLdn + "'";
                        pmemsidList.add(str);
                    } catch (Exception e) {
                    }
                }
                Map<String, Map<String, String>> categoryCounterMap = new HashMap<>();
                JSONObject fields = data.getJSONObject("fields");
                String dateTimeActual = getDateTime(data.getLong("timestamp"));
                String dateTime = getTimeForQuarterly(dateTimeActual, 00);
                System.out.println("dateTime: " + dateTime);

                Iterator<String> keys = fields.keys();
                while (keys.hasNext()) {
                    String key = keys.next();
                    Object value = fields.get(key);
                    try {

                        counterValueMap.put(key, String.valueOf(value));
                    } catch (Exception e) {
                        logger.error("Exception While converting : {}", line, e);
                    }
                }
                String key = category + "##" + measObjLdn + "##" + dateTime + "##" + index;
                categoryCounterMap.put(key, counterValueMap);
                fileContent.add(RowFactory.create(processingTime, categoryCounterMap));
                index++;

            } catch (Exception e) {

                logger.error("Exception While Processing JSON Line: {}", line, e);
            }
        }

        System.out.println("Total Rows Added: " + index);
        return fileContent;
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

    // private String getDateTime(Long dateTime) {
    // Instant instant = Instant.ofEpochSecond(dateTime);
    // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
    // .withZone(ZoneId.of("UTC")); // Use desired timezone
    // String formattedDate = formatter.format(instant);
    // return formattedDate;
    // }
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

    private String getTimeForQuarterly(String timeKey, Integer mins) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(DateUtils.addMinutes(formatter.parse(timeKey), -mins));
        int minute = calendar.get(Calendar.MINUTE);
        int roundedMinute = (minute / 15) * 15;
        if (minute % 15 != 0) {
            roundedMinute = ((minute / 15) + 1) * 15;
        }
        calendar.set(Calendar.MINUTE, roundedMinute);
        calendar.set(Calendar.SECOND, 0);
        return DateUtils.format("yyyyMMddHHmmss", calendar.getTime());
    }

    // private String getTimeForQuarterly(String timeKey, Integer mins) throws
    // ParseException {
    // SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
    // Calendar calendar = Calendar.getInstance();
    // calendar.setTime(DateUtils.addMinutes(formatter.parse(timeKey), -mins));
    // calendar.set(Calendar.MINUTE, 0);
    // calendar.set(Calendar.SECOND, 0);
    // return DateUtils.format("yyyyMMddHHmmss", calendar.getTime());
    // }

    // private String getProcessedTime(String zipFilePath) {
    // // Pattern pattern = Pattern.compile("/([^/]+)/$");
    // // Matcher matcher = pattern.matcher(zipFilePath);
    // // if (matcher.find()) {

    // try {
    // return
    // StringUtils.substringAfterLast(StringUtils.substringBeforeLast(zipFilePath,
    // "/"), "/");
    // } catch (Exception e) {
    // return "0000";
    // }
    // // String folderName = matcher.group(1);
    // // System.out.println("Folder name found:" + folderName);
    // // return folderName;
    // // } else {
    // // System.out.println("Folder name not found!");
    // // }

    // }

    @Override
    public String getName() {
        return "CiscoParser";
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