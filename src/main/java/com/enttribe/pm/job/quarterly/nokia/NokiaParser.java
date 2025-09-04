package com.enttribe.pm.job.quarterly.nokia;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

public class NokiaParser implements UDF3<String, String, byte[], List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(NokiaParser.class);
    private JobContext jobcontext;

    public NokiaParser(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public NokiaParser() {

    }

    public static void main(String[] args) {

        String zipFilePath = "/bntvpm/RawFiles/PM/03-12-2024/0800/nokia-router-output.json";
        String fileName = "nokia-router-output.json";

        try {
            byte[] jsonContent = Files
                    .readAllBytes(Paths.get("/Users/ent-00356/Desktop/nokia-router-output.json"));

            List<Row> rowList = new NokiaParser().call(zipFilePath, fileName, jsonContent);

            System.out.println("Size of rowList: " + rowList.size());

            int count = 0;
            for (Row row : rowList) {
                if (count < 5) {
                    System.out.println("Row " + (count + 1) + " : " + row);
                    count++;
                } else {
                    break;
                }
            }

        } catch (Exception e) {
            logger.error("Exception Occured In Call Method : {}", e.getMessage());
        }

    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] jsonContent) throws Exception {

        List<Row> fileContent = new ArrayList<>();

        String jsonString = new String(jsonContent);

        String[] lines = jsonString.split("\n");

        System.out.println("Lines Length : " + lines.length);

        int index = 1;
        for (String line : lines) {

            try {

                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);

                JSONObject data = new JSONObject(line);

                String category = data.getString("name");


                if (category != null) {

                    if (category.equalsIgnoreCase("snmp")) {
                        category = "DEVICE";
                    }
                    if (category.equalsIgnoreCase("interface")) {
                        category = "INTERFACE";         //jeevan
                    }
                    category = category.toUpperCase();
                }

                JSONObject tagObject = data.getJSONObject("tags");
                JSONObject fieldObject = data.getJSONObject("fields");

                String measObjLdn = tagObject.getString("agent_host");

                Map<String, String> counterValueMap = new HashMap<>();

                if (measObjLdn != null) {

                    try {

                        counterValueMap.put("INTERFACE_DESC", tagObject.getString("ifDescr"));

                        String desc = tagObject.getString("ifDescr");
                        String[] descArray = desc.split(",");
                        String extractedPart = descArray[0];
                        measObjLdn = measObjLdn + "_" +  extractedPart;

                    } catch (Exception e) {

                        logger.error("Exception While Getting INTERFACE_DESC : ", e);
                    }
                }

                try {
                    for (String key : fieldObject.keySet()) {
                      
                        Object value = fieldObject.get(key);

                        counterValueMap.put(key.trim(), value.toString().trim());
                    }
                } catch (Exception e) {

                    logger.error("Exception While Storing CounterValueMap : {}", e);
                }

                String dateTimeActual = getDateTime(data.getLong("timestamp"));

                String dateTime = getTimeForQuarterly(dateTimeActual, 00);

                String key = category + "##" + measObjLdn + "##" + dateTime + "##" + index;

                Map<String, Map<String, String>> categoryCounterMap = new HashMap<>();
                categoryCounterMap.put(key, counterValueMap);

                fileContent.add(RowFactory.create(processingTime, categoryCounterMap));
                index++;

            } catch (Exception e) {

                logger.error("Exception While Processing JSON Line: {}", line, e);

            }

        }
        logger.info("inside the file content; {}",fileContent);
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

    private String getDateTime(Long dateTime) {
        try {

            logger.info("Received dateTime (epoch seconds): {}", dateTime);

            Instant instant = Instant.ofEpochSecond(dateTime);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
                    .withZone(ZoneId.of("UTC")); // Use desired timezone

            String formattedDate = formatter.format(instant);

            logger.info("Formatted date: {}", formattedDate);

            return formattedDate;

        } catch (Exception e) {

            logger.error("Error while converting dateTime: {}", dateTime, e);
            return null;
        }
    }

    private String getTimeForQuarterly(String timeKey, Integer mins) throws ParseException {
        try {

            logger.info("Received timeKey: {}, mins: {}", timeKey, mins);

            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
            Date parsedDate = formatter.parse(timeKey);

            logger.info("Parsed timeKey into Date: {}", parsedDate);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(DateUtils.addMinutes(parsedDate, -mins));

            int minute = calendar.get(Calendar.MINUTE);
            int roundedMinute = (minute / 15) * 15;
            if (minute % 15 != 0) {
                roundedMinute = ((minute / 15) + 1) * 15;
            }

            logger.info("Original minute: {}, Rounded minute: {}", minute, roundedMinute);

            calendar.set(Calendar.MINUTE, roundedMinute);
            calendar.set(Calendar.SECOND, 0);

            String roundedTime = DateUtils.format("yyyyMMddHHmmss", calendar.getTime());

            logger.info("Final rounded time: {}", roundedTime);

            return roundedTime;

        } catch (ParseException e) {

            logger.error("Error while processing timeKey: {} and mins: {}", timeKey, mins, e);
            throw e;
        }
    }

    private String getProcessedTime(String zipFilePath) {

        try {
            String processedTime = StringUtils.substringAfterLast(StringUtils.substringBeforeLast(zipFilePath, "/"),
                    "/");

            logger.info("Extracted Processed Time : {}", processedTime);

            return processedTime;

        } catch (Exception e) {

            logger.info("Returning Default Processing Time : 0000");

            return "0000";
        }

    }

    @Override
    public String getName() {
        return "NokiaParser";
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