package com.enttribe.pm.job.quarterly.adva;

import java.time.format.DateTimeParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import com.enttribe.commons.lang.DateUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

public class AdvaParser implements UDF3<String, String, byte[], List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AdvaParser.class);
    private JobContext jobcontext;

    public AdvaParser(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public AdvaParser() {

    }

    public static void main(String[] args) {

        String zipFilePath = "/Users/bootnext-mac-112/Downloads/pmRawFile/Archive.zip";

        String[] filenames = new String[] {
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpPhysicalLayerPerformance20250118_1645.csv",
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpPhysicalLayerInstant20250118_1645.csv",
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpOTUPerformance20250118_1645.csv",
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpOTUFECPerformance20250118_1645.csv",
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpNetworkElement20250118_1645.csv",
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpEthernetPort20250118_1645.csv",
                "/Users/bootnext-mac-112/Downloads/20250118_1800/pmDumpCard20250118_1645.csv" };
        try {
            for (int i = 0; i < filenames.length; i++) {

                String filePath = filenames[i];
                String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);

                byte[] csvContent = Files
                        .readAllBytes(Paths.get(filePath));

                List<Row> rowList = new AdvaParser().call(zipFilePath, fileName, csvContent);

                for (int rowIndex = 0; rowIndex < Math.min(1000, rowList.size()); rowIndex++) {

                    Row row = rowList.get(rowIndex);

                    for (int j = 0; j < row.length() && j < 20; j++) {

                        System.out.println("Row : " + rowIndex + " && Column : " + j + " -> " + row.get(j));

                    }

                }
                System.out.println("Size of rowList: " + rowList.size());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Handles null or empty values in CSV records.
     *
     * @param record     The CSV record.
     * @param index      The index of the field.
     * @param defaultVal The default value to return if the field is null or empty.
     * @return The trimmed value or the default value if null/empty.
     */
    private static String handleNull(CSVRecord record, int index, String defaultVal) {
        if (record == null || index >= record.size()) {
            return defaultVal; // Return default value if record is null or index is out of bounds
        }
        String value = record.get(index).trim();
        return value.isEmpty() ? defaultVal : value;
    }

    public static String convertISTtoUTC(String istTimeString) {
        // Define the formatter to parse and format the date-time
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        try {
            // Validate the input: return null for empty or non-numeric strings
            if (istTimeString == null || istTimeString.isEmpty() || !istTimeString.matches("\\d{14}")) {
                System.err.println("Invalid input: " + istTimeString);
                return null;
            }

            // Parse the input string into LocalDateTime
            LocalDateTime localDateTime = LocalDateTime.parse(istTimeString, formatter);

            // Associate the LocalDateTime with the IST timezone
            ZonedDateTime istZonedDateTime = localDateTime.atZone(ZoneId.of("Asia/Kolkata"));

            // Convert the IST ZonedDateTime to UTC
            ZonedDateTime utcZonedDateTime = istZonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));

            // Format the UTC ZonedDateTime back to the same string format
            return utcZonedDateTime.format(formatter);

        } catch (DateTimeParseException e) {
            // Handle parsing errors gracefully
            System.err.println("Invalid input: " + istTimeString + " - " + e.getMessage());
            return null;
        }
    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] csvContent) throws Exception {
        List<Row> fileContent = new ArrayList<>();
        Reader in = new InputStreamReader(new ByteArrayInputStream(csvContent));
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withDelimiter('|')
                .withFirstRecordAsHeader()
                .parse(in);

        int index = 1;
        for (CSVRecord record : records) {
            try {
                // if (record.size() < 8 || record.get(0).trim().equals("Connection")) {
                // continue;
                // }

                // String connection = handleNull(record, 0, "Unknown_Connection");
                // String granularity = handleNull(record, 1, "Unknown_Granularity");
                String key = handleNull(record, 2, null);
                // String unit = handleNull(record, 3, "Unknown_Unit");
                // String aggregation = handleNull(record, 4, "Unknown_Aggregation");
                String nename = handleNull(record, 5, null);
                String entity = handleNull(record, 6, null);
                String interfaceData = entity;
                String period = handleNull(record, 7, null);
                period = convertISTtoUTC(period);
                // System.out.println("period : " + period);
                // String value = record.size() > 8 ? handleNull(record, 8, null) : null;
                String value = handleNull(record, 8, null);

                // Extract key and value from colu2 and colu5
                // String key = record.get("Type"); // Key from colu2
                // String value = record.get("Value"); // Value from colu5
                if (value == null || value.isEmpty() || value == "") {
                    value = null;
                }
                long timestamp = (long) Double.parseDouble(period);
                if (entity.contains("OM-")) {
                    int lastHyphenIndex = entity.lastIndexOf("-");
                    if (lastHyphenIndex != -1) {
                        entity = entity.substring(0, lastHyphenIndex);
                    }
                }

                // Convert long to string representation
                String timePeriod = String.valueOf(timestamp);
                // String timePeriod = record.get("Period"); // Value from colu5
                // String timePeriod = getCurrentDateTime(); // Value from colu5
                // String entity = record.get("Entity"); // Value from colu5
                // key = key.replaceAll("[()]", "").trim();
                // String nename = record.get("NE");
                // key = key.replaceAll("\\s*\\([^)]*\\)", "").trim();
                key = key.replace("(", "").replace(")", "").trim();
                System.out.println("key : " + key + " Value : " + value);

                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);
                // Create a map for the key-value pair
                Map<String, String> counterValueMap = new HashMap<>();
                counterValueMap.put(key, value);
                counterValueMap.put("interface_desc", interfaceData);

                // Prepare data for Row object
                Map<String, Map<String, String>> categoryCounterMap = new HashMap<>();
                String category = "ODU OTU PERFORMANCE"; // Define a default category or extract from data
                String dateTime = getTimeForQuarterly(timePeriod, 0); // Example date/time

                String rowKey = category + "##" + nename + "_" + entity + "##" + dateTime + "##" + index;
                categoryCounterMap.put(rowKey, counterValueMap);
                if (nename.equals("LALITPUR_OADM_GNE")) {
                    System.out.println("categoryCounterMap : " + categoryCounterMap);
                }
                // Add the row to the list
                fileContent.add(RowFactory.create(processingTime, categoryCounterMap));
                index++;

            } catch (Exception e) {
                logger.error("Exception While Processing CSV Line: {}", record, e);
                // System.out.println("Exception While Processing CSV Line:" + record);
            }
        }

        // System.out.println("Total Rows Added: " + index);
        System.out.println("EfileContent " + fileContent);
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

    public static String getCurrentDateTime() {
        // Define the desired format (YYYYMMDDHHmm)
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        // Get the current date and time
        LocalDateTime now = LocalDateTime.now();

        // Format and return the current date and time
        return now.format(formatter);
    }

    private String getProcessedTime(String zipFilePath) {
        try {
            // return zipFilePath.substring(zipFilePath.lastIndexOf("/") + 1);
            return StringUtils.substringAfterLast(StringUtils.substringBeforeLast(zipFilePath, "/"), "/");
            // return "0000";
        } catch (Exception e) {
            return "0000";
        }
    }

    private String getTimeForQuarterly(String timeKey, Integer mins) throws ParseException {
        // Adjust input to ensure it matches the expected format
        if (timeKey.length() == 12) {
            timeKey += "00"; // Add seconds if missing
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = formatter.parse(timeKey);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(DateUtils.addMinutes(date, -mins));

        int minute = calendar.get(Calendar.MINUTE);
        int roundedMinute = (minute / 15) * 15;
        if (minute % 15 != 0) {
            roundedMinute = ((minute / 15) + 1) * 15;
        }

        if (roundedMinute == 60) {
            calendar.add(Calendar.HOUR_OF_DAY, 1);
            roundedMinute = 0;
        }

        calendar.set(Calendar.MINUTE, roundedMinute);
        calendar.set(Calendar.SECOND, 0);

        return formatter.format(calendar.getTime());
    }

    // public static String getTimeForQuarterly(String dateTime, int quarterOffset)
    // {
    // // Parse the input dateTime
    // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    // LocalDateTime date = LocalDateTime.parse(dateTime, formatter);

    // // Adjust by quarterOffset (each quarter is 3 months)
    // date = date.plusMonths(quarterOffset * 3);

    // // Round the minutes to the nearest 15-minute interval
    // int minutes = date.getMinute();
    // int roundedMinutes = ((minutes + 7) / 15) * 15; // Round to the nearest 15

    // if (roundedMinutes == 60) { // Handle hour overflow
    // date = date.plusHours(1);
    // roundedMinutes = 0;
    // }

    // // Set the rounded minutes back to the LocalDateTime object
    // date = date.withMinute(roundedMinutes).withSecond(0).withNano(0);

    // // Return the adjusted time as a string
    // return date.format(formatter);
    // }

    @Override
    public String getName() {
        return "AdvaParser";
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