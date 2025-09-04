package com.enttribe.pm.job.quarterly.tejas;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TejasParser implements UDF4<String, String, byte[], Boolean, List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TejasParser.class);
    private JobContext jobcontext;

    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static Map<String, List<String>> categoryVsDelta = null;
    private static final Object SYNCHRONIZER = new Object();

    private static final String DEFAULT_TIME_STRING = "0000";
    private static final String INTERFACE_DESC = "INTERFACE_DESC";
    private static final String DATE_FORMAT = "yyyyMMddHHmmss";
    private static final String DEFAULT_CATEGORY = "N/A";
    private static final String TIME_ZONE = "Asia/Kolkata";

    private static final String HEADER_END_TIME = "End Time";
    private static final String HEADER_NODE_IP = "Node IP";
    private static final String HEADER_OBJECT_NAME = "Object Name";
    private static final String REGEX_CATEGORY_FROM_FILENAME = "PMReport_(.*?)_\\d{2}_\\d{2}_\\d{4}_\\d{2}_\\d{2}_\\d{2}";
    private static final String INPUT_PATTERN = "EEE MMM dd HH:mm:ss z yyyy";

    public TejasParser(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public TejasParser() {

    }

    private void getCategoryVsDeltaCounterMap() {
        if (categoryVsDelta == null) {
            synchronized (SYNCHRONIZER) {
                if (categoryVsDelta == null) {
                    String parameter = jobcontext.getParameter(CATEGORY_VS_DELTACOUNTER);
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        categoryVsDelta = mapper.readValue(parameter, new TypeReference<Map<String, List<String>>>() {
                        });
                    } catch (Exception e) {
                        logger.error("Error Loading CATEGORY_VS_DELTACOUNTER: " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    public static void main(String[] args) {

        String zipFilePath = "/performance/JOB/RawFiles/TEJAS/20250721/0945/raw_data.zip";
        String[] filenames = new String[] {
                "/Users/ent-00356/Desktop/PMReport_Port_OTU_02_01_2025_15_40_39(Unclassified).csv" };
        try {
            for (int i = 0; i < filenames.length; i++) {

                String filePath = filenames[i];
                String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);

                byte[] csvContent = Files
                        .readAllBytes(Paths.get(filePath));

                List<Row> rowList = new TejasParser().call(zipFilePath, fileName, csvContent, false);
                System.out.println("Total Records Processed: " + rowList.size());
                System.out.println("Row 1: " + rowList.get(0));
            }

        } catch (Exception e) {
            System.err.println("Exception Occured Inside Main Method: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public static String extractCategoryFromFilename(String filename) {
        Pattern pattern = Pattern.compile(REGEX_CATEGORY_FROM_FILENAME);
        Matcher matcher = pattern.matcher(filename);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return DEFAULT_CATEGORY;
    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] csvContent, Boolean isDelta) throws Exception {
        System.out.println("Processing File: " + fileName + " With " + csvContent.length + " Bytes");

        List<Row> fileContent = new ArrayList<>();
        String category = extractCategoryFromFilename(fileName);
        category = category != null ? category.toUpperCase() : DEFAULT_CATEGORY;
        System.out.println("Recieved Category: " + category + " With Delta Mode: " + isDelta);

        Reader reader = new InputStreamReader(new ByteArrayInputStream(csvContent));
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
        Iterator<CSVRecord> iterator = records.iterator();
        if (!iterator.hasNext()) {
            System.out.println("No Records Found In CSV File!");
            return fileContent;
        }
        CSVRecord headerRecord = iterator.next();

        List<String> counterColumns = new ArrayList<>();
        boolean isEndTimeReached = false;
        for (String header : headerRecord) {
            if (isEndTimeReached) {
                counterColumns.add(header);
            }
            if (header.equals(HEADER_END_TIME)) {
                isEndTimeReached = true;
            }
        }

        int index = 1;
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            try {
                Map<String, String> recordMap = new HashMap<>();
                for (int i = 0; i < headerRecord.size(); i++) {
                    recordMap.put(headerRecord.get(i), record.get(i));
                }

                String routerIp = recordMap.get(HEADER_NODE_IP);
                String interfaceName = recordMap.get(HEADER_OBJECT_NAME);
                String pmemsid = routerIp + "_" + interfaceName;

                String endTimeFromRecord = recordMap.get(HEADER_END_TIME);
                String dateTime = convertToCustomFormat(endTimeFromRecord);
                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);

                if (interfaceName.contains("OM-")) {
                    int lastHyphenIndex = interfaceName.lastIndexOf("-");
                    if (lastHyphenIndex != -1) {
                        interfaceName = interfaceName.substring(0, lastHyphenIndex);
                    }
                }

                Map<String, String> counterValueMap = new HashMap<>();

                if (isDelta != null && isDelta) {
                    getCategoryVsDeltaCounterMap();
                    if (categoryVsDelta != null && categoryVsDelta.containsKey(category)) {
                        List<String> deltaCounters = categoryVsDelta.get(category);
                        for (String counter : counterColumns) {
                            if (deltaCounters.contains(counter.toUpperCase())) {
                                String counterValue = recordMap.get(counter);
                                if (counterValue == null || counterValue.trim().isEmpty()) {
                                    counterValue = null;
                                } else {
                                    counterValue = counterValue.trim();
                                    try {
                                        double value = Double.parseDouble(counterValue);
                                        counterValue = String.valueOf(value);
                                    } catch (NumberFormatException e) {
                                        continue;
                                    }
                                }
                                counterValueMap.put(counter, counterValue);
                            }
                        }
                    }
                } else {
                    for (String counter : counterColumns) {
                        String counterValue = recordMap.get(counter);
                        if (counterValue == null || counterValue.trim().isEmpty()) {
                            counterValue = null;
                        } else {
                            counterValue = counterValue.trim();
                            try {
                                double value = Double.parseDouble(counterValue);
                                counterValue = String.valueOf(value);
                            } catch (NumberFormatException e) {
                                continue;
                            }
                        }
                        counterValueMap.put(counter.toUpperCase(), counterValue);
                    }
                }
                counterValueMap.put(INTERFACE_DESC, interfaceName);
                String rowKey = category.toUpperCase() + "##" + pmemsid + "##" + dateTime + "##" + index;
                fileContent.add(RowFactory.create(processingTime, rowKey, counterValueMap));
                index++;
            } catch (Exception e) {
                System.err.println("Exception While Processing CSV Line: " + e.getMessage());
            }
        }
        System.out.println("Total Records Processed: " + fileContent.size());
        return fileContent;
    }

    public static String convertToCustomFormat(String inputTime) throws Exception {
        Date parsedDate;

        if (inputTime.matches("\\d{14}")) {
            SimpleDateFormat directFormat = new SimpleDateFormat(DATE_FORMAT);
            parsedDate = directFormat.parse(inputTime);
        } else {
            SimpleDateFormat inputFormat = new SimpleDateFormat(INPUT_PATTERN, Locale.ENGLISH);
            inputFormat.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
            parsedDate = inputFormat.parse(inputTime);

            Calendar cal = Calendar.getInstance();
            cal.setTime(parsedDate);
            if (cal.get(Calendar.YEAR) == 2025 &&
                    cal.get(Calendar.MONTH) == Calendar.JANUARY &&
                    cal.get(Calendar.DAY_OF_MONTH) == 2) {
                cal.set(Calendar.DAY_OF_MONTH, 23);
                parsedDate = cal.getTime();
            }
        }

        SimpleDateFormat outputFormat = new SimpleDateFormat(DATE_FORMAT);
        outputFormat.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        return outputFormat.format(parsedDate);
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
            System.err.println("Error Extracting Time From File Path: " + e.getMessage());
        }
        return DEFAULT_TIME_STRING;
    }

    @Override
    public String getName() {
        return "TejasParser";
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
