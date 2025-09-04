package com.enttribe.pm.job.quarterly.adva;

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
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.enttribe.commons.lang.DateUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AdvaParserForDelta implements UDF4<String, String, byte[], Boolean, List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AdvaParserForDelta.class);
    private JobContext jobcontext;

    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static Map<String, List<String>> categoryVsDelta = null;
    private static final Object SYNCHRONIZER = new Object();
    private static final String FIXED_CATEGORY = "ODU OTU PERFORMANCE";
    private static final String QUARTER_FORMAT = "yyyyMMddHHmmss";
    private static final String DEFAULT_PROCESSING_TIME = "0000";

    public AdvaParserForDelta(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public AdvaParserForDelta() {
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
                        System.err.println("Exception While Loading Category Vs Delta Counter: " + e.getMessage());
                    }
                }
            }
        }
    }

    public static void main(String[] args) {

        String zipFilePath = "/performance/JOB/RawFiles/ADVA/20250721/0945/raw_data.zip";

        String[] filenames = new String[] {
                "/Users/ent-00356/Downloads/custom-adva.csv" };
        try {
            for (int i = 0; i < filenames.length; i++) {

                String filePath = filenames[i];
                String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);

                byte[] csvContent = Files
                        .readAllBytes(Paths.get(filePath));

                List<Row> rowList = new AdvaParserForDelta().call(zipFilePath, fileName, csvContent, false);
                System.out.println("Total Records Processed: " + rowList.size());
                System.out.println("Row 2: " + rowList.get(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static String handleNull(CSVRecord record, int index, String defaultVal) {
        if (record == null || index >= record.size()) {
            return defaultVal;
        }
        String value = record.get(index).trim();
        return value.isEmpty() ? defaultVal : value;
    }

    @Override
    public List<Row> call(String zipFilePath, String fileName, byte[] csvContent, Boolean isDelta) throws Exception {
        System.out.println("Processing File: " + fileName + " With " + csvContent.length + " Bytes");
        System.out.println("Recieved Delta Mode: " + isDelta);
        List<Row> fileContent = new ArrayList<>();
        Reader in = new InputStreamReader(new ByteArrayInputStream(csvContent));
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withDelimiter('|')
                .withFirstRecordAsHeader()
                .parse(in);

        int index = 1;
        for (CSVRecord record : records) {
            try {
                System.out.println("Processing Record: " + index + " With Record: " + record);
                String counterName = handleNull(record, 2, null);
                String entityName = handleNull(record, 5, null) ;
                String interfaceName = handleNull(record, 6, null);
                String period = handleNull(record, 7, null);
                String value = handleNull(record, 8, null);

                System.out.println("Counter Name: " + counterName + " Entity Name: " + entityName + " Interface Name: "
                        + interfaceName + " Period: " + period + " Value: " + value);

                if (value == null || value.isEmpty() || value == "") {
                    value = null;
                }

                if (interfaceName.contains("OM-")) {
                    int lastHyphenIndex = interfaceName.lastIndexOf("-");
                    if (lastHyphenIndex != -1) {
                        interfaceName = interfaceName.substring(0, lastHyphenIndex);
                    }
                }

                counterName = counterName != null ? counterName.replace("(", "").replace(")", "").trim().toUpperCase()
                        : null;
                entityName = entityName != null ? entityName.replace("(", "").replace(")", "").trim().toUpperCase()
                        : null;
                interfaceName = interfaceName != null ? interfaceName.replace("(", "").replace(")", "").trim().toUpperCase()
                        : null;

                String processingTime = getProcessedTimeFromZipFilePath(zipFilePath);
                Map<String, String> counterValueMap = new HashMap<>();
                String dateTime = getTimeForQuarterly(period, 0);

                if (isDelta != null && isDelta) {
                    getCategoryVsDeltaCounterMap();
                    if (categoryVsDelta != null && categoryVsDelta.containsKey(FIXED_CATEGORY)) {
                        List<String> deltaCounters = categoryVsDelta.get(FIXED_CATEGORY);
                        if (deltaCounters.contains(counterName)) {
                            counterValueMap.put(counterName, value);
                        } else {
                        }
                    } else {
                    }
                } else {
                    counterValueMap.put(counterName, value);
                }
                counterValueMap.put("INTERFACE_DESC", interfaceName);

                String pmemsid = entityName + "_" + interfaceName;
                System.out.println("Generated Pmemsid: " + pmemsid);

                String rowKey = FIXED_CATEGORY + "##" + pmemsid + "##" + dateTime + "##" + index;
                System.out.println("Generated Row Key: " + rowKey);
                fileContent.add(RowFactory.create(processingTime, rowKey, counterValueMap));
                index++;

            } catch (Exception e) {
                logger.error("Exception While Processing CSV Line: {}", record, e);
            }
        }
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

    private String getTimeForQuarterly(String timeKey, Integer mins) throws ParseException {
        try {

            if (timeKey.length() == 12) {
                timeKey += "00";
            }
            SimpleDateFormat inputFormatter = new SimpleDateFormat(QUARTER_FORMAT);
            if (timeKey == null || timeKey.isEmpty() || timeKey.equalsIgnoreCase("Period")) {
                return DEFAULT_PROCESSING_TIME;
            }
            Date date = inputFormatter.parse(timeKey);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(DateUtils.addMinutes(date, -mins));

            int minute = calendar.get(Calendar.MINUTE);
            int roundedMinute = (minute / 15) * 15;

            calendar.set(Calendar.MINUTE, roundedMinute);
            calendar.set(Calendar.SECOND, 0);
            SimpleDateFormat outputFormatter = new SimpleDateFormat(QUARTER_FORMAT);
            return outputFormatter.format(calendar.getTime());

        } catch (Exception e) {
            logger.error("Error Extracting Time For Quarterly: {}, Mins: {} With Error: {}", timeKey, mins,
                    e.getMessage());
        }
        return DEFAULT_PROCESSING_TIME;
    }

    @Override
    public String getName() {
        return "AdvaParser";
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
