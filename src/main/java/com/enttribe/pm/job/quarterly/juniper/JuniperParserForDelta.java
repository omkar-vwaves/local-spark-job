package com.enttribe.pm.job.quarterly.juniper;

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

public class JuniperParserForDelta implements UDF4<String, String, byte[], Boolean, List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(JuniperParserForDelta.class);
    public JobContext jobcontext;
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

                    // logger.info("[getCategoryVsDeltaCounterMap] - Starting initialization of categoryVSDetla map");

                    String parameter = jobcontext.getParameter(CATEGORY_VS_DELTACOUNTER);
                    ObjectMapper mapper = new ObjectMapper();

                    try {

                        categoryVSDetla = mapper.readValue(parameter, new TypeReference<Map<String, List<String>>>() {
                        });

                        // logger.info(
                        //         "[getCategoryVsDeltaCounterMap] - categoryVSDetla map initialized successfully with size: {}",
                        //         categoryVSDetla.size());

                    } catch (Exception e) {
                        logger.error(
                                "[getCategoryVsDeltaCounterMap] - Error during initialization of categoryVSDetla map: {}",
                                e.getMessage(), e);
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

            List<Row> rowList = new JuniperParserForDelta().call(zipFilePath, fileName,
                    jsonContent, false);
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
    public List<Row> call(String zipFilePath, String fileName, byte[] jsonContent, Boolean isDelta) throws Exception {

        // logger.info("JuniperParser: Zip File Path: {}, File Name: {}, JSON Content Length : {}", zipFilePath, fileName,
        //         jsonContent.length);

        List<Row> fileContent = new ArrayList<>();
        String jsonString = new String(jsonContent);
        String[] lines = jsonString.split("\n");

        int index = 1;
        if (isDelta) {

            // logger.info("[processDeltaFlag] - isDelta flag is true, invoking getCategoryVsDeltaCounterMap");
            getCategoryVsDeltaCounterMap();

        } else {
            // logger.info(
            //         "[processDeltaFlag] - isDelta flag is false, skipping getCategoryVsDeltaCounterMap invocation");
        }
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
                // String pmemsid = tagObject.getString("router_ip");

                Map<String, String> counterValueMap = new HashMap<>();
                if (pmemsid != null) {
                    try {

                        String interfaceDesc = tagObject.getString("ifDescr");
                        if (!isDelta) {
                            counterValueMap.put("IFDESCR", interfaceDesc);
                        }

                        String extractedInterfaceDesc = interfaceDesc.split(",")[0];
                        if (extractedInterfaceDesc != null) {
                            pmemsid = pmemsid + "_" + extractedInterfaceDesc; // Added
                        }
                        // 172.31.31.148_ge-0/1/7
                    } catch (Exception e) {
                        logger.error("Exception While getting interface_desc:");
                    }
                }

                // EXTRACT TIMESTAMP
                String dateTime = getQuarterlyDateTime(mainJSONObject.getLong("timestamp"));

                // INITIALIZE COUNTER VALUE MAP
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
                logger.error("JuniperParser: Exception While Processing JSON Line: {}, Error Message: {}", line,
                        e.getMessage());
            }
        }

        // System.out.println("JuniperParser: Result Size : " + index);
        return fileContent;
    }

    private static String getQuarterlyDateTime(Long epochSeconds) {

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(epochSeconds * 1000L);

        int minute = calendar.get(Calendar.MINUTE);
        int flooredMinute = (minute / 15) * 15;

        calendar.set(Calendar.MINUTE, flooredMinute);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
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