package com.enttribe.pm.job.quarterly.nokia;

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

public class NokiaParserForDelta implements UDF4<String, String, byte[], Boolean, List<Row>>, AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(NokiaParserForDelta.class);
    private JobContext jobcontext;
    private static final String CATEGORY_VS_DELTACOUNTER = "CATEGORY_VS_DELTACOUNTER";
    private static Map<String, List<String>> categoryVSDetla = null;
    private static Object SYNCHRONIZER = new Object();

    public NokiaParserForDelta(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public NokiaParserForDelta() {
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
        String fileName = "Nokia-switch.json";

        try {
            byte[] jsonContent = Files
                    .readAllBytes(Paths.get("/Users/bootnext-mac-111/Downloads/snmp-output.json"));

            List<Row> rowList = new NokiaParserForDelta().call(zipFilePath, fileName,
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

        logger.info("Zip File Path: {}, File Name: {}, JSON Content Length : {}", zipFilePath, fileName,
                jsonContent.length);

        List<Row> fileContent = new ArrayList<>();
        String jsonString = new String(jsonContent);
        String[] lines = jsonString.split("\n");

        int index = 1;
        
            getCategoryVsDeltaCounterMap();

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
                Map<String, String> deltaCounterValueMap = new HashMap<>();
                if (pmemsid != null) {
                    try {

                        String interfaceDesc = tagObject.getString("ifDescr");
                        // if (!isDelta) {
                        //     counterValueMap.put("IFDESCR", interfaceDesc);
                        // }
                        counterValueMap.put("IFDESCR", interfaceDesc);
                        // deltaCounterValueMap.put("IFDESCR", interfaceDesc);

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
                
                List<String> deltaKeys = categoryVSDetla != null ? categoryVSDetla.get(category) : null;
                        for (String key : fieldsJSONObject.keySet()) {
                    String value = fieldsJSONObject.optString(key, null);
                    if (value != null && !"null".equals(value)) {
                        String upperKey = key.toUpperCase();
                        counterValueMap.put(upperKey, value);
                        if (deltaKeys != null && deltaKeys.contains(upperKey)) {
                            deltaCounterValueMap.put(upperKey, value);
                        }
                    }
                }

                String baseKey = category + "##" + pmemsid ; // 1200 1145
                // String deltaMapKey = baseKey + "##" + index++ ;
                // String mapKey = baseKey + "##" + index++;
                // String deltaMapKey = baseKey;
                // String mapKey = baseKey ;
                if(!deltaCounterValueMap.isEmpty()){
                    fileContent.add(RowFactory.create(processingTime, baseKey, deltaCounterValueMap, true,dateTime));
                }
                fileContent.add(RowFactory.create(processingTime, baseKey, counterValueMap, false,dateTime));
                

            } catch (Exception e) {
                logger.error("NokiaParser: Exception While Processing JSON Line: {}, Error Message: {}", line,
                        e.getMessage());
            }
        }
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
        return "NokiaParser";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("PT", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("mapKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("counterValueMap",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
        fields.add(DataTypes.createStructField("isDelta", DataTypes.BooleanType, true));
        fields.add(DataTypes.createStructField("dateTime", DataTypes.StringType, true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }
}