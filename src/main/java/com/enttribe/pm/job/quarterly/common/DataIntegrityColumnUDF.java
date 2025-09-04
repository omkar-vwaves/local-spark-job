package com.enttribe.pm.job.quarterly.common;

import java.util.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.enttribe.sparkrunner.udf.AbstractUDF;

public class DataIntegrityColumnUDF implements
        UDF7<String, scala.collection.immutable.Map<String, String>, String, String, String, String, String, List<Row>>,
        AbstractUDF {

    static long startTime;
    static {
        startTime = System.currentTimeMillis();
    }
    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataIntegrityColumnUDF.class);

    @Override
    public List<Row> call(String rowKey, scala.collection.immutable.Map<String, String> metaData, String processingDate,
            String processingHour, String jobType, String emsType, String technology) {

        logger.info("DataIntegrityColumnUDF Execution Started :: ");

        List<Row> rowList = new ArrayList<>();

        try {

            Map<String, String> mapAsJavaMap = scala.jdk.CollectionConverters.MapHasAsJava(metaData).asJava();

            String timeStamp = String.valueOf(getTimeStampForRowKey(processingDate + processingHour));
            Map<String, String> updatedDIColumnMap = updateDIColumn(mapAsJavaMap, processingDate, processingHour,
                    emsType);

            if (rowKey == null || rowKey.isEmpty()) {
                rowKey = mapAsJavaMap.get("RK") + getRowsuffix(jobType, processingDate + processingHour);
            } else {
                String separator = "##";
                int lastIndex = rowKey.lastIndexOf(separator);
                String timeKey = (lastIndex != -1) ? rowKey.substring(lastIndex + separator.length()) : rowKey;
                String prefixPart = (lastIndex != -1) ? rowKey.substring(0, lastIndex) : "";

                rowKey = prefixPart + getRowsuffix(jobType, timeKey);
            }

            if (rowKey != null) {
                rowKey = rowKey.replace("##", "").replace("null", "");
            }

            updatedDIColumnMap.remove("RK");
            updatedDIColumnMap.put("T", technology);
            updatedDIColumnMap.put("PD", updatedDIColumnMap.get("AD"));
            updatedDIColumnMap.put("PT", updatedDIColumnMap.get("AT"));

            Row row = (Row) RowFactory
                    .create(new Object[] { rowKey, timeStamp,
                            updatedDIColumnMap.get("AD"),
                            updatedDIColumnMap.get("AT"),
                            updatedDIColumnMap.get("DN"),
                            updatedDIColumnMap.get("E"),
                            updatedDIColumnMap.get("EMS"),
                            updatedDIColumnMap.get("HR"),
                            updatedDIColumnMap.get("NE"),
                            updatedDIColumnMap.get("NT"),
                            updatedDIColumnMap.get("PD"),
                            updatedDIColumnMap.get("PT"),
                            updatedDIColumnMap.get("R"),
                            updatedDIColumnMap.get("STS"),
                            updatedDIColumnMap.get("VR"),
                            updatedDIColumnMap.get("T") });
            rowList.add(row);

            long endTime = System.currentTimeMillis();
            logger.info("DataIntegrityColumnUDF Execution Completed : {} Milliseconds", endTime - startTime);

        } catch (Exception e) {
            logger.error("Exception Occured In DataIntegrityColumnUDF : {}", e.getMessage(), e);
        }
        return rowList;
    }

    private Map<String, String> updateDIColumn(Map<String, String> mapAsJavaMap,
            String processingDate, String processingHour, String emsType) {

        Map<String, String> columnMap = new HashMap<>();
        // String actualDate = mapAsJavaMap.get("Date"); //250626
         String actualDate = mapAsJavaMap.get("date"); //20250626
        String actualHour = mapAsJavaMap.get("Time"); // 0000

        columnMap.put("E", "1");
        columnMap.put("EMS", emsType);
        columnMap.put("HR", processingHour);

        if (actualDate != null && !actualDate.isEmpty() && actualHour != null && !actualHour.isEmpty()) {

            String actualDateTime = actualDate + actualHour; // 2506260000
            columnMap.put("AD", actualDate);
            columnMap.put("AT", actualDateTime);

            String processingDateTime = processingDate + processingHour + "00"; // 20250626061500

            if (actualDateTime.equalsIgnoreCase(processingDateTime)) {
                columnMap.put("STS", "RECEIVED");
                columnMap.put("R", "1");
            } else {
                columnMap.put("STS", "DELAYED"); // s
                columnMap.put("D", "1");
                columnMap.put("R", "1");
            }
        } else {
            columnMap.put("STS", "NOT_RECEIVED");
            columnMap.put("R", "0");
        }

        columnMap.putAll(putDIColumn(mapAsJavaMap, processingDate, processingHour));
        return columnMap;
    }

    private Map<String, String> putDIColumn(Map<String, String> mapAsJavaMap, String processingDate,
            String processingHour) {

        Map<String, String> columnMap = new HashMap<>();
        columnMap.put("PD", processingDate);
        columnMap.put("PT", processingDate + processingHour + "00");
        // columnMap.put("NE", mapAsJavaMap.getOrDefault("NAM", ""));
        // columnMap.put("NT", mapAsJavaMap.getOrDefault("NET", ""));
        columnMap.put("NE", mapAsJavaMap.getOrDefault("ENTITY_NAME", ""));
        columnMap.put("NT", mapAsJavaMap.getOrDefault("ENTITY_TYPE", ""));
       
        columnMap.put("DN", mapAsJavaMap.getOrDefault("D", ""));
        columnMap.put("VR", mapAsJavaMap.getOrDefault("V", ""));

        return columnMap;
    }

    private long getTimeStampForRowKey(String processingTime) {
        long timestamp = System.currentTimeMillis();
        try {
            timestamp = com.enttribe.commons.lang.DateUtils.parse("yyMMddHHmm", processingTime + "00").getTime();
        } catch (Exception e) {
            logger.error("Exception in Getting Timestamp : {}", e.getMessage(), e);
        }
        return timestamp;
    }

    private String getRowsuffix(String jobType, String timekey) {
        String updateTimeKey;
        jobType = jobType.toUpperCase();

        switch (jobType) {
            case "QUARTERLY" -> updateTimeKey = timekey.substring(0, 10);
            case "HOURLY" -> updateTimeKey = timekey.substring(0, 8);
            case "BBH", "DAILY", "NBH" -> updateTimeKey = timekey.substring(0, 6);
            case "WEEKLY" -> {
                LocalDate date = LocalDate.parse(timekey, DateTimeFormatter.ofPattern("yyyyMMdd"));
                date = date.minusDays(1);
                DateTimeFormatter weekFormatter = DateTimeFormatter.ofPattern("yyww");
                updateTimeKey = date.format(weekFormatter);
            }
            case "MONTHLY" -> {
                LocalDate date = LocalDate.parse(timekey, DateTimeFormatter.ofPattern("yyyyMMdd"));
                DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyMM");
                updateTimeKey = date.format(monthFormatter);
            }
            default -> updateTimeKey = timekey;
        }
        return updateTimeKey;
    }

    @Override
    public String getName() {
        return "DataIntegrityColumnUDF";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true)); 
        fields.add(DataTypes.createStructField("timeStamp", DataTypes.StringType, true)); // TIMESTAMP IN EPOCH FROM FILE
        fields.add(DataTypes.createStructField("AD", DataTypes.StringType, true)); // ACTUAL DATE (FROM META JSON) FROM FILE
        fields.add(DataTypes.createStructField("AT", DataTypes.StringType, true)); // ACTUAL TIME (FROM META JSON) FROM FILE
        fields.add(DataTypes.createStructField("DN", DataTypes.StringType, true)); // DOMAIN
        fields.add(DataTypes.createStructField("E", DataTypes.StringType, true)); // ?
        fields.add(DataTypes.createStructField("EMS", DataTypes.StringType, true)); // EMS_TYPE
        fields.add(DataTypes.createStructField("HR", DataTypes.StringType, true)); // PROCESSING HOUR (HHMM)
        fields.add(DataTypes.createStructField("NE", DataTypes.StringType, true)); // ENTITY NAME (NODE NAME -> FILE LEVEL)
        fields.add(DataTypes.createStructField("NT", DataTypes.StringType, true)); // NE TYPE (ENTITY TYPE -> FILE LEVEL)
        fields.add(DataTypes.createStructField("PD", DataTypes.StringType, true)); // PROCESSING DATE - GENERATE FLOW FILE
        fields.add(DataTypes.createStructField("PT", DataTypes.StringType, true)); // PROCESSING TIME - GENERATE FLOW FILE
        fields.add(DataTypes.createStructField("R", DataTypes.StringType, true)); // RECEIVED (TRUE=1, FALSE=0)
        fields.add(DataTypes.createStructField("STS", DataTypes.StringType, true)); // STATUS (RECEIVED, DELAYED, NOT_RECEIVED)
        fields.add(DataTypes.createStructField("VR", DataTypes.StringType, true)); // VENDOR
        fields.add(DataTypes.createStructField("T", DataTypes.StringType, true)); // TECHNOLOGY
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

}
