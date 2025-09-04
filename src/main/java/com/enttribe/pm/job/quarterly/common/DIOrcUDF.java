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

public class DIOrcUDF implements
        UDF7<String, scala.collection.immutable.Map<String, String>, String, String, String, String, String, List<Row>>,
        AbstractUDF {

    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DIOrcUDF.class);

    @Override
    public List<Row> call(String rowKey, scala.collection.immutable.Map<String, String> metaData, String processingDate,
            String processingHour, String jobType, String emsType, String technology) {

        List<Row> rowList = new ArrayList<>();
        try {

            Map<String, String> mapAsJavaMap = scala.jdk.CollectionConverters.MapHasAsJava(metaData).asJava();

            logger.info(
                    "Entering DIOrcUDF With Parameters: rowKey={}, processingDate={}, processingHour={}, jobType={}, emsType={}, technology={}",
                    rowKey, processingDate, processingHour, jobType, emsType, technology);

            logger.info("🚀 DIOrcUDF Received MetaData: {}", mapAsJavaMap);

            String entityName = mapAsJavaMap.get("NAM");
            String entityType = mapAsJavaMap.get("NET");
            String isExist = "1";
            String domain = mapAsJavaMap.get("D");
            String vendor = mapAsJavaMap.get("V");
            String actualDate = mapAsJavaMap.get("Date"); // YYMMDD
            String actualHour = mapAsJavaMap.get("Time"); // HHMM

            String actualTime = actualDate + actualHour;
            String timestamp = String.valueOf(convertToEpochMiliseconds(actualTime));
            String processingTime = processingDate + processingHour; // YYMMDD + HHMM

            String status = null;
            String isReceived = null;

            logger.info("🚀 Actual Date: {}, Actual Hour: {}, Actual Time: {}, Processing Date: {}, Processing Hour: {}, Processing Time: {}",
                    actualDate, actualHour, actualTime, processingDate, processingHour, processingTime);

            if (actualDate != null && !actualDate.isEmpty() && actualHour != null && !actualHour.isEmpty()) {
                if (actualTime.equalsIgnoreCase(processingTime)) {
                    status = "RECEIVED";
                    isReceived = "1";
                } else {
                    status = "DELAYED";
                    isReceived = "1";
                }
            } else {
                status = "NOT_RECEIVED";
                isReceived = "0";
            }

            if (rowKey == null || rowKey.isEmpty()) {
                rowKey = computeTimeKeyByJobType(jobType, processingTime);
            } else {
                int lastIndex = rowKey.lastIndexOf("##");
                String timeKey = (lastIndex != -1) ? rowKey.substring(lastIndex + 2) : rowKey;
                String prefixPart = (lastIndex != -1) ? rowKey.substring(0, lastIndex) : "";
                rowKey = prefixPart + computeTimeKeyByJobType(jobType, timeKey);
            }

            rowKey = rowKey.replace("##", "").replace("null", "");

            Row row = (Row) RowFactory
                    .create(new Object[] { rowKey, timestamp, isExist, processingHour, entityName, entityType,
                            processingDate, isReceived, status, domain, vendor, emsType, technology, actualDate,
                            actualTime, processingTime });
            rowList.add(row);

        } catch (Exception e) {
            logger.error("Exception Occured In DIOrcUDF : {}", e.getMessage(), e);
        }
        return rowList;
    }

    private long convertToEpochMiliseconds(String processingTime) {
        try {
            return com.enttribe.commons.lang.DateUtils
                    .parse("yyMMddHHmm", processingTime)
                    .getTime();

        } catch (Exception e) {
            long fallbackTimestamp = System.currentTimeMillis();
            logger.error("Failed to Parse processingTime '{}'. Returning Current time: {}. Error: {}",
                    processingTime, fallbackTimestamp, e.getMessage(), e);
            return fallbackTimestamp;
        }
    }

    private String computeTimeKeyByJobType(String jobType, String processingTime) {
        String updateTimeKey = processingTime;
        jobType = jobType != null ? jobType.toUpperCase() : "";

        try {
            switch (jobType) {
                case "QUARTERLY" -> {
                    if (processingTime.length() >= 10) {
                        updateTimeKey = processingTime.substring(0, 10); // YYMMDDHHMM
                    }
                }
                case "HOURLY" -> {
                    if (processingTime.length() >= 8) {
                        updateTimeKey = processingTime.substring(0, 8); // YYMMDDHH
                    }
                }
                case "BBH", "DAILY", "NBH" -> {
                    if (processingTime.length() >= 6) {
                        updateTimeKey = processingTime.substring(0, 6); // YYMMDD
                    }
                }
                case "WEEKLY" -> {
                    if (processingTime.length() >= 6) {
                        String padded = "20" + processingTime.substring(0, 6); // convert YYMMDD → YYYYMMDD
                        LocalDate date = LocalDate.parse(padded, DateTimeFormatter.ofPattern("yyyyMMdd"));
                        date = date.minusDays(1);
                        updateTimeKey = date.format(DateTimeFormatter.ofPattern("yyww")); // YYWW
                    }
                }
                case "MONTHLY" -> {
                    if (processingTime.length() >= 6) {
                        String padded = "20" + processingTime.substring(0, 6); // convert YYMMDD → YYYYMMDD
                        LocalDate date = LocalDate.parse(padded, DateTimeFormatter.ofPattern("yyyyMMdd"));
                        updateTimeKey = date.format(DateTimeFormatter.ofPattern("yyMM")); // YYMM
                    }
                }
                default -> {
                }
            }
        } catch (Exception e) {
            logger.error("Failed to Compute Row Suffix For jobType={} With input={}. Returning Original: {}",
                    jobType, processingTime, e.getMessage());
        }

        return updateTimeKey;
    }

    @Override
    public String getName() {
        return "DIOrcUDF";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("ROW_KEY", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("TIMESTAMP", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("IS_EXIST", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("PROCESSING_HOUR", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ENTITY_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ENTITY_TYPE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("PROCESSING_DATE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("IS_RECEIVED", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("STATUS", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("DOMAIN", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("VENDOR", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("EMS_TYPE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("TECHNOLOGY", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ACTUAL_DATE", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ACTUAL_TIME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("PROCESSING_TIME", DataTypes.StringType, true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

}
