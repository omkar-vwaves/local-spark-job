package com.enttribe.pm.job.report.common;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF10;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.json.JSONArray;
import org.json.JSONObject;

import com.enttribe.sparkrunner.context.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.udf.AbstractUDF;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReadConfigUDF implements
        UDF10<String, String, String, String, String, String, String, String, String, String, List<Row>>, AbstractUDF {

    private static final Logger logger = LoggerFactory.getLogger(ReadConfigUDF.class);
    public JobContext jobContext;
    private static Map<String, String> jobContextMap = new HashMap<>();

    private static String FALLBACK_SPARK_PM_JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static String FALLBACK_SPARK_PM_JDBC_URL = "jdbc:mysql://mysql-nst-cluster.nstdb.svc.cluster.local:6446/PERFORMANCE?autoReconnect=true";
    private static String FALLBACK_SPARK_PM_JDBC_USERNAME = "PERFORMANCE";
    private static String FALLBACK_SPARK_PM_JDBC_PASSWORD = "perform!123";

    @Override
    public String getName() {
        return "ReadReportConfig";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("REPORT_WIDGET_DETAILS", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("NODE_AND_AGGREGATION_DETAILS", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("EXTRA_PARAMETERS", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("META_COLUMNS", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("KPI_MAP", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("RAG_CONFIGURATION", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("KPI_GROUP_MAP", DataTypes.StringType, true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

    @Override
    public List<Row> call(String reportWidgetIdPk, String generatedReportId, String domain, String vendor,
            String technology,
            String reportMeasure, String reportName, String frequency, String generatedType, String configuration)
            throws Exception {

        logger.info("[ReadConfigUDF] UDF Execution Started!");
        jobContextMap = jobContext.getParameters();

        List<Row> rows = new ArrayList<>();

        Map<String, String> reportWidgetDetailsMap = new LinkedHashMap<>();
        reportWidgetDetailsMap.put("reportWidgetIdPk", reportWidgetIdPk);
        reportWidgetDetailsMap.put("generatedReportId", generatedReportId);
        reportWidgetDetailsMap.put("domain", domain);
        reportWidgetDetailsMap.put("vendor", vendor);
        reportWidgetDetailsMap.put("technology", technology);
        reportWidgetDetailsMap.put("reportMeasure", reportMeasure);
        reportWidgetDetailsMap.put("reportName", reportName);
        frequency = frequency.equalsIgnoreCase("Hour") ? "PERHOUR" : frequency;
        frequency = frequency.equalsIgnoreCase("Day") ? "PERDAY" : frequency;
        reportWidgetDetailsMap.put("frequency", frequency);
        reportWidgetDetailsMap.put("configuration", configuration);
        reportWidgetDetailsMap.put("generatedType", generatedType);
        processTheReportWidgetDetails(reportWidgetDetailsMap, rows);
        logger.info("[ReadConfigUDF] Execution Completed!");

        return rows;
    }

    private static Map<String, String> getMetaColumnsMap(String metaColumns, Map<String, String> extraParametersMap) {
        logger.info("[ReadConfigUDF] Getting Meta Columns Map!");
        Map<String, String> metaColumnsMap = new LinkedHashMap<>();
        String[] metaColumnsArray = metaColumns.split("#");
        String str1 = metaColumnsArray[0];
        String str2 = metaColumnsArray[1];
        String[] str1Array = str1.split(",");
        String[] str2Array = str2.split(",");

        for (int i = 0; i < str1Array.length; i++) {
            metaColumnsMap.put(str1Array[i].toUpperCase().trim(), str2Array[i].trim());
        }
        logger.info("[ReadConfigUDF] Meta Columns Map: {}", metaColumnsMap);
        if (!metaColumnsMap.containsKey("DT")) {
            metaColumnsMap.put("DT", "Date");
        }
        if (!metaColumnsMap.containsKey("HR")) {
            metaColumnsMap.put("HR", "Time");
        }
        return metaColumnsMap;
    }

    public static Map<String, List<String>> getKpiGroupMap(String configuration) {
        Map<String, List<String>> kpiGroupMap = new HashMap<>();

        String fixedJson = configuration.replace("'", "\"");
        JSONObject jsonObject = new JSONObject(fixedJson);

        String reportFormatType = jsonObject.optString("reportFormatType", "").trim();
        if (reportFormatType.equalsIgnoreCase("csv")) {
            return kpiGroupMap;
        }

        JSONArray kpiArray = jsonObject.optJSONArray("kpi");
        if (kpiArray == null) {
            return kpiGroupMap;
        }

        Map<String, String> individualKpiGroupMap = new LinkedHashMap<>();

        for (int i = 0; i < kpiArray.length(); i++) {
            JSONObject kpiObject = kpiArray.getJSONObject(i);

            String headerName = kpiObject.optString("headerName", "").trim();
            if (headerName.isEmpty()) {
                headerName = kpiObject.optString("kpiName", "N/A").trim();
            }

            String kpiGroup = kpiObject.optString("kpigroup", "OTHER").trim();
            if (kpiGroup.isEmpty()) {
                kpiGroup = "OTHER";
            }

            individualKpiGroupMap.put(headerName, kpiGroup);
        }

        for (Map.Entry<String, String> entry : individualKpiGroupMap.entrySet()) {
            String headerName = entry.getKey();
            String group = entry.getValue();

            kpiGroupMap.computeIfAbsent(group, k -> new ArrayList<>()).add(headerName);
        }

        logger.info("Individual KPI Group Map: {}", individualKpiGroupMap);
        logger.info("Distinct Groups: {}", kpiGroupMap.keySet());
        logger.info("KPI Group Map: {}", kpiGroupMap);

        return kpiGroupMap;
    }

    private static Map<String, String> getKPIDetails(String configuration) {

        String fixedJson = configuration.replace("'", "\"");
        JSONObject jsonObject = new JSONObject(fixedJson);

        Map<String, String> kpiMap = new HashMap<>();

        try {
            JSONArray kpiArray = jsonObject.getJSONArray("kpi");

            for (int i = 0; i < kpiArray.length(); i++) {
                JSONObject kpiObject = kpiArray.getJSONObject(i);
                String kpiCode = kpiObject.optString("kpicode", "").trim();
                String kpiName = kpiObject.optString("kpiName", "").trim();
                kpiMap.put(kpiCode, kpiName);
            }

        } catch (Exception e) {
            System.out.println("Error in Extracting KPI List, Message: " + e.getMessage() + ", Error: " + e);
        }

        logger.info("[ReadConfigUDF] KPI Map: {}", kpiMap);

        return kpiMap;

    }

    private void processTheReportWidgetDetails(Map<String, String> reportWidgetDetailsMap, List<Row> rows)
            throws JsonProcessingException {

        Map<String, String> nodeAndAggregationDetailsMap = getNodeAndAggregationDetails(reportWidgetDetailsMap);
        Map<String, String> extraParametersMap = getExtraParameters(reportWidgetDetailsMap);
        String metaCols = extraParametersMap.get("metaColumns");
        Map<String, String> metaColumnsMap = getMetaColumnsMap(metaCols, extraParametersMap);

        String configuration = reportWidgetDetailsMap.get("configuration");
        Map<String, String> kpiMap = getKPIDetails(configuration);
        Map<String, List<String>> kpiGroupMap = getKpiGroupMap(configuration);

        Map<String, Map<String, String>> ragConfigurationMap = getRagConfiguration(configuration);
        if (ragConfigurationMap == null || ragConfigurationMap.isEmpty()) {
            ragConfigurationMap = new HashMap<>();
        }

        String reportWidgetDetails = new ObjectMapper().writeValueAsString(reportWidgetDetailsMap);
        String nodeAndAggregationDetails = new ObjectMapper().writeValueAsString(nodeAndAggregationDetailsMap);
        String extraParameters = new ObjectMapper().writeValueAsString(extraParametersMap);
        String metaColumns = new ObjectMapper().writeValueAsString(metaColumnsMap);
        String kpiMapDetails = new ObjectMapper().writeValueAsString(kpiMap);
        String ragConfiguration = new ObjectMapper().writeValueAsString(ragConfigurationMap);
        String kpiGroupMapJson = new ObjectMapper().writeValueAsString(kpiGroupMap);

        logger.info("[ReadConfigUDF] Report Widget Details: {}", reportWidgetDetails);
        logger.info("[ReadConfigUDF] Node and Aggregation Details: {}", nodeAndAggregationDetails);
        logger.info("[ReadConfigUDF] Extra Parameters: {}", extraParameters);
        logger.info("[ReadConfigUDF] Meta Columns: {}", metaColumns);
        logger.info("[ReadConfigUDF] KPI Map: {}", kpiMapDetails);
        logger.info("[ReadConfigUDF] RAG Configuration: {}", ragConfiguration);
        logger.info("[ReadConfigUDF] KPI Group Map: {}", kpiGroupMapJson);

        jobContext.setParameters("REPORT_WIDGET_DETAILS", reportWidgetDetails);
        jobContext.setParameters("NODE_AND_AGGREGATION_DETAILS", nodeAndAggregationDetails);
        jobContext.setParameters("EXTRA_PARAMETERS", extraParameters);
        jobContext.setParameters("META_COLUMNS", metaColumns);
        jobContext.setParameters("KPI_MAP", kpiMapDetails);
        jobContext.setParameters("RAG_CONFIGURATION", ragConfiguration);
        jobContext.setParameters("KPI_GROUP_MAP", kpiGroupMapJson);

        Row row = RowFactory.create(reportWidgetDetails, nodeAndAggregationDetails, extraParameters, metaColumns,
                kpiMapDetails, ragConfiguration, kpiGroupMapJson);
        rows.add(row);
    }

    private static Map<String, String> getExtraParameters(Map<String, String> reportWidgetDetails) {

        Map<String, String> extraParameters = new LinkedHashMap<>();

        try {

            String fixedJson = reportWidgetDetails.get("configuration").replace("'", "\"");
            JSONObject jsonObject = new JSONObject(fixedJson);

            String metaColumns = jsonObject.optString("metaColumns", "");
            String reportFormatType = jsonObject.optString("reportFormatType", "");
            String toDate = jsonObject.optString("toDate", "");
            String fromDate = jsonObject.optString("fromDate", "");
            String kpiExpression = jsonObject.optString("kpiExpression", "");
            String generatedType = reportWidgetDetails.get("generatedType");
            String selectedHeader = jsonObject.optString("selectedHeader", "default");
            String startTime = "";
            String endTime = "";
            logger.info("Input From Date: {}, To Date: {}, Generated Type: {}", fromDate, toDate, generatedType);
            if (generatedType.equalsIgnoreCase("SCHEDULED")) {

                // ${domain}_${technology}_${vendor}_${ems_type}_${frequency}_KPICOMPUTATION',

                String duration = jsonObject.optString("duration", "");
                String frequency = jsonObject.getJSONArray("frequency").getString(0);
                String numberOfInstance = jsonObject.optString("numberOfInstance", "1");
                numberOfInstance = numberOfInstance.isEmpty() ? "1" : numberOfInstance;
                String domain = reportWidgetDetails.get("domain");
                String technology = reportWidgetDetails.get("technology");
                String vendor = reportWidgetDetails.get("vendor");
                String emsType = "NA";

                String freqBaseKEY = frequency;

                if (frequency.equalsIgnoreCase("PERHOUR")) {
                    freqBaseKEY = "HOURLY";
                } else if (frequency.equalsIgnoreCase("PERDAY")) {
                    freqBaseKEY = "DAILY";
                } else if (frequency.equalsIgnoreCase("PERWEEK")) {
                    freqBaseKEY = "WEEKLY";
                } else if (frequency.equalsIgnoreCase("PERMONTH")) {
                    freqBaseKEY = "MONTHLY";
                }

                String configKey = domain + "_" + technology + "_" + vendor + "_" + emsType + "_" + freqBaseKEY
                        + "_KPICOMPUTATION";
                String configTag = "PM";
                String configType = "PM";
                String applicationName = "PERFORMANCE_MANAGEMENT_APP_NAME";
                logger.info(
                        "[SCHEDULED] Duration: {}, Frequency: {}, Vendor: {}, Domain: {}, Technology: {}, EMS Type: {}",
                        duration, frequency, vendor, domain, technology, emsType);
                logger.info("[SCHEDULED] Base Config Key: {}, Config Tag: {}, Config Type: {}, Application Name: {}",
                        configKey, configTag, configType, applicationName);

                String configValue = getConfigValueFromBaseConfig(configKey, configTag, configType, applicationName,
                        frequency);

                logger.info("[SCHEDULED] Config Value: {}", configValue);

                List<String> timeStampList = getTimeStampList(configValue, duration, numberOfInstance, frequency);
                List<String> dateList = getDateListBasedOnEpoch(timeStampList, frequency);

                logger.info("[SCHEDULED] TimeStamp List: {}", timeStampList);
                logger.info("[SCHEDULED] Date List: {}", dateList);

                String scheduleTimestampFilter = timeStampList.stream()
                        .map(epoch -> {
                            LocalDateTime dateTime = LocalDateTime.ofInstant(
                                    java.time.Instant.ofEpochMilli(Long.parseLong(epoch)),
                                    ZoneOffset.UTC);
                            return "'" + dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'";
                        })
                        .collect(Collectors.joining(","));

                String scheduleDateFilter = dateList.stream()
                        .map(s -> "'" + s + "'")
                        .collect(Collectors.joining(","));

                logger.info("[SCHEDULED] Timestamp Filter: {}", scheduleTimestampFilter);
                logger.info("[SCHEDULED] Date Filter: {}", scheduleDateFilter);

                extraParameters.put("scheduleTimestampFilter", scheduleTimestampFilter);
                extraParameters.put("scheduleDateFilter", scheduleDateFilter);

            } else {
                // startTime = convertToExpectedTimeStampFormat(toDate);
                // endTime = convertToExpectedTimeStampFormat(fromDate);
                startTime = convertToExpectedTimeStampFormat(fromDate);
                endTime = convertToExpectedTimeStampFormat(toDate);

            }

            String utcOffsetInMinute = "0";
            if (jsonObject.has("utcOffsetInMinute")) {
                utcOffsetInMinute = jsonObject.optString("utcOffsetInMinute", "0");
            }

            extraParameters.put("metaColumns", metaColumns);
            extraParameters.put("reportFormatType", reportFormatType);
            extraParameters.put("toDate", toDate);
            extraParameters.put("fromDate", fromDate);
            extraParameters.put("startTime", startTime);
            extraParameters.put("endTime", endTime);
            extraParameters.put("kpiExpression", kpiExpression);
            extraParameters.put("selectedHeader", selectedHeader);
            extraParameters.put("utcOffsetInMinute", utcOffsetInMinute);

            String isSpecificDate = "false";
            JSONArray specificDate = jsonObject.getJSONArray("specificDate");
            if (specificDate.length() > 0) {
                isSpecificDate = "true";
            }
            extraParameters.put("isSpecificDate", isSpecificDate);

            if (isSpecificDate.equals("true")) {

                List<Long> specificDateList = new ArrayList<>();

                logger.info("Specific Date Found, Processing Specific Date List!");

                for (int i = 0; i < specificDate.length(); i++) {
                    specificDateList.add(specificDate.getLong(i));
                }

                String dateList = specificDateList.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));

                logger.info("Formatted Date List: {}", dateList);

                extraParameters.put("dateList", dateList);

            } else {
                logger.info("No Specific Date Found, Using Default Date Range!");
            }

        } catch (Exception e) {
            logger.error("Error in Getting CQL Parameters, Message: {}, Error: {}", e.getMessage(), e);

        }

        logger.info("[ReadConfigUDF] Extra Parameters: {}", extraParameters);
        return extraParameters;
    }

    private static List<String> getDateListBasedOnEpoch(List<String> timeStampList, String frequency) {

        logger.info("[ReadConfigUDF] Getting Date List Based On Epoch - Frequency: {}", frequency);
        LinkedHashSet<String> uniqueDates = new LinkedHashSet<>();

        DateTimeFormatter formatter = null;
        if (frequency.equalsIgnoreCase("PERHOUR") || frequency.equalsIgnoreCase("PERDAY")
                || frequency.equalsIgnoreCase("PERWEEK") || frequency.equalsIgnoreCase("PERMONTH")) {
            formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        }

        for (String timeStamp : timeStampList) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(Long.parseLong(timeStamp)),
                    ZoneOffset.UTC);
            String formattedDate = dateTime.format(formatter);
            uniqueDates.add(formattedDate);
        }

        List<String> dateList = new ArrayList<>(uniqueDates);

        logger.info("[ReadConfigUDF] Generated Unique Date List: {} (Original: {} timestamps, Unique: {} dates)",
                dateList, timeStampList.size(), dateList.size());
        return dateList;
    }

    private static List<String> getTimeStampList(String configValue, String duration, String numberOfInstance,
            String frequency) {

        logger.info("[ReadConfigUDF] Getting TimeStamp List!");
        logger.info("[ReadConfigUDF] Config Value: {}", configValue);
        logger.info("[ReadConfigUDF] Duration: {}", duration);
        logger.info("[ReadConfigUDF] Number Of Instance: {}", numberOfInstance);
        logger.info("[ReadConfigUDF] Frequency: {}", frequency);

        List<String> timeStampList = new ArrayList<>();

        try {
            LocalDateTime endTime;
            LocalDateTime startTime;

            if (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("Hour")
                    || frequency.equalsIgnoreCase("PERHOUR") || frequency.equalsIgnoreCase("5 Min")
                    || frequency.equalsIgnoreCase("FIVEMIN")) {

                logger.info("[ReadConfigUDF] Frequency is 15 Min or Hour, Parsing End Time!");
                DateTimeFormatter configFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
                endTime = LocalDateTime.parse(configValue, configFormatter);
            } else {

                logger.info("[ReadConfigUDF] Frequency is Day or Week, Parsing End Time!");
                DateTimeFormatter configFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                endTime = LocalDate.parse(configValue, configFormatter).atTime(23, 59);
            }

            logger.info("[ReadConfigUDF] Calculating Start Time, With Duration: {}, Number Of Instance: {}", duration,
                    numberOfInstance);
            startTime = calculateStartTime(endTime, duration, numberOfInstance);

            logger.info("[ReadConfigUDF] Start Time: {}, End Time: {}", startTime, endTime);

            if (frequency.equalsIgnoreCase("5 Min") || frequency.equalsIgnoreCase("FIVEMIN")) {
                logger.info("[ReadConfigUDF] Frequency is 5 Min, Generating 5 Min Timestamps!");
                timeStampList = generate5MinTimestamps(startTime, endTime);
            } else if (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY")) {
                logger.info("[ReadConfigUDF] Frequency is 15 Min, Generating Quarter Timestamps!");
                timeStampList = generateQuarterTimestamps(startTime, endTime);
            } else if (frequency.equalsIgnoreCase("Hour") || frequency.equalsIgnoreCase("PERHOUR")) {
                logger.info("[ReadConfigUDF] Frequency is Hour, Generating Hourly Timestamps!");
                timeStampList = generateHourlyTimestamps(startTime, endTime);
            } else if (frequency.equalsIgnoreCase("Day") || frequency.equalsIgnoreCase("PERDAY")) {
                logger.info("[ReadConfigUDF] Frequency is Day, Generating Daily Timestamps!");
                timeStampList = generateDailyTimestamps(startTime, endTime);
            } else if (frequency.equalsIgnoreCase("Week") || frequency.equalsIgnoreCase("PERWEEK")) {
                logger.info("[ReadConfigUDF] Frequency is Week, Generating Weekly Timestamps!");
                timeStampList = generateWeeklyTimestamps(startTime, endTime);
            } else if (frequency.equalsIgnoreCase("Month") || frequency.equalsIgnoreCase("PERMONTH")) {
                logger.info("[ReadConfigUDF] Frequency is Month, Generating Monthly Timestamps!");
                timeStampList = generateMonthlyTimestamps(startTime, endTime);
            } else {
                logger.info("[ReadConfigUDF] By Default, Generating Quarter Timestamps!");
                timeStampList = generateQuarterTimestamps(startTime, endTime);
            }
            logger.info("[ReadConfigUDF] Generated Timestamps Size: {}", timeStampList.size());
            logger.info("[ReadConfigUDF] Generated Timestamps: {}", timeStampList);

        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error generating timestamp list: {}", e.getMessage(), e);
        }

        return timeStampList;
    }

    private static List<String> generateMonthlyTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        List<String> timestamps = new ArrayList<>();
        LocalDateTime current = startTime;

        while (!current.isAfter(endTime)) {

            long timestampMillis = current.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(timestampMillis));
            current = current.plusMonths(1);
        }

        logger.info("[ReadConfigUDF] Generating {} monthly timestamps from {} to {}", timestamps.size(), startTime,
                endTime);

        return timestamps;
    }

    private static List<String> generateWeeklyTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        List<String> timestamps = new ArrayList<>();
        LocalDateTime current = startTime;

        while (!current.isAfter(endTime)) {

            long timestampMillis = current.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(timestampMillis));
            current = current.plusDays(7);
        }

        logger.info("[ReadConfigUDF] Generating {} weekly timestamps from {} to {}", timestamps.size(), startTime,
                endTime);

        return timestamps;
    }

    private static List<String> generate5MinTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        List<String> timestamps = new ArrayList<>();

        // For 5-minute frequency, we need to generate timestamps for each 5-minute
        // interval
        // but starting from the 5-minute mark after the start time
        LocalDateTime current = startTime.plusMinutes(5).withSecond(0).withNano(0);

        while (!current.isAfter(endTime)) {
            long timestampMillis = current.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(timestampMillis));
            current = current.plusMinutes(5);
        }

        logger.info("[ReadConfigUDF] Generating {} 5 Min timestamps from {} to {}", timestamps.size(), startTime,
                endTime);

        return timestamps;
    }

    private static LocalDateTime calculateStartTime(LocalDateTime endTime, String duration, String numberOfInstance) {
        int instances = Integer.parseInt(numberOfInstance);

        switch (duration.toUpperCase()) {
            case "DAILY":
                return endTime.minusHours(instances * 24);
            case "HOURLY":
                return endTime.minusHours(instances);
            case "WEEKLY":
                return endTime.minusWeeks(instances);
            case "MONTHLY":
                return endTime.minusMonths(instances);
            default:
                return endTime.minusDays(instances);
        }
    }

    private static List<String> generateQuarterTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        List<String> timestamps = new ArrayList<>();
        LocalDateTime current = startTime;

        // Round start time to the next 15-minute quarter (not the current one)
        int startMinute = current.getMinute();
        int quarterStartMinute = ((startMinute / 15) * 15) + 15;
        if (quarterStartMinute >= 60) {
            quarterStartMinute = 0;
            current = current.plusHours(1);
        }
        current = current.withMinute(quarterStartMinute).withSecond(0).withNano(0);

        // Calculate the number of 15-minute intervals needed
        long totalMinutes = java.time.Duration.between(current, endTime).toMinutes();
        int numberOfQuarters = (int) (totalMinutes / 15);

        logger.info("[ReadConfigUDF] Generating {} quarters from {} to {}", numberOfQuarters, current, endTime);

        for (int i = 0; i < numberOfQuarters; i++) {
            // Convert to milliseconds timestamp
            long timestampMillis = current.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(timestampMillis));

            // Move to next 15-minute quarter
            current = current.plusMinutes(15);
        }

        // Add the end time if it's not already included
        if (!current.isAfter(endTime)) {
            long endTimestampMillis = endTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(endTimestampMillis));
        }

        return timestamps;
    }

    private static List<String> generateHourlyTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        List<String> timestamps = new ArrayList<>();

        // For hourly frequency, we need to generate timestamps for each hour within the
        // range
        // but starting from the hour after the start time
        LocalDateTime current = startTime.plusHours(1).withMinute(0).withSecond(0).withNano(0);

        while (!current.isAfter(endTime)) {
            long timestampMillis = current.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(timestampMillis));
            current = current.plusHours(1);
        }

        logger.info("[ReadConfigUDF] Generating {} hourly timestamps from {} to {}", timestamps.size(), startTime,
                endTime);

        return timestamps;
    }

    private static List<String> generateDailyTimestamps(LocalDateTime startTime, LocalDateTime endTime) {
        List<String> timestamps = new ArrayList<>();

        // For daily frequency, we need to generate timestamps for each day within the
        // range
        // but starting from the day after the start time
        LocalDateTime current = startTime.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);

        while (!current.isAfter(endTime)) {
            long timestampMillis = current.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
            timestamps.add(String.valueOf(timestampMillis));
            current = current.plusDays(1);
        }

        logger.info("[ReadConfigUDF] Generating {} daily timestamps from {} to {}", timestamps.size(), startTime,
                endTime);

        return timestamps;
    }

    private static String getConfigValueFromBaseConfig(String configKey, String configTag, String configType,
            String applicationName, String frequency) {

        String configValue = "";

        String jdbcUrl = jobContextMap.get("SPARK_PLATFORM_JDBC_URL");
        String jdbcDriver = jobContextMap.get("SPARK_PLATFORM_JDBC_DRIVER");
        String jdbcUsername = jobContextMap.get("SPARK_PLATFORM_JDBC_USERNAME");
        String jdbcPassword = jobContextMap.get("SPARK_PLATFORM_JDBC_PASSWORD");

        logger.info("[ReadConfigUDF] Platform Credentials: {}, {}, {}, {}", jdbcUrl, jdbcDriver, jdbcUsername,
                jdbcPassword);

        String query = "SELECT DISTINCT CONFIG_VALUE FROM BASE_CONFIGURATION WHERE CONFIG_KEY = '" + configKey
                + "' AND CONFIG_TAG = '"
                + configTag + "' AND CONFIG_TYPE = '" + configType + "' AND APPLICATION_NAME = '" + applicationName
                + "'";

        logger.info("[ReadConfigUDF] Base Config Query: {}", query);
        ResultSet resultSet = null;
        try {
            resultSet = getResultSet(query, jdbcUrl, jdbcDriver, jdbcUsername, jdbcPassword);
            if (resultSet != null && resultSet.next()) {
                configValue = resultSet.getString("CONFIG_VALUE");
                logger.info("[ReadConfigUDF] Config Value Found: {}", configValue);
            } else {
                configValue = getDefaultConfigValueBasedOnFrequency(frequency);
                logger.info("[ReadConfigUDF] No Config Value Found, Using Default Config Value Based On Frequency: {}",
                        configValue);
            }
        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error in Getting Base Config Value, Message: {}, Error: {}", e.getMessage(),
                    e);
            configValue = getDefaultConfigValueBasedOnFrequency(frequency);
            logger.info("[ReadConfigUDF] Exception Fallback, Using Default Config Value Based On Frequency: {}",
                    configValue);
        }
        return configValue;
    }

    private static String getDefaultConfigValueBasedOnFrequency(String frequency) {
        String configValue = "";
        if (frequency.equalsIgnoreCase("15 Min")) {
            configValue = getRecentCompletedQuarter(); // YYYYMMDDHHMM
        } else if (frequency.equalsIgnoreCase("Hour")) {
            configValue = getRecentCompletedHour(); // YYYYMMDDHHMM
        } else if (frequency.equalsIgnoreCase("Day")) {
            configValue = getRecentCompletedDay(); // YYYYMMDD
        } else if (frequency.equalsIgnoreCase("Week")) {
            configValue = getRecentCompletedWeek(); // YYYYMMDD
        } else {
            configValue = getRecentCompletedQuarter(); // YYYYMMDDHHMM
        }
        return configValue;
    }

    private static String getRecentCompletedQuarter() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        int currentMinute = now.getMinute();
        int quarterMinute = (currentMinute / 15) * 15;
        if (quarterMinute == currentMinute) {
            now = now.minusMinutes(15);
            quarterMinute = (now.getMinute() / 15) * 15;
        } else {
            now = now.minusMinutes(15);
            quarterMinute = (now.getMinute() / 15) * 15;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        return now.withMinute(quarterMinute).format(formatter);
    }

    private static String getRecentCompletedHour() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        LocalDateTime completedHour = now.minusHours(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHH00");
        return completedHour.format(formatter);
    }

    private static String getRecentCompletedDay() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        LocalDateTime completedDay = now.minusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return completedDay.format(formatter);
    }

    private static String getRecentCompletedWeek() {
        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
        LocalDateTime completedWeek = now.minusWeeks(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        return completedWeek.format(formatter);
    }

    private static ResultSet getResultSet(String query, String jdbcUrl, String jdbcDriver, String jdbcUsername,
            String jdbcPassword) {
        ResultSet resultSet = null;
        try {
            Class.forName(jdbcDriver);
            Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error in Getting Base Config ResultSet, Message: {}, Error: {}",
                    e.getMessage(), e);
        }
        return resultSet;
    }

    private static String convertToExpectedTimeStampFormat(String date) {
        logger.info("[ReadConfigUDF] Converting Date to Expected TimeStamp Format: {}", date);
        try {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("MMM d,yyyy H:mm");
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxx");
            LocalDateTime localDateTime = LocalDateTime.parse(date, inputFormatter);
            String formattedDate = localDateTime.atOffset(ZoneOffset.UTC).format(outputFormatter);
            logger.info("[ReadConfigUDF] Formatted Date: {}", formattedDate);
            return formattedDate;
        } catch (Exception e) {
            // logger.error("[ReadConfigUDF] Error in Converting Date to Expected TimeStamp
            // Format: {}", e.getMessage(),
            // e);
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxx");
            String formattedDate = now.atOffset(ZoneOffset.UTC).format(outputFormatter);
            logger.info("[ReadConfigUDF] Current Formatted Date: {}", formattedDate);
            return formattedDate;
        }
    }

    private static Map<String, String> getNodeAndAggregationDetails(Map<String, String> reportWidgetDetails) {

        Map<String, String> nodeAndAggregationDetails = new LinkedHashMap<>();

        try {
            String fixedJson = "";
            if (reportWidgetDetails != null && reportWidgetDetails.get("configuration") != null) {
                fixedJson = reportWidgetDetails.get("configuration").replace("'", "\"");
            }
            JSONObject jsonObject = null;
            try {
                jsonObject = new JSONObject(fixedJson);
            } catch (Exception e) {
                logger.error("[ReadConfigUDF] Invalid JSON configuration: {}", fixedJson, e);
                return nodeAndAggregationDetails;
            }

            JSONArray geoL1JSONArray = jsonObject.has("geography_l1") ? jsonObject.optJSONArray("geography_l1")
                    : new JSONArray();
            JSONArray geoL2JSONArray = jsonObject.has("geography_l2") ? jsonObject.optJSONArray("geography_l2")
                    : new JSONArray();
            JSONArray geoL3JSONArray = jsonObject.has("geography_l3") ? jsonObject.optJSONArray("geography_l3")
                    : new JSONArray();
            JSONArray geoL4JSONArray = jsonObject.has("geography_l4") ? jsonObject.optJSONArray("geography_l4")
                    : new JSONArray();
            JSONArray nodeArray = jsonObject.has("node") ? jsonObject.optJSONArray("node") : new JSONArray();
            JSONArray moArray = jsonObject.has("mo") ? jsonObject.optJSONArray("mo") : new JSONArray();

            if (containsNumericIds(geoL1JSONArray)) {
                geoL1JSONArray = getGeographyNamesByIds(geoL1JSONArray, "L1");
            }
            if (containsNumericIds(geoL2JSONArray)) {
                geoL2JSONArray = getGeographyNamesByIds(geoL2JSONArray, "L2");
            }
            if (containsNumericIds(geoL3JSONArray)) {
                geoL3JSONArray = getGeographyNamesByIds(geoL3JSONArray, "L3");
            }
            if (containsNumericIds(geoL4JSONArray)) {
                geoL4JSONArray = getGeographyNamesByIds(geoL4JSONArray, "L4");
            }

            List<String> geoL1List = geoL1JSONArray != null ? getStringListFromArray(geoL1JSONArray)
                    : new ArrayList<>();
            List<String> geoL2List = geoL2JSONArray != null ? getStringListFromArray(geoL2JSONArray)
                    : new ArrayList<>();
            List<String> geoL3List = geoL3JSONArray != null ? getStringListFromArray(geoL3JSONArray)
                    : new ArrayList<>();
            List<String> geoL4List = geoL4JSONArray != null ? getStringListFromArray(geoL4JSONArray)
                    : new ArrayList<>();
            List<String> nodeList = nodeArray != null ? getStringListFromArray(nodeArray) : new ArrayList<>();
            List<String> moList = moArray != null ? getStringListFromArray(moArray) : new ArrayList<>();

            String geoL1 = (!geoL1List.isEmpty() && geoL1List.get(0) != null && !geoL1List.get(0).isEmpty())
                    ? geoL1List.get(0).toUpperCase()
                    : "";
            String geoL2 = (!geoL2List.isEmpty() && geoL2List.get(0) != null && !geoL2List.get(0).isEmpty())
                    ? geoL2List.get(0).toUpperCase()
                    : "";
            String geoL3 = (!geoL3List.isEmpty() && geoL3List.get(0) != null && !geoL3List.get(0).isEmpty())
                    ? geoL3List.get(0).toUpperCase()
                    : "";
            String geoL4 = (!geoL4List.isEmpty() && geoL4List.get(0) != null && !geoL4List.get(0).isEmpty())
                    ? geoL4List.get(0).toUpperCase()
                    : "";
            String node = (!nodeList.isEmpty() && nodeList.get(0) != null && !nodeList.get(0).isEmpty())
                    ? nodeList.get(0).toUpperCase()
                    : "";
            String mo = (!moList.isEmpty() && moList.get(0) != null && !moList.get(0).isEmpty())
                    ? moList.get(0).toUpperCase()
                    : "";

            String netype = "";
            try {
                netype = jsonObject.has("netype") ? jsonObject.optString("netype", "") : "";
                netype = netype != null && !netype.isEmpty() ? netype.toUpperCase() : "";
            } catch (Exception e) {
                netype = "";
            }

            logger.info(
                    "[ReadConfigUDF] Provided Node & Aggragtaion Details: geoL1={}, geoL2={}, geoL3={}, geoL4={}, node={}",
                    geoL1, geoL2, geoL3, geoL4, node);

            nodeAndAggregationDetails.put("geoL1", geoL1);
            nodeAndAggregationDetails.put("geoL2", geoL2);
            nodeAndAggregationDetails.put("geoL3", geoL3);
            nodeAndAggregationDetails.put("geoL4", geoL4);
            nodeAndAggregationDetails.put("node", node);
            nodeAndAggregationDetails.put("mo", mo);
            nodeAndAggregationDetails.put("netype", netype);
            nodeAndAggregationDetails.put("geoL1List", geoL1List.toString());
            nodeAndAggregationDetails.put("geoL2List", geoL2List.toString());
            nodeAndAggregationDetails.put("geoL3List", geoL3List.toString());
            nodeAndAggregationDetails.put("geoL4List", geoL4List.toString());
            nodeAndAggregationDetails.put("nodeList", nodeList.toString());
            nodeAndAggregationDetails.put("moList", moList.toString());

            String isGeoL1MultiSelect = (!geoL1.contains("CLUBBED") && !geoL1.contains("INDIVIDUAL")) ? "true"
                    : "false";
            String isGeoL2MultiSelect = (!geoL2.contains("CLUBBED") && !geoL2.contains("INDIVIDUAL")) ? "true"
                    : "false";
            String isGeoL3MultiSelect = (!geoL3.contains("CLUBBED") && !geoL3.contains("INDIVIDUAL")) ? "true"
                    : "false";
            String isGeoL4MultiSelect = (!geoL4.contains("CLUBBED") && !geoL4.contains("INDIVIDUAL")) ? "true"
                    : "false";
            String isNodeMultiSelect = (!node.contains("CLUBBED") && !node.contains("INDIVIDUAL")) ? "true" : "false";

            nodeAndAggregationDetails.put("isGeoL1MultiSelect", isGeoL1MultiSelect);
            nodeAndAggregationDetails.put("isGeoL2MultiSelect", isGeoL2MultiSelect);
            nodeAndAggregationDetails.put("isGeoL3MultiSelect", isGeoL3MultiSelect);
            nodeAndAggregationDetails.put("isGeoL4MultiSelect", isGeoL4MultiSelect);
            nodeAndAggregationDetails.put("isNodeMultiSelect", isNodeMultiSelect);

            JSONArray nodeNameArray = jsonObject.has("cells") ? jsonObject.optJSONArray("cells") : new JSONArray();
            List<String> nodeNameList = nodeNameArray != null ? getStringListFromArray(nodeNameArray)
                    : new ArrayList<>();
            String isNodeNameListEmpty = nodeNameList.isEmpty() ? "true" : "false";
            nodeAndAggregationDetails.put("isNodeNameListEmpty", isNodeNameListEmpty);

            Map<String, String> nodeInfoMap = new LinkedHashMap<>();
            if ("false".equals(isNodeNameListEmpty)) {
                try {
                    nodeInfoMap = getNodeInfoMap(nodeNameList, reportWidgetDetails);
                    String nodeInfoMapJson = new ObjectMapper().writeValueAsString(nodeInfoMap);
                    nodeAndAggregationDetails.put("nodeInfoMap", nodeInfoMapJson);
                } catch (Exception e) {
                    logger.error("[ReadConfigUDF] Error while building nodeInfoMap: {}", e.getMessage(), e);
                    nodeAndAggregationDetails.put("nodeInfoMap", "{}");
                }
            }

        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error in Getting Node and Aggregation Details, Message: {}, Error: {}",
                    e.getMessage(), e);
        }

        return nodeAndAggregationDetails;

    }

    private static Map<String, String> getNodeInfoMap(List<String> nodeNameList,
            Map<String, String> reportWidgetDetails) {
        Map<String, String> nodeInfoMap = new LinkedHashMap<>();
        ResultSet resultSet = getNodeInfoResultSet(nodeNameList, reportWidgetDetails);
        try {
            while (resultSet.next()) {
                String neId = resultSet.getString("NE_ID");
                String neName = resultSet.getString("NE_NAME");
                nodeInfoMap.put(neId, neName);
            }
        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error in Getting Node Info Map, Message: {}, Error: {}", e.getMessage(), e);
        }
        return nodeInfoMap;
    }

    private static ResultSet getNodeInfoResultSet(List<String> nodeNameList, Map<String, String> reportWidgetDetails) {

        String domain = reportWidgetDetails.get("domain");
        String vendor = reportWidgetDetails.get("vendor");
        String technology = reportWidgetDetails.get("technology");

        domain = domain != null && !domain.isEmpty() ? domain.toUpperCase() : "N/A";
        vendor = vendor != null && !vendor.isEmpty() ? vendor.toUpperCase() : "N/A";
        technology = technology != null && !technology.isEmpty() ? technology.toUpperCase() : "N/A";

        List<String> quotedNodeNames = nodeNameList.stream()
                .map(name -> "'" + name.trim().toUpperCase() + "'")
                .collect(Collectors.toList());

        String query = "SELECT NE_ID, NE_NAME FROM NETWORK_ELEMENT WHERE UPPER(DOMAIN) = '" + domain
                + "' AND UPPER(VENDOR) = '"
                + vendor
                + "' AND UPPER(TECHNOLOGY) = '" + technology + "' AND UPPER(NE_NAME) IN ("
                + String.join(",", quotedNodeNames) + ")";

        logger.info("[ReadConfigUDF] Node Info Query: {}", query);

        ResultSet resultSet = null;

        try {
            resultSet = getResultSet(query);
        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error in Getting Node Info ResultSet, Message: {}, Error: {}", e.getMessage(),
                    e);
        }
        return resultSet;
    }

    private static ResultSet getResultSet(String query) {

        ResultSet resultSet = null;
        try {
            String SPARK_PM_JDBC_DRIVER = jobContextMap.get("SPARK_PM_JDBC_DRIVER");
            String SPARK_PM_JDBC_URL = jobContextMap.get("SPARK_PM_JDBC_URL");
            String SPARK_PM_JDBC_USERNAME = jobContextMap.get("SPARK_PM_JDBC_USERNAME");
            String SPARK_PM_JDBC_PASSWORD = jobContextMap.get("SPARK_PM_JDBC_PASSWORD");

            logger.info("[ReadConfigUDF] PM JDBC Driver: {}, URL: {}, Username: {}, Password: {}", SPARK_PM_JDBC_DRIVER,
                    SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

            SPARK_PM_JDBC_DRIVER = SPARK_PM_JDBC_DRIVER != null && !SPARK_PM_JDBC_DRIVER.isEmpty()
                    ? SPARK_PM_JDBC_DRIVER
                    : FALLBACK_SPARK_PM_JDBC_DRIVER;
            SPARK_PM_JDBC_URL = SPARK_PM_JDBC_URL != null && !SPARK_PM_JDBC_URL.isEmpty() ? SPARK_PM_JDBC_URL
                    : FALLBACK_SPARK_PM_JDBC_URL;
            SPARK_PM_JDBC_USERNAME = SPARK_PM_JDBC_USERNAME != null && !SPARK_PM_JDBC_USERNAME.isEmpty()
                    ? SPARK_PM_JDBC_USERNAME
                    : FALLBACK_SPARK_PM_JDBC_USERNAME;
            SPARK_PM_JDBC_PASSWORD = SPARK_PM_JDBC_PASSWORD != null && !SPARK_PM_JDBC_PASSWORD.isEmpty()
                    ? SPARK_PM_JDBC_PASSWORD
                    : FALLBACK_SPARK_PM_JDBC_PASSWORD;

            Class.forName(SPARK_PM_JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(
                    SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(query);

            logger.info("[ReadConfigUDF] Query Executed Successfully!");

        } catch (Exception e) {
            logger.error("[ReadConfigUDF] Error in Getting ResultSet, Message: {}, Error: {}", e.getMessage(), e);
        }
        return resultSet;
    }

    public static List<String> getStringListFromArray(JSONArray array) {

        if (array == null || array.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> list = new ArrayList<>();

        for (int i = 0; i < array.length(); i++) {

            try {
                list.add(array.getString(i));
            } catch (Exception e) {
                logger.error("Error Parsing String from JSONArray At index {} | Message: {} | Error: {}", i,
                        e.getMessage(), e);
            }

        }

        return list;
    }

    private static boolean containsNumericIds(JSONArray array) {
        if (array == null || array.isEmpty()) {
            return false;
        }

        for (int i = 0; i < array.length(); i++) {
            try {
                Object value = array.get(i);
                if (value instanceof Number) {
                    return true;
                } else if (value instanceof String) {
                    String strValue = (String) value;
                    if (!strValue.trim().isEmpty()) {
                        try {
                            Long.parseLong(strValue.trim());
                            return true;
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("[ReadConfigUDF] Error Checking Numeric ID At Index {} | Message: {} | Error: {}", i,
                        e.getMessage(), e);
            }
        }
        return false;
    }

    private static JSONArray getGeographyNamesByIds(JSONArray ids, String level) {
        JSONArray namesArray = new JSONArray();

        if (ids == null || ids.isEmpty()) {
            return namesArray;
        }

        try {
            String tableName = switch (level.toUpperCase()) {
                case "L1" -> "PRIMARY_GEO_L1";
                case "L2" -> "PRIMARY_GEO_L2";
                case "L3" -> "PRIMARY_GEO_L3";
                case "L4" -> "PRIMARY_GEO_L4";
                default -> "PRIMARY_GEO_L1";
            };

            List<String> numericIds = new ArrayList<>();
            for (int i = 0; i < ids.length(); i++) {
                try {
                    Object value = ids.get(i);
                    if (value instanceof Number) {
                        numericIds.add(String.valueOf(((Number) value).longValue()));
                    } else if (value instanceof String) {
                        String strValue = (String) value;
                        if (!strValue.trim().isEmpty()) {
                            try {
                                Long.parseLong(strValue.trim());
                                numericIds.add(strValue.trim());
                            } catch (NumberFormatException e) {
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error Parsing Numeric ID at index {} | Message: {} | Error: {}", i, e.getMessage(),
                            e);
                }
            }

            if (numericIds.isEmpty()) {
                logger.info("[ReadConfigUDF] No Numeric IDs Found for Geography Level: {}", level);
                return namesArray;
            }

            String quotedIds = numericIds.stream()
                    .map(id -> "'" + id + "'")
                    .collect(Collectors.joining(","));

            String query = "SELECT ID, UPPER(GEO_NAME) AS GEO_NAME FROM " + tableName + " WHERE ID IN (" + quotedIds
                    + ")";
            logger.info("[ReadConfigUDF] Geography Query For Level {}: {}", level, query);

            ResultSet resultSet = getResultSet(query);
            if (resultSet != null) {
                Map<String, String> idToNameMap = new HashMap<>();
                while (resultSet.next()) {
                    String id = resultSet.getString("ID");
                    String name = resultSet.getString("GEO_NAME");
                    idToNameMap.put(id, name);
                }

                for (String id : numericIds) {
                    String name = idToNameMap.get(id);
                    if (name != null) {
                        namesArray.put(name);
                    } else {
                        logger.info("[ReadConfigUDF] No Name Found for Geography ID: {} In Level: {}", id, level);
                        namesArray.put(id);
                    }
                }
            }

        } catch (Exception e) {
            logger.error(
                    "[ReadConfigUDF] Error in Getting Geography Names By IDs For Level {} | Message: {} | Error: {}",
                    level, e.getMessage(), e);
        }

        logger.info("[ReadConfigUDF] Converted {} IDs to {} Names for Geography Level: {}",
                ids.length(), namesArray.length(), level);
        return namesArray;
    }

    public static Map<String, Map<String, String>> getRagConfiguration(String configuration) {

        String fixedJson = configuration.replace("'", "\"");
        Map<String, Map<String, String>> ragConfigurationMap = new HashMap<>();
        JSONObject jsonObject = new JSONObject(fixedJson);
        logger.info("[ReadConfigUDF] Configuration JSON Object: {}", jsonObject.toString());
        JSONArray kpiArray = jsonObject.getJSONArray("kpi");

        for (int i = 0; i < kpiArray.length(); i++) {
            JSONObject kpiObject = kpiArray.getJSONObject(i);
            String kpiCode = kpiObject.getString("kpicode");

            JSONArray colorInputRangeConfigArray = kpiObject.getJSONArray("colorInputRangeConfig");
            if (!ragConfigurationMap.containsKey(kpiCode)) {
                ragConfigurationMap.put(kpiCode, new HashMap<>());
            }

            Map<String, String> conditionMap = ragConfigurationMap.get(kpiCode);

            for (int j = 0; j < colorInputRangeConfigArray.length(); j++) {
                JSONObject config = colorInputRangeConfigArray.getJSONObject(j);
                String selectedCondition = config.getString("selectedCondition");

                String condition;
                String selectedHexColor;

                if (selectedCondition.equals("inRange")) {
                    JSONArray inputRangeArray = config.getJSONArray("inputRange");
                    for (int k = 0; k < inputRangeArray.length(); k++) {
                        JSONObject range = inputRangeArray.getJSONObject(k);
                        String inputFrom = range.getString("inputFrom");
                        String inputTo = range.getString("inputTo");
                        selectedHexColor = range.getString("selectedHexColor");

                        condition = "(KPI#" + kpiCode + " >= " + inputFrom + " && KPI#" + kpiCode + " <= " + inputTo
                                + ")";
                        conditionMap.put(condition, selectedHexColor);
                    }
                } else {
                    String inputFilter;
                    try {
                        Object inputFilterObj = config.get("inputFilter");
                        if (inputFilterObj == null) {
                            logger.info("[ReadConfigUDF] inputFilter is null, Skipping condition");
                            continue;
                        }
                        inputFilter = inputFilterObj.toString();
                    } catch (Exception e) {
                        logger.error("[ReadConfigUDF] Error Getting inputFilter: {}", e.getMessage(), e);
                        continue;
                    }

                    selectedHexColor = config.getString("selectedHexColor");

                    String operator = switch (selectedCondition) {
                        case "equals" -> " = ";
                        case "notEquals" -> " != ";
                        case "gt" -> " > ";
                        case "gte" -> " >= ";
                        case "lt" -> " < ";
                        case "lte" -> " <= ";
                        default -> null;
                    };

                    if (operator != null) {
                        condition = "(KPI#" + kpiCode + operator + inputFilter + ")";
                        conditionMap.put(condition, selectedHexColor);
                    }
                }
            }
        }

        logger.info("[ReadConfigUDF] Generated RAG Configuration Map: {}", ragConfigurationMap);
        return ragConfigurationMap;
    }

}
