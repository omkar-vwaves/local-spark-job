package com.enttribe.custom.processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionalDailyScheduled implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(OptionalDailyScheduled.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final String FIXED_SCHEDULE_TIME = "15:30";
    private static final int DEFAULT_NUMBER_OF_INSTANCES = 1;
    
    private volatile boolean isRunning = false;
    
    private volatile java.sql.Connection storedConnection = null;
    private volatile ProcessSession storedSession = null;
    private volatile FlowFile storedFlowFile = null;

    @Override
    public void run() {
        logger.info("OptionalDailyScheduled Executed Started!");
        startScheduledProcessor();
    }

    public void startScheduledProcessor() {
        if (isRunning) {
            logger.info("OptionalDailyScheduled Processor is Already Running!");
            return;
        }

        isRunning = true;
        logger.info("Starting OptionalDailyScheduled Processor in Continuous Mode (Reload RulesEvery 1 Minute)...");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                logger.info("Scheduled Processing Triggered - Checking for Ready Reports...");
                
                if (storedConnection != null && !storedConnection.isClosed()) {
                    logger.info("Processing Scheduled Reports with Stored Connection...");
                    processScheduledReports(storedFlowFile, storedSession, storedConnection);
                } else {
                    logger.info("No Valid Connection Available for Scheduled Processing");
                }
            } catch (Exception e) {
                logger.info("Error in Scheduled Report Processing: ", e);
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    public void stopScheduledProcessor() {
        isRunning = false;
        scheduler.shutdown();
        logger.info("OptionalDailyScheduled Processor Stopped!");
    }

    private void processScheduledReports(FlowFile inputFlowFile, ProcessSession session,
            java.sql.Connection connection) {
        try {
            List<Map<String, Object>> readyReports = getReadyScheduledReports(connection);
            logger.info("Ready Scheduled Reports: {}", readyReports);

            if (readyReports.isEmpty()) {
                logger.info("No Ready Scheduled Reports Found");
                if (inputFlowFile != null && session != null) {
                    session.transfer(inputFlowFile, RELATIONSHIP_SUCCESS);
                }
            } else {
                for (Map<String, Object> report : readyReports) {
                    processReport(report, inputFlowFile, session, connection);
                }
            }

            logger.info("=====================");
        } catch (Exception e) {
            logger.info("Error Processing Scheduled Reports: ", e);
            logger.info("=====================");
            if (inputFlowFile != null && session != null) {
                try {
                    session.transfer(inputFlowFile, RELATIONSHIP_FAILURE);
                } catch (Exception transferError) {
                    logger.info("Error Transferring FlowFile to Failure: ", transferError);
                }
            }
        }
    }

    private List<Map<String, Object>> getReadyScheduledReports(Connection connection) throws SQLException {
        List<Map<String, Object>> readyReports = new ArrayList<>();

        String query = """
                SELECT
                    rw.REPORT_WIDGET_ID_PK,
                    COALESCE(GROUP_CONCAT(jt.email ORDER BY jt.email SEPARATOR ', '), '') AS EMAIL,
                    rw.CONFIGURATION AS CONFIGURATION_RAW,
                    rw.DURATION,
                    rw.REPORT_TYPE,
                    rw.GENERATED_TYPE,
                    rw.REPORT_NAME,
                    rw.VENDOR,
                    rw.USER_ID_FK,
                    rw.GEOGRAPHY,
                    rw.REPORT_LEVEL,
                    rw.TECHNOLOGY,
                    rw.DOMAIN,
                    rw.REPORT_MEASURE,
                    rw.FREQUENCY,
                    rw.NODE,
                    rw.SERVER_INSTANCE,
                    rw.FILE_MANAGEMENT_FK,
                    rw.CONFIGURATION
                FROM REPORT_WIDGET rw
                LEFT JOIN (
                    SELECT j.email, rpk.REPORT_WIDGET_ID_PK
                    FROM REPORT_WIDGET rpk
                    JOIN JSON_TABLE(
                        rpk.FLOW_CONFIGURATION,
                        '$.email[*]' COLUMNS (email VARCHAR(255) PATH '$')
                    ) AS j
                ) jt
                ON rw.REPORT_WIDGET_ID_PK = jt.REPORT_WIDGET_ID_PK
                WHERE rw.DURATION = 'DAILY'
                  AND rw.GENERATED_TYPE = 'SCHEDULED'
                  AND rw.DELETED = 0
                  AND (rw.STATUS = 'NOT_CREATED' OR rw.STATUS = 'CREATED')
                  AND rw.CONFIGURATION LIKE '%scheduleAt%'
                  AND (rw.CONFIGURATION LIKE '%scheduleAt%:[]%' OR rw.CONFIGURATION LIKE '%scheduleAt%: [%]%')
                GROUP BY rw.REPORT_WIDGET_ID_PK, CONFIGURATION_RAW
                """;

        logger.info("Ready Scheduled Reports Query: {}", query);

        try (PreparedStatement stmt = connection.prepareStatement(query);
                ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int reportWidgetId = rs.getInt("REPORT_WIDGET_ID_PK");
                String configurationRaw = rs.getString("CONFIGURATION_RAW");

                String scheduledAt = extractScheduleTime(configurationRaw);
                String numberOfInstanceStr = extractNumberOfInstance(configurationRaw);

                logger.info("REPORT_WIDGET_ID_PK: {}, Extracted Scheduled At: {}, Number of Instance: {}",
                        reportWidgetId, scheduledAt, numberOfInstanceStr);

                if (scheduledAt == null || scheduledAt.trim().isEmpty()) {
                    logger.info("REPORT_WIDGET_ID_PK: {}, Processing Report with Empty ScheduleAt", reportWidgetId);
                    
                    logger.info("REPORT_WIDGET_ID_PK: {}, Using Fixed Schedule Time: {}, Default Number of Instances: {}", 
                            reportWidgetId, FIXED_SCHEDULE_TIME, DEFAULT_NUMBER_OF_INSTANCES);

                    boolean isTimeToExecute = isTimeToExecute(FIXED_SCHEDULE_TIME, reportWidgetId);
                    logger.info("REPORT_WIDGET_ID_PK: {}, Fixed Schedule Time Check: {}", reportWidgetId, isTimeToExecute);

                    if (isTimeToExecute) {
                        Map<String, Object> report = new LinkedHashMap<>();
                        report.put("REPORT_WIDGET_ID_PK", rs.getInt("REPORT_WIDGET_ID_PK"));
                        report.put("REPORT_NAME", rs.getString("REPORT_NAME"));
                        report.put("DOMAIN", rs.getString("DOMAIN"));
                        report.put("VENDOR", rs.getString("VENDOR"));
                        report.put("TECHNOLOGY", rs.getString("TECHNOLOGY"));
                        report.put("GENERATED_TYPE", rs.getString("GENERATED_TYPE"));
                        report.put("USER_ID_FK", rs.getInt("USER_ID_FK"));
                        report.put("EMAIL", rs.getString("EMAIL"));
                        report.put("SCHEDULED_AT", FIXED_SCHEDULE_TIME);
                        report.put("NUMBER_OF_INSTANCE", DEFAULT_NUMBER_OF_INSTANCES);
                        report.put("DURATION", rs.getString("DURATION"));
                        report.put("FREQUENCY", rs.getString("FREQUENCY"));
                        report.put("REPORT_TYPE", rs.getString("REPORT_TYPE"));
                        report.put("GEOGRAPHY", rs.getString("GEOGRAPHY"));
                        report.put("REPORT_LEVEL", rs.getString("REPORT_LEVEL"));
                        report.put("NODE", rs.getString("NODE"));
                        report.put("FILE_MANAGEMENT_FK", rs.getInt("FILE_MANAGEMENT_FK"));
                        readyReports.add(report);
                    } else {
                        LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
                        LocalDateTime scheduledTime = LocalDateTime.now(ZoneId.of("UTC"))
                                .withHour(0)
                                .withMinute(0)
                                .withSecond(0)
                                .withNano(0);

                        long minutesUntilExecution = java.time.Duration.between(now, scheduledTime).toMinutes();

                        if (minutesUntilExecution > 0) {
                            long hours = minutesUntilExecution / 60;
                            long minutes = minutesUntilExecution % 60;
                            String timeRemaining = String.format("%02d:%02d", hours, minutes);

                            logger.info(
                                    "REPORT_WIDGET_ID_PK: {}, Fixed Schedule 00:00 UTC, Current Time: {}:{} UTC, Time Remaining: {} ({}:{} Hours)",
                                    reportWidgetId,
                                    now.getHour(), now.getMinute(),
                                    timeRemaining, hours, minutes);
                        } else if (minutesUntilExecution < 0) {
                            long hours = Math.abs(minutesUntilExecution) / 60;
                            long minutes = Math.abs(minutesUntilExecution) % 60;
                            String timePast = String.format("%02d:%02d", hours, minutes);

                            logger.info(
                                    "REPORT_WIDGET_ID_PK: {}, Fixed Schedule 00:00 UTC, Current Time: {}:{} UTC, Already Past Scheduled Time by {} ({}:{} Hours)",
                                    reportWidgetId,
                                    now.getHour(), now.getMinute(),
                                    timePast, hours, minutes);
                        } else {
                            logger.info(
                                    "REPORT_WIDGET_ID_PK: {}, Fixed Schedule 00:00 UTC, Current Time: {}:{} UTC, Exactly at Scheduled Time",
                                    reportWidgetId,
                                    now.getHour(), now.getMinute());
                        }
                    }
                } else {
                    logger.info(
                            "REPORT_WIDGET_ID_PK: {}, Skipping Report with Valid ScheduleAt: '{}'",
                            reportWidgetId, scheduledAt);
                }
            }
        }

        return readyReports;
    }

    private String extractScheduleTime(String configuration) {
        try {
            if (configuration.contains("'scheduleAt':")) {
                int startIndex = configuration.indexOf("'scheduleAt':") + "'scheduleAt':".length();
                int endIndex = configuration.indexOf("]", startIndex);
                if (endIndex > startIndex) {
                    String schedulePart = configuration.substring(startIndex, endIndex + 1);
                    int timeStart = schedulePart.indexOf("'") + 1;
                    int timeEnd = schedulePart.lastIndexOf("'");
                    if (timeStart > 0 && timeEnd > timeStart) {
                        return schedulePart.substring(timeStart, timeEnd);
                    }
                }
            }
            return null;
        } catch (Exception e) {
            logger.info("Error Extracting Schedule Time From Configuration: {}", configuration, e);
            return null;
        }
    }

    private String extractNumberOfInstance(String configuration) {
        try {
            if (configuration.contains("'numberOfInstance':")) {
                int startIndex = configuration.indexOf("'numberOfInstance':") + "'numberOfInstance':".length();
                int endIndex = configuration.indexOf(",", startIndex);
                if (endIndex == -1) {
                    endIndex = configuration.indexOf("}", startIndex);
                }
                if (endIndex > startIndex) {
                    String instancePart = configuration.substring(startIndex, endIndex);
                    return instancePart.replaceAll("['\"\\s]", "");
                }
            }
            return null;
        } catch (Exception e) {
            logger.info("Error Extracting Number Of Instances From Configuration: {}", configuration, e);
            return null;
        }
    }

    private boolean isTimeToExecute(String scheduledAt, int reportWidgetId) {
        try {
            String[] timeParts = scheduledAt.split(":");
            int scheduledHour = Integer.parseInt(timeParts[0]);
            int scheduledMinute = Integer.parseInt(timeParts[1]);

            LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
            int currentHour = now.getHour();
            int currentMinute = now.getMinute();

            logger.info("REPORT_WIDGET_ID_PK: {}, Current UTC Time: {}:{}, Scheduled Time: {}:{}, Scheduled At: {}",
                    reportWidgetId, currentHour, currentMinute, scheduledHour, scheduledMinute, scheduledAt);

            if (currentHour == scheduledHour && currentMinute == scheduledMinute) {
                logger.info("REPORT_WIDGET_ID_PK: {}, Time to execute: TRUE", reportWidgetId);
                return true;
            }

            logger.info("REPORT_WIDGET_ID_PK: {}, Time to execute: FALSE", reportWidgetId);
            return false;
        } catch (Exception e) {
            logger.info("REPORT_WIDGET_ID_PK: {}, Error parsing scheduled time: " + scheduledAt, reportWidgetId, e);
            return false;
        }
    }

    private void processReport(Map<String, Object> report, FlowFile inputFlowFile, ProcessSession session,
            Connection connection) {
        try {
            int reportWidgetId = (Integer) report.get("REPORT_WIDGET_ID_PK");
            int numberOfInstances = (Integer) report.get("NUMBER_OF_INSTANCE");
            String scheduledAt = (String) report.get("SCHEDULED_AT");

            if (isAlreadyProcessedToday(reportWidgetId, scheduledAt, connection)) {
                logger.info("Report Already Processed Today: REPORT_WIDGET_ID_PK={}", reportWidgetId);
                if (inputFlowFile != null && session != null) {
                    session.transfer(inputFlowFile, RELATIONSHIP_SUCCESS);
                }
                return;
            }

            String configKey = "TRANSPORT_COMMON_JUNIPER_NA_DAILY_KPICOMPUTATION";
            String configValue = getLatestHourlyConfigValue(configKey);
            String frequency = (String) report.get("FREQUENCY");
            String duration = (String) report.get("DURATION");
            List<Long> timestamps = generateTimestamps(configValue, numberOfInstances, frequency, duration,
                    reportWidgetId);
            logger.info("REPORT_WIDGET_ID_PK: {}, Timestamps: {}", reportWidgetId, timestamps);

            int generatedReportId = insertGeneratedReport(report, connection);
            logger.info("REPORT_WIDGET_ID_PK: {}, GENERATED_REPORT_ID_PK: {}", reportWidgetId, generatedReportId);

            if (!timestamps.isEmpty()) {
                logger.info("REPORT_WIDGET_ID_PK: {}, Creating Single Flow File with All Timestamps: {}",
                        reportWidgetId,
                        timestamps);
                createFlowFile(report, timestamps, generatedReportId, inputFlowFile, session);
            } else {
                logger.info("REPORT_WIDGET_ID_PK: {}, No Timestamps Generated, Cannot Create Flow File",
                        reportWidgetId);
                // Transfer input FlowFile to success even if no timestamps generated
                if (inputFlowFile != null && session != null) {
                    session.transfer(inputFlowFile, RELATIONSHIP_SUCCESS);
                }
            }

            updateReportWidgetStatus(reportWidgetId, "IN_PROGRESS", connection);

            logger.info("Processed Scheduled Report: REPORT_WIDGET_ID_PK={}, Instances={}",
                    reportWidgetId, numberOfInstances);

        } catch (Exception e) {
            logger.info("Error Processing Report: ", e);
            // Transfer input FlowFile to failure on info
            if (inputFlowFile != null && session != null) {
                try {
                    session.transfer(inputFlowFile, RELATIONSHIP_FAILURE);
                } catch (Exception transferError) {
                    logger.info("Error transferring FlowFile to failure: ", transferError);
                }
            }
        }
    }

    private String getLatestHourlyConfigValue(String configKey) {
        Connection configConnection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        boolean shouldCloseConnection = false;

        try {
            if (configConnection == null || configConnection.isClosed()) {
                String SPARK_PLATFORM_JDBC_DRIVER = "org.mariadb.jdbc.Driver";
                String SPARK_PLATFORM_JDBC_URL = "jdbc:mysql://localhost:3306/PLATFORM?autoReconnect=true&allowPublicKeyRetrieval=true&useSSL=false";
                String SPARK_PLATFORM_JDBC_USERNAME = "root";
                String SPARK_PLATFORM_JDBC_PASSWORD = "root";

                logger.info("Creating New PLATFORM Connection for Config Lookup: Driver: {}, URL: {}, Username: {}",
                        SPARK_PLATFORM_JDBC_DRIVER, SPARK_PLATFORM_JDBC_URL, SPARK_PLATFORM_JDBC_USERNAME);

                Class.forName(SPARK_PLATFORM_JDBC_DRIVER);
                configConnection = DriverManager.getConnection(SPARK_PLATFORM_JDBC_URL, SPARK_PLATFORM_JDBC_USERNAME,
                        SPARK_PLATFORM_JDBC_PASSWORD);
                shouldCloseConnection = true;
            }

            String query = "SELECT CONFIG_VALUE FROM BASE_CONFIGURATION WHERE CONFIG_KEY = ?";
            stmt = configConnection.prepareStatement(query);
            stmt.setString(1, configKey);
            rs = stmt.executeQuery();

            if (rs.next()) {
                String configValue = rs.getString("CONFIG_VALUE");
                logger.info("Retrieved Config Value for Key '{}': {}", configKey, configValue);
                return configValue;
            } else {
                String recentlyCompletedHour = getRecentlyCompletedHour();
                logger.info("No Config Value Found for Key: {}, Returning Recently Completed Hour: {}", configKey,
                        recentlyCompletedHour);
                return recentlyCompletedHour;
            }

        } catch (ClassNotFoundException e) {
            logger.info("ClassNotFoundException In Get Latest Hourly Config Value for Key '{}': {}", configKey,
                    e.getMessage());
            return getRecentlyCompletedHour();
        } catch (SQLException e) {
            logger.info("SQLException In Get Latest Hourly Config Value for Key '{}': {}", configKey, e.getMessage());
            return getRecentlyCompletedHour();
        } catch (Exception e) {
            logger.info("Unexpected Error Getting Latest Hourly Config Value for Key '{}': {}", configKey,
                    e.getMessage());
            return getRecentlyCompletedHour();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
                if (shouldCloseConnection && configConnection != null)
                    configConnection.close();
            } catch (SQLException e) {
                logger.info("Error Closing Database Resources: {}", e.getMessage());
            }
        }
    }

    private String getRecentlyCompletedHour() {
        return LocalDateTime.now(ZoneId.of("UTC"))
                .minusHours(1)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
    }

    private List<Long> generateTimestamps(String configValue, int numberOfInstances, String frequency, String duration,
            int reportWidgetId) {
        List<Long> timestamps = new ArrayList<>();

        try {
            int year = Integer.parseInt(configValue.substring(0, 4));
            int month = Integer.parseInt(configValue.substring(4, 6));
            int day = Integer.parseInt(configValue.substring(6, 8));
            int hour = Integer.parseInt(configValue.substring(8, 10));
            int minute = Integer.parseInt(configValue.substring(10, 12));

            LocalDateTime baseTime = LocalDateTime.of(year, month, day, hour, minute, 0, 0);
            int intervalMinutes = getIntervalMinutes(frequency);
            logger.info("REPORT_WIDGET_ID_PK: {}, Interval Minutes: {}", reportWidgetId, intervalMinutes);
            int totalDurationMinutes = getTotalDurationMinutes(duration, numberOfInstances, intervalMinutes);
            logger.info("REPORT_WIDGET_ID_PK: {}, Total Duration Minutes: {}", reportWidgetId, totalDurationMinutes);

            for (int i = 0; i < numberOfInstances; i++) {
                LocalDateTime instanceTime = baseTime.minusMinutes(i * intervalMinutes);
                long epochMillis = instanceTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
                timestamps.add(epochMillis);
            }

            logger.info("Generated Timestamps From Config Value '{}': {}", configValue, timestamps);

        } catch (Exception e) {
            logger.info("Error Generating Timestamps from Config Value '{}': {}", configValue, e.getMessage());
        }

        return timestamps;
    }

    private int getIntervalMinutes(String frequency) {
        if (frequency == null || frequency.trim().isEmpty()) {
            logger.info("Frequency is Null or Empty, Defaulting to 60 Minutes (Hour)");
            return 60;
        }

        switch (frequency.trim()) {
            case "15 Min":
                return 15;
            case "5 Min":
                return 5;
            case "Hour":
                return 60;
            case "Day":
                return 24 * 60;
            default:
                logger.info("Unknown frequency: {}, Defaulting to 60 Minutes (Hour)", frequency);
                return 60;
        }
    }

    private int getTotalDurationMinutes(String duration, int numberOfInstances, int intervalMinutes) {
        if (duration == null || duration.trim().isEmpty()) {
            logger.info("Duration is Null or Empty, Defaulting to DAILY Calculation");
            return numberOfInstances * intervalMinutes;
        }

        switch (duration.trim()) {
            case "DAILY":
                return numberOfInstances * intervalMinutes;
            case "HOURLY":
                return numberOfInstances * intervalMinutes;
            default:
                logger.info("Unknown Duration: {}, Defaulting to DAILY Calculation", duration);
                return numberOfInstances * intervalMinutes;
        }
    }

    private boolean isAlreadyProcessedToday(int reportWidgetId, String scheduledAt, Connection connection) {
        if (connection == null) {
            logger.info("Database connection is null, cannot check if report already processed today");
            return false;
        }

        try {
            String query = """
                    SELECT COUNT(*) as count
                    FROM GENERATED_REPORT
                    WHERE REPORT_WIDGET_ID = ?
                    AND DATE(GENERATED_DATE) = CURDATE()
                    AND PROGRESS_STATE IN ('IN_QUEUE', 'IN_PROGRESS', 'GENERATED')
                    """;

            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setInt(1, reportWidgetId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int count = rs.getInt("count");
                        logger.info("REPORT_WIDGET_ID_PK: {}, Already Processed Today Count: {}", reportWidgetId,
                                count);
                        return count > 0;
                    }
                }
            }
        } catch (SQLException e) {
            logger.info("SQL Error Checking if Report Already Processed Today for REPORT_WIDGET_ID_PK {}: {}",
                    reportWidgetId, e.getMessage());
        } catch (Exception e) {
            logger.info("Unexpected Error Checking if Report Already Processed Today for REPORT_WIDGET_ID_PK {}: {}",
                    reportWidgetId, e.getMessage());
        }
        return false;
    }

    private int insertGeneratedReport(Map<String, Object> report, Connection connection) throws SQLException {
        if (connection == null) {
            throw new SQLException("Database connection is null, cannot insert generated report");
        }

        String insertQuery = """
                INSERT INTO GENERATED_REPORT (
                    DOMAIN, DURATION, GENERATED_DATE, NODE, REPORT_NAME,
                    REPORT_TYPE, VENDOR, USER_ID_FK,
                    GENERATED_TYPE, PROGRESS_STATE, GEOGRAPHY, REPORT_LEVEL,
                    TECHNOLOGY, REPORT_WIDGET_ID, FILE_MANAGEMENT_FK
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement stmt = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, (String) report.get("DOMAIN"));
            stmt.setString(2, (String) report.get("DURATION"));
            stmt.setString(3,
                    LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            stmt.setString(4, (String) report.get("NODE"));
            stmt.setString(5, (String) report.get("REPORT_NAME"));
            stmt.setString(6, (String) report.get("REPORT_TYPE"));
            stmt.setString(7, (String) report.get("VENDOR"));
            stmt.setInt(8, (Integer) report.get("USER_ID_FK"));
            stmt.setString(9, (String) report.get("GENERATED_TYPE"));
            stmt.setString(10, "IN_QUEUE");
            stmt.setString(11, (String) report.get("GEOGRAPHY"));
            stmt.setString(12, (String) report.get("REPORT_LEVEL"));
            stmt.setString(13, (String) report.get("TECHNOLOGY"));
            stmt.setInt(14, (Integer) report.get("REPORT_WIDGET_ID_PK"));
            stmt.setInt(15, (Integer) report.get("FILE_MANAGEMENT_FK"));

            int affectedRows = stmt.executeUpdate();
            logger.info("Inserted Generated Report, Affected Rows: {}", affectedRows);

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    int generatedId = rs.getInt(1);
                    logger.info("Generated Report ID: {}", generatedId);
                    return generatedId;
                }
            }
        }

        throw new SQLException("Failed to Insert Generated Report - No generated key returned");
    }

    private void updateReportWidgetStatus(int reportWidgetId, String status, Connection connection)
            throws SQLException {
        if (connection == null) {
            throw new SQLException("Database connection is null, cannot update report widget status");
        }

        String updateQuery = "UPDATE REPORT_WIDGET SET STATUS = ? WHERE REPORT_WIDGET_ID_PK = ?";

        try (PreparedStatement stmt = connection.prepareStatement(updateQuery)) {
            stmt.setString(1, status);
            stmt.setInt(2, reportWidgetId);
            int affectedRows = stmt.executeUpdate();
            logger.info("Updated REPORT_WIDGET Status for ID {} to {}, Affected Rows: {}", reportWidgetId, status,
                    affectedRows);
        }
    }

    private void createFlowFile(Map<String, Object> report, List<Long> timestamps, int generatedReportId,
            FlowFile inputFlowFile, ProcessSession session) {
        if (session == null) {
            logger.info("ProcessSession is Null, Cannot Create Flow File");
            return;
        }

        if (inputFlowFile == null) {
            logger.info("Input FlowFile is Null, Cannot Create Flow File");
            return;
        }

        try {
            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("READY_TO_START", "true");
            attributes.put("IS_SCHEDULED_WITH_INPUT", "TRUE");
            attributes.put("REPORT_WIDGET_ID_PK", String.valueOf(report.get("REPORT_WIDGET_ID_PK")));
            attributes.put("GENERATED_REPORT_ID_PK", String.valueOf(generatedReportId));
            String timestampsString = timestamps.stream()
                    .map(String::valueOf)
                    .collect(java.util.stream.Collectors.joining(","));
            attributes.put("INPUT_TIMESTAMP_FILTER", timestampsString);
            attributes.put("EMAIL", (String) report.get("EMAIL"));

            logger.info("Flow File Attributes: {}", attributes);

            inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
            session.transfer(inputFlowFile, RELATIONSHIP_SUCCESS);

            logger.info(
                    "Updated Input Flow File for Scheduled Report: REPORT_WIDGET_ID_PK={}, GENERATED_REPORT_ID_PK={}, Timestamps={}",
                    report.get("REPORT_WIDGET_ID_PK"),
                    generatedReportId, timestamps);
        } catch (Exception e) {
            logger.info("Error Creating Flow File for REPORT_WIDGET_ID_PK {}: {}",
                    report.get("REPORT_WIDGET_ID_PK"), e.getMessage());
            if (inputFlowFile != null) {
                try {
                    session.transfer(inputFlowFile, RELATIONSHIP_FAILURE);
                } catch (Exception transferError) {
                    logger.info("Error transferring FlowFile to failure: ", transferError);
                }
            }
        }
    }

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flow File Successfully Processed!")
            .build();

    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flow File Failed to Process!")
            .build();

    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            java.sql.Connection connection, String cacheValue) {

        logger.info("Starting OptionalDailyScheduled Logic...");

        try {
            if (session == null) {
                logger.info("ProcessSession is Null, Cannot Proceed!");
                // return flowFile;
            }

            if (connection == null) {
                logger.info("Database Connection is Null, Cannot Proceed!");
                if (flowFile != null) {
                    // session.transfer(flowFile, RELATIONSHIP_FAILURE);
                }
                // return flowFile;
            }

            // Store connection and session for scheduled processing
            updateStoredParameters(flowFile, session, connection);

            if (!isRunning) {
                startScheduledProcessor();
            }
            
            // Process immediately on first call
            processScheduledReports(flowFile, session, connection);

        } catch (Exception e) {
            logger.info("Error in OptionalDailyScheduled Processor: ", e);
            if (flowFile != null && session != null) {
                try {
                    session.transfer(flowFile, RELATIONSHIP_FAILURE);
                } catch (Exception transferError) {
                    logger.info("Error transferring FlowFile to failure: ", transferError);
                }
            }
        }

        return flowFile;
    }

    public void cleanup() {
        stopScheduledProcessor();
    }
    
    // Method to update stored parameters for scheduled processing
    public void updateStoredParameters(FlowFile flowFile, ProcessSession session, java.sql.Connection connection) {
        this.storedFlowFile = flowFile;
        this.storedSession = session;
        this.storedConnection = connection;
        logger.info("Updated Stored Parameters for Scheduled Processing");
    }
}
