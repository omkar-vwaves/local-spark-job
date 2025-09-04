package com.enttribe.custom.processor;

import java.sql.Connection;
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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HourlyScheduledTest02 implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HourlyScheduledTest02.class);
    private static final String DEFAULT_HOURLY_SCHEDULE_HOUR = "00:00";

    @Override
    public void run() {
        logger.info("HourlyScheduledTest02 Executed Started!");
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

        long startTime = System.currentTimeMillis();

        logger.info("========== HourlyScheduledTest02 Execution Started ==========");

        try {
            flowFile = processScheduledReports(flowFile, session, connection);
        } catch (Exception e) {
            logger.info("Exception Occured in HourlyScheduledTest02 Processor: ", e);
            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("READY_TO_START", "false");
            attributes.put("failure.reason", "Exception Occured in HourlyScheduledTest02 Processor: ");
            attributes.put("failure.details", e.getMessage());
            flowFile = session.putAllAttributes(flowFile, attributes);
            logger.info("Transferred New FlowFile to Failure!");
        }

        long endTime = System.currentTimeMillis();
        long totalTimeMillis = endTime - startTime;
        long totalSeconds = totalTimeMillis / 1000;
        long minutes = totalSeconds / 60;
        long seconds = totalSeconds % 60;
        logger.info(String.format("Total Time Consumed For Execution: %02d:%02d (MM:SS)", minutes, seconds));

        logger.info("========== HourlyScheduledTest02 Execution Completed ==========");

        String readyToStart = flowFile.getAttribute("READY_TO_START");
        if (readyToStart != null && readyToStart.equals("true")) {
            session.transfer(flowFile, RELATIONSHIP_SUCCESS);
        } else {
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        }

        return flowFile;
    }

    private FlowFile processScheduledReports(FlowFile inputFlowFile, ProcessSession session,
            java.sql.Connection connection) {
        try {
            String currentHourAttr = inputFlowFile.getAttribute("CURRENT_HOUR");
            String reportIdAttr = inputFlowFile.getAttribute("REPORT_WIDGET_ID_PK");

            logger.info("Processing Scheduled Reports with CURRENT_HOUR={} And REPORT_WIDGET_ID_PK={}", currentHourAttr,
                    reportIdAttr);

            if (currentHourAttr == null || !currentHourAttr.matches("^\\d{2}:\\d{2}$")) {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Invalid CURRENT_HOUR");
                attributes.put("failure.details", "CURRENT_HOUR Attribute Missing or Invalid Format (HH:mm)");
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                logger.info("CURRENT_HOUR Attribute is Missing or Invalid Format. Value: {}", currentHourAttr);
                return inputFlowFile;
            }

            Integer requestedReportId = null;
            try {
                requestedReportId = (reportIdAttr == null || reportIdAttr.trim().isEmpty()) ? null
                        : Integer.valueOf(reportIdAttr.trim());
            } catch (NumberFormatException nfe) {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Invalid REPORT_WIDGET_ID_PK");
                attributes.put("failure.details", "REPORT_WIDGET_ID_PK Must Be An Integer!");
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                logger.info("Invalid REPORT_WIDGET_ID_PK: {}", reportIdAttr);
                return inputFlowFile;
            }

            if (requestedReportId == null) {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Missing REPORT_WIDGET_ID_PK");
                attributes.put("failure.details", "REPORT_WIDGET_ID_PK Attribute is Required On Input FlowFile");
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                session.transfer(inputFlowFile, RELATIONSHIP_FAILURE);
                logger.info(
                        "REPORT_WIDGET_ID_PK is Missing in the Input FlowFile. Cannot Proceed With Scheduled Report Processing.");
                return inputFlowFile;
            }

            List<Map<String, Object>> readyReports = getReadyScheduledReports(connection, requestedReportId);
            logger.info("Ready Scheduled Reports (Filtered by REPORT_WIDGET_ID_PK={}): {}", requestedReportId,
                    readyReports);

            if (readyReports.isEmpty()) {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "No Ready Scheduled Reports Found!");
                attributes.put("failure.details",
                        "No Ready Scheduled Reports Found For REPORT_WIDGET_ID_PK=" + requestedReportId);
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                return inputFlowFile;
            }

            Map<String, Object> matchedReport = readyReports.get(0);

            inputFlowFile = processReport(matchedReport, inputFlowFile, session, connection);
            return inputFlowFile;

        } catch (Exception e) {
            logger.info("Error Processing Scheduled Reports: ", e);
            try {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Scheduled Reports Processing Error");
                attributes.put("failure.details", e.getMessage());
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                return inputFlowFile;
            } catch (Exception transferError) {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Scheduled Reports Processing Error");
                attributes.put("failure.details", e.getMessage());
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);

                logger.info("Error Transferring FlowFile to Failure: ", transferError);

                return inputFlowFile;
            }
        }
    }

    private List<Map<String, Object>> getReadyScheduledReports(Connection connection, Integer requestedReportId)
            throws SQLException {
        List<Map<String, Object>> readyReports = new ArrayList<>();

        String query = """
                SELECT
                    rw.REPORT_WIDGET_ID_PK,
                    COALESCE(GROUP_CONCAT(jt.email ORDER BY jt.email SEPARATOR ', '), '') AS EMAIL,
                    JSON_UNQUOTE(JSON_EXTRACT(rw.FLOW_CONFIGURATION, '$.reportFormatType')) AS EXTENSION,
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
                WHERE rw.REPORT_WIDGET_ID_PK = ?
                """;

        logger.info("Ready Scheduled Reports Query (Filtered): {}", query);

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, requestedReportId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    int reportWidgetId = rs.getInt("REPORT_WIDGET_ID_PK");
                    String configurationRaw = rs.getString("CONFIGURATION_RAW");

                    List<String> scheduledHours = extractScheduleHours(configurationRaw);
                    String numberOfInstanceStr = extractNumberOfInstance(configurationRaw);

                    logger.info("REPORT_WIDGET_ID_PK: {}, Extracted Scheduled Hours: {}, Number of Instance: {}",
                            reportWidgetId, scheduledHours, numberOfInstanceStr);

                    if (scheduledHours != null && !scheduledHours.isEmpty()) {
                        int numberOfInstance = 1;
                        if (numberOfInstanceStr != null && !numberOfInstanceStr.trim().isEmpty()) {
                            try {
                                numberOfInstance = Integer.parseInt(numberOfInstanceStr);
                            } catch (NumberFormatException e) {
                                logger.info(
                                        "REPORT_WIDGET_ID_PK: {}, Invalid Number Of Instance Value '{}', Using Default Value 1",
                                        reportWidgetId, numberOfInstanceStr);
                                numberOfInstance = 1;
                            }
                        } else {
                            logger.info(
                                    "REPORT_WIDGET_ID_PK: {}, Number Of Instance Is Null Or Empty, Using Default Value 1",
                                    reportWidgetId);
                        }

                        Map<String, Object> report = new LinkedHashMap<>();
                        report.put("REPORT_WIDGET_ID_PK", rs.getInt("REPORT_WIDGET_ID_PK"));
                        report.put("REPORT_NAME", rs.getString("REPORT_NAME"));
                        report.put("DOMAIN", rs.getString("DOMAIN"));
                        report.put("VENDOR", rs.getString("VENDOR"));
                        report.put("TECHNOLOGY", rs.getString("TECHNOLOGY"));
                        report.put("GENERATED_TYPE", rs.getString("GENERATED_TYPE"));
                        report.put("USER_ID_FK", rs.getInt("USER_ID_FK"));
                        report.put("EMAIL", rs.getString("EMAIL"));
                        report.put("SCHEDULED_HOURS", scheduledHours);
                        report.put("SCHEDULED_AT", scheduledHours.get(0));
                        report.put("NUMBER_OF_INSTANCE", numberOfInstance);
                        report.put("DURATION", rs.getString("DURATION"));
                        report.put("FREQUENCY", rs.getString("FREQUENCY"));
                        report.put("REPORT_TYPE", rs.getString("REPORT_TYPE"));
                        report.put("GEOGRAPHY", rs.getString("GEOGRAPHY"));
                        report.put("REPORT_LEVEL", rs.getString("REPORT_LEVEL"));
                        report.put("NODE", rs.getString("NODE"));
                        report.put("FILE_MANAGEMENT_FK", rs.getInt("FILE_MANAGEMENT_FK"));
                        report.put("REPORT_MEASURE", rs.getString("REPORT_MEASURE"));
                        report.put("EXTENSION", rs.getString("EXTENSION"));
                        readyReports.add(report);
                    } else {
                        logger.info(
                                "REPORT_WIDGET_ID_PK: {}, Could Not Extract Schedule Time Or Number Of Instances From Configuration: {}",
                                reportWidgetId, configurationRaw);
                    }
                }
            }
        }

        return readyReports;
    }

    private List<String> extractScheduleHours(String configuration) {
        List<String> scheduleHours = new ArrayList<>();
        try {
            if (configuration.contains("'scheduleAt':")) {
                int startIndex = configuration.indexOf("'scheduleAt':") + "'scheduleAt':".length();
                int endIndex = configuration.indexOf("]", startIndex);
                if (endIndex > startIndex) {
                    String schedulePart = configuration.substring(startIndex, endIndex + 1).trim();

                    if ("[]".equals(schedulePart)) {
                        scheduleHours.add(DEFAULT_HOURLY_SCHEDULE_HOUR);
                        return scheduleHours;
                    }

                    // Extract all hours from the array
                    String[] parts = schedulePart.split("'");
                    for (String part : parts) {
                        if (part.matches("\\d{2}:\\d{2}")) {
                            scheduleHours.add(part);
                        }
                    }
                }
            }
            
            if (scheduleHours.isEmpty()) {
                scheduleHours.add(DEFAULT_HOURLY_SCHEDULE_HOUR);
            }
            
            return scheduleHours;
        } catch (Exception e) {
            logger.info("Error Extracting Schedule Hours From Configuration: {}", configuration, e);
            scheduleHours.add(DEFAULT_HOURLY_SCHEDULE_HOUR);
            return scheduleHours;
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

    private FlowFile processReport(Map<String, Object> report, FlowFile inputFlowFile, ProcessSession session,
            Connection connection) {
        try {
            int reportWidgetId = (Integer) report.get("REPORT_WIDGET_ID_PK");
            int numberOfInstances = (Integer) report.get("NUMBER_OF_INSTANCE");
            @SuppressWarnings("unchecked")
            List<String> scheduledHours = (List<String>) report.get("SCHEDULED_HOURS");
            String currentHour = getCurrentHourFromContext(inputFlowFile);

            if (!isCurrentHourScheduled(currentHour, scheduledHours)) {
                logger.info("REPORT_WIDGET_ID_PK: {}, Current Hour '{}' Is Not In Scheduled Hours: {}", 
                        reportWidgetId, currentHour, scheduledHours);
                
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Current Hour Not Scheduled!");
                attributes.put("failure.details", 
                        "Current Hour '" + currentHour + "' Is Not In Scheduled Hours: " + scheduledHours);
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                return inputFlowFile;
            }

            boolean isAlreadyProcessed = isAlreadyProcessedForHour(reportWidgetId, currentHour, connection);
            if (isAlreadyProcessed) {
                logger.info("Report Already Processed For Hour {}: REPORT_WIDGET_ID_PK={}", currentHour, reportWidgetId);

                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Report Already Processed For This Hour!");
                attributes.put("failure.details",
                        "Report with ID " + reportWidgetId + " Has Already Processed For Hour " + currentHour + 
                        " on " + java.time.LocalDate.now());
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                logger.info("Transferred New FlowFile to Failure!");
                return inputFlowFile;
            } else {
                logger.info("REPORT_WIDGET_ID_PK: {}, Not Already Processed For Hour: {}", reportWidgetId, currentHour);
            }

            int generatedReportId = insertGeneratedReport(report, connection);
            logger.info("REPORT_WIDGET_ID_PK: {}, GENERATED_REPORT_ID: {}", reportWidgetId, generatedReportId);

            inputFlowFile = createFlowFile(report, generatedReportId, currentHour, inputFlowFile, session);
            updateReportWidgetStatus(reportWidgetId, "IN_PROGRESS", generatedReportId, connection);

            logger.info("Processed Scheduled Report: REPORT_WIDGET_ID_PK={}, Instances={}",
                    reportWidgetId, numberOfInstances);

            return inputFlowFile;

        } catch (Exception e) {
            logger.info("Error Processing Report: ", e);
            if (session != null) {
                try {
                    Map<String, String> attributes = new LinkedHashMap<>();
                    attributes.put("READY_TO_START", "false");
                    attributes.put("failure.reason", "Report Processing Error");
                    attributes.put("failure.details", e.getMessage());
                    inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                    logger.info("Transferred New FlowFile to Failure!");
                    return inputFlowFile;
                } catch (Exception transferError) {
                    logger.info("Error transferring FlowFile to failure: ", transferError);
                    Map<String, String> attributes = new LinkedHashMap<>();
                    attributes.put("READY_TO_START", "false");
                    attributes.put("failure.reason", "Flow File Creation Error");
                    attributes.put("failure.details", e.getMessage());
                    inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                    return inputFlowFile;
                }
            }
        }
        return inputFlowFile;
    }

    private int insertGeneratedReport(Map<String, Object> report, Connection connection) throws SQLException {

        String reportMeasure = (String) report.get("REPORT_MEASURE");
        String reportType;
        switch (reportMeasure) {
            case "Performance Report" -> reportType = "PERFORMANCE_REPORT";
            case "Trend Report" -> reportType = "TREND_REPORT";
            case "Exception Report" -> reportType = "EXCEPTION_REPORT";
            case "OTF Report" -> reportType = "OTF_REPORT";
            default -> reportType = "PERFORMANCE_REPORT";
        }

        String reportSearch = generateReportSearch(reportMeasure, (String) report.get("REPORT_NAME"));

        logger.info("REPORT_TYPE: {}", reportType);
        logger.info("REPORT_SEARCH: {}", reportSearch);

        report.put("REPORT_TYPE", reportType);
        report.put("REPORT_SEARCH", reportSearch);

        String insertQuery = """
                INSERT INTO GENERATED_REPORT (
                    DOMAIN, DURATION, GENERATED_DATE, NODE, REPORT_NAME,
                    REPORT_TYPE, VENDOR, USER_ID_FK,
                    GENERATED_TYPE, PROGRESS_STATE, GEOGRAPHY, REPORT_LEVEL,
                    TECHNOLOGY, REPORT_WIDGET_ID, FILE_MANAGEMENT_FK, REPORT_SEARCH
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        logger.info("Inserting Generated Report: {}", report);
        logger.info("Mapped REPORT_TYPE: {}, REPORT_SEARCH: {}", reportType, reportSearch);

        int generatedId = -1;
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
            stmt.setString(16, (String) report.get("REPORT_SEARCH"));

            int affectedRows = stmt.executeUpdate();
            logger.info("Inserted Generated Report, Affected Rows: {}", affectedRows);

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    generatedId = rs.getInt(1);
                    logger.info("Inserted Generated Report ID: {}", generatedId);
                }
            }
        }

        if (generatedId == -1) {
            throw new SQLException("Failed to Insert Generated Report - No Generated Key Returned!");
        }

        insertUserMapping(generatedId, (Integer) report.get("USER_ID_FK"), connection);

        return generatedId;
    }

    private String generateReportSearch(String reportMeasure, String reportName) {
        String baseSearch = "performance_report";

        try {
            if (reportMeasure != null) {
                switch (reportMeasure.trim()) {
                    case "Performance Report" -> baseSearch = "performance_report";
                    case "Trend Report" -> baseSearch = "trend_report";
                    case "Exception Report" -> baseSearch = "exception_report";
                    case "OTF Report" -> baseSearch = "otf_report";
                    default -> baseSearch = "performance_report";
                }
            }

            String normalizedReportName = (reportName == null ? "" : reportName)
                    .toLowerCase()
                    .replaceAll("\\s+", "");

            return baseSearch + normalizedReportName;
        } catch (Exception e) {
            return "performance_report";
        }
    }

    private void insertUserMapping(int generatedReportId, Integer userId, Connection connection) throws SQLException {
        String insertMappingQuery = """
                INSERT INTO PM_REPORTS_USER_MAPPING (
                    GENERATED_REPORT_ID_FK, USER_ID_FK
                ) VALUES (?, ?)
                """;

        logger.info("Inserting User Mapping: GENERATED_REPORT_ID_FK={}, USER_ID_FK={}", generatedReportId, userId);

        try (PreparedStatement stmt = connection.prepareStatement(insertMappingQuery)) {
            stmt.setInt(1, generatedReportId);
            stmt.setInt(2, userId);

            int affectedRows = stmt.executeUpdate();
            logger.info("Inserted User Mapping, Affected Rows: {}", affectedRows);
        }
    }

    private void updateReportWidgetStatus(int reportWidgetId, String status, int generatedReportId,
            Connection connection)
            throws SQLException {

        String updateQuery = "UPDATE REPORT_WIDGET SET STATUS = ?, GENERATED_REPORT_ID = ? WHERE REPORT_WIDGET_ID_PK = ?";
        try (PreparedStatement stmt = connection.prepareStatement(updateQuery)) {
            stmt.setString(1, status);
            stmt.setInt(2, generatedReportId);
            stmt.setInt(3, reportWidgetId);
            int affectedRows = stmt.executeUpdate();
            logger.info(
                    "Updated REPORT_WIDGET Status for REPORT_WIDGET_ID_PK {} to Status: {}, GENERATED_REPORT_ID: {}, Affected Rows: {}",
                    reportWidgetId, status, generatedReportId, affectedRows);
        }
    }

    private FlowFile createFlowFile(Map<String, Object> report, int generatedReportId, String scheduledAt,
            FlowFile inputFlowFile, ProcessSession session) {

        try {
            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("REPORT_WIDGET_ID_PK", String.valueOf(report.get("REPORT_WIDGET_ID_PK")));
            attributes.put("GENERATED_REPORT_ID", String.valueOf(generatedReportId));
            attributes.put("generated_report_id", String.valueOf(generatedReportId));
            attributes.put("EMAIL", (String) report.get("EMAIL"));
            attributes.put("email", (String) report.get("EMAIL"));
            attributes.put("REPORT_NAME", (String) report.get("REPORT_NAME"));
            attributes.put("report_name", (String) report.get("REPORT_NAME"));
            attributes.put("EXTENSION", (String) report.get("EXTENSION"));
            attributes.put("extension", (String) report.get("EXTENSION"));
            attributes.put("READY_TO_START", "true");
            attributes.put("success.reason", "Flow File Created Successfully");
            attributes.put("success.details", "Report is Successfully Triggered At " + scheduledAt);

            logger.info("Flow File Attributes: {}", attributes);
            inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);

            logger.info("Transferred FlowFile to Success for REPORT_WIDGET_ID_PK: {}",
                    report.get("REPORT_WIDGET_ID_PK"));

            return inputFlowFile;
        } catch (Exception e) {
            logger.info("Error Creating Flow File for REPORT_WIDGET_ID_PK {}: {}",
                    report.get("REPORT_WIDGET_ID_PK"), e.getMessage());
            try {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Flow File Creation Error");
                attributes.put("failure.details", e.getMessage());
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                return inputFlowFile;
            } catch (Exception transferError) {
                Map<String, String> attributes = new LinkedHashMap<>();
                attributes.put("READY_TO_START", "false");
                attributes.put("failure.reason", "Flow File Creation Error");
                attributes.put("failure.details", e.getMessage());
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                logger.info("Error Transferring FlowFile to Failure: ", transferError);
                return inputFlowFile;
            }
        }
    }

    private String getCurrentHourFromContext(FlowFile inputFlowFile) {
        String currentHour = inputFlowFile.getAttribute("CURRENT_HOUR");
        if (currentHour == null || !currentHour.matches("^\\d{2}:\\d{2}$")) {
            logger.info("CURRENT_HOUR Attribute is Missing or Invalid Format. Value: {}", currentHour);
            return DEFAULT_HOURLY_SCHEDULE_HOUR;
        }
        return currentHour;
    }

    private boolean isCurrentHourScheduled(String currentHour, List<String> scheduledHours) {
        if (scheduledHours == null || scheduledHours.isEmpty()) {
            return false;
        }
        return scheduledHours.contains(currentHour);
    }

    private boolean isAlreadyProcessedForHour(int reportWidgetId, String currentHour, Connection connection) {
        try {
            String query = """
                    SELECT COUNT(*) AS COUNT
                    FROM GENERATED_REPORT
                    WHERE REPORT_WIDGET_ID = ?
                    AND DATE(GENERATED_DATE) = CURDATE()
                    AND HOUR(GENERATED_DATE) = ?
                    """;

            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setInt(1, reportWidgetId);
                stmt.setString(2, currentHour.split(":")[0]);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int count = rs.getInt("COUNT");
                        logger.info("REPORT_WIDGET_ID_PK: {}, Already Processed For Hour {} Count: {}", 
                                reportWidgetId, currentHour, count);
                        return count > 0;
                    }
                }
            }
        } catch (SQLException e) {
            logger.info("SQL Error Checking if Report Already Processed For Hour {} for REPORT_WIDGET_ID_PK {}: {}", 
                    currentHour, reportWidgetId, e.getMessage());
        } catch (Exception e) {
            logger.info("Unexpected Error Checking if Report Already Processed For Hour {} for REPORT_WIDGET_ID_PK {}: {}", 
                    currentHour, reportWidgetId, e.getMessage());
        }
        return false;
    }

}
