package com.enttribe.custom.processor;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.Relationship.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertIntoProcessingAudit implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(InsertIntoProcessingAudit.class);

    public static final Relationship REL_SUCCESS = new Builder()
            .name("success")
            .description("FlowFile Successfully Processed!")
            .build();

    public static final Relationship REL_FAILURE = new Builder()
            .name("failure")
            .description("FlowFile Failed To Process!")
            .build();

    @Override
    public void run() {
    }

    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) {

        logger.info("InsertIntoProcessingAudit Execution Started!");

        String listOfDomVenTechFreq = flowFile.getAttribute("listOfDomVenTechFreq");
        int daysToAdd = Integer.parseInt(flowFile.getAttribute("daysToAdd"));

        logger.info("Retrieved FlowFile Attributes | listOfDomVenTechFreq: {}, daysToAdd: {}",
                listOfDomVenTechFreq, daysToAdd);

        int totalInserted = 0;

        logger.info("daysToAdd: {}", daysToAdd);

        try (Connection conn = connection) {
            for (String entry : listOfDomVenTechFreq.split(",")) {
                String[] parts = entry.split("@");
                if (parts.length != 5) {
                    logger.error("Invalid Entry: {}", entry);
                    continue;
                }

                String domain = parts[0].trim();
                String vendor = parts[1].trim();
                String emsType = parts[2].trim();
                String technology = parts[3].trim();
                String[] frequencies = parts[4].split("#");

                for (String dataLevel : frequencies) {
                    dataLevel = dataLevel.trim().toUpperCase();
                    if (!dataLevel.isEmpty()) {
                        int inserted = insertAudit(conn, domain, vendor, technology, "PM", dataLevel, emsType,
                                daysToAdd);
                        totalInserted += inserted;
                    }
                }
            }

            flowFile = session.putAttribute(flowFile, "audit.status", "success");
            flowFile = session.putAttribute(flowFile, "audit.component", "InsertIntoProcessingAudit");
            flowFile = session.putAttribute(flowFile, "audit.timestamp", ZonedDateTime.now(ZoneOffset.UTC).toString());
            flowFile = session.putAttribute(flowFile, "audit.total.records.inserted", String.valueOf(totalInserted));
            flowFile = session.putAttribute(flowFile, "file_name", "InsertIntoProcessingAudit");

            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            logger.error("Processing Failed for InsertIntoProcessingAudit", e);
            flowFile = session.putAttribute(flowFile, "audit.status", "failure");
            flowFile = session.putAttribute(flowFile, "audit.component", "InsertIntoProcessingAudit");
            flowFile = session.putAttribute(flowFile, "audit.error.message", e.getMessage());

            session.transfer(flowFile, REL_FAILURE);
        }

        logger.info("InsertIntoProcessingAudit Execution Completed!");

        return flowFile;
    }

    public static int insertAudit(Connection conn, String domain, String vendor, String technology,
            String module, String dataLevel, String emsType, int daysToAdd) throws SQLException {

        logger.info(
                "Entering insertAudit() | Domain: {}, Vendor: {}, Technology: {}, Module: {}, DataLevel: {}, EmsType: {}, DaysToAdd: {}",
                domain, vendor, technology, module, dataLevel, emsType, daysToAdd);

        ZonedDateTime nowUtc = ZonedDateTime.now(ZoneOffset.UTC).plusDays(daysToAdd);
        ZonedDateTime startUtc;
        LocalDate auditDate;

        switch (dataLevel) {
            case "MONTHLY":
                startUtc = nowUtc.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
                break;
            case "WEEKLY":
                startUtc = nowUtc.with(DayOfWeek.MONDAY).truncatedTo(ChronoUnit.DAYS);
                break;
            default:
                startUtc = nowUtc.truncatedTo(ChronoUnit.DAYS);
        }

        auditDate = startUtc.toLocalDate();
        logger.info("Computed Audit Start Date/Time | Start UTC: {}, Audit Date: {}", startUtc, auditDate);

        List<Timestamp> timestampsToInsert = new ArrayList<>();
        int intervalCount;
        int intervalMinutes;

        switch (dataLevel) {
            case "FIVEMIN":
                intervalCount = 288;
                intervalMinutes = 5;
                break;
            case "QUARTERLY":
                intervalCount = 96;
                intervalMinutes = 15;
                break;
            case "HOURLY":
                intervalCount = 24;
                intervalMinutes = 60;
                break;
            case "DAILY":
                intervalCount = 1;
                intervalMinutes = 1440;
                break;
            case "WEEKLY":
                intervalCount = 1;
                intervalMinutes = 10080;
                break;
            case "MONTHLY":
                intervalCount = 1;
                intervalMinutes = 43200;
                break;
            default:
                logger.warn("Unsupported dataLevel: {}", dataLevel);
                return 0;
        }

        logger.info("Interval Configuration | DataLevel: {}, IntervalCount: {}, IntervalMinutes: {}",
                dataLevel, intervalCount, intervalMinutes);

        int skippedCount = 0;
        for (int i = 0; i < intervalCount; i++) {
            ZonedDateTime auditTime = startUtc.plusMinutes((long) i * intervalMinutes);
            Timestamp ts = Timestamp.from(auditTime.toInstant());
            if (!exists(conn, ts, dataLevel, domain, vendor, module, technology, emsType)) {
                timestampsToInsert.add(ts);
            } else {
                skippedCount++;
            }
        }

        logger.info("Audit Preparation Summary | ToInsert: {}, SkippedExisting: {}",
                timestampsToInsert.size(), skippedCount);

        if (!timestampsToInsert.isEmpty()) {
            String insertSQL = "INSERT INTO PROCESSING_AUDIT (AUDIT_DATE, AUDIT_TIME, DATA_LEVEL, AVAILABLE, PROCESSED, DOMAIN, VENDOR, TECHNOLOGY, MODULE, EMS_TYPE, JOB_STATUS, MODIFICATION_TIME, IS_FILE_AVAILABLE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                for (Timestamp ts : timestampsToInsert) {
                    pstmt.setDate(1, Date.valueOf(auditDate));
                    pstmt.setTimestamp(2, ts);
                    pstmt.setString(3, dataLevel);
                    pstmt.setInt(4, 1);
                    pstmt.setInt(5, 0);
                    pstmt.setString(6, domain);
                    pstmt.setString(7, vendor);
                    pstmt.setString(8, technology);
                    pstmt.setString(9, module);
                    pstmt.setString(10, emsType);
                    pstmt.setInt(11, 0);
                    pstmt.setTimestamp(12, ts);
                    pstmt.setInt(13, 0);
                    pstmt.addBatch();
                }
                int[] batchResult = pstmt.executeBatch();
                int insertedCount = batchResult.length;
                logger.info("Successfully Inserted {} Audit Rows | [{} | {} | {} | {} | {}]",
                        insertedCount, domain, vendor, technology, module, dataLevel);
                return insertedCount;

            } catch (SQLException ex) {
                logger.error("SQL Error During Audit Insert | [{} | {} | {} | {} | {}] | Message: {}",
                        domain, vendor, technology, module, dataLevel, ex.getMessage(), ex);
                throw ex;
            }
        } else {
            logger.info("No New Audit Rows to Insert | [{} | {} | {} | {} | {}]",
                    domain, vendor, technology, module, dataLevel);
            return 0;
        }
    }

    private static boolean exists(Connection conn, Timestamp auditTime, String dataLevel, String domain,
            String vendor, String module, String technology, String emsType) throws SQLException {

        logger.info(
                "Entering exists() | AuditTime: {}, DataLevel: {}, Domain: {}, Vendor: {}, Module: {}, Technology: {}, EmsType: {}",
                auditTime, dataLevel, domain, vendor, module, technology, emsType);

        String query = "SELECT 1 FROM PROCESSING_AUDIT WHERE AUDIT_TIME = ? AND DATA_LEVEL = ? AND DOMAIN = ? AND VENDOR = ? AND MODULE = ? AND TECHNOLOGY = ? AND EMS_TYPE = ? LIMIT 1";

        boolean recordExists = false;

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setTimestamp(1, auditTime);
            pstmt.setString(2, dataLevel);
            pstmt.setString(3, domain);
            pstmt.setString(4, vendor);
            pstmt.setString(5, module);
            pstmt.setString(6, technology);
            pstmt.setString(7, emsType);
            try (ResultSet rs = pstmt.executeQuery()) {
                recordExists = rs.next();
            }
            logger.info("Record Existence Check Completed | Exists: {}", recordExists);
            return recordExists;
        }
    }
}
