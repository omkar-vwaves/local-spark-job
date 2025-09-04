package com.enttribe.pm.job.alert;

import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.context.JobContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GenerateAlert extends Processor {

    private static final int BATCH_SIZE = 200;
    private static final int MAX_BATCH_SIZE = 1000;
    private static final int MIN_BATCH_SIZE = 50;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final int CONNECTION_TIMEOUT = 30000;

    private static final int LARGE_DATASET_THRESHOLD = 100000;
    private static final int MAX_UNIQUE_KEYS_FOR_PRELOAD = 10000;
    private static final int DEBUG_LOG_LIMIT = 5;

    private static final int SIMPLE_INSERT_BATCH_SIZE = 2000;
    private static final int OPTIMIZED_BATCH_SIZE = 200;
    private static final int UPDATE_BATCH_SIZE = 500;
    private static final int INDIVIDUAL_UPDATE_BATCH_SIZE = 25;
    private static final int UPDATE_BATCH_SIZE_FALLBACK = 150;

    private static final int PRELOAD_QUERY_TIMEOUT = 60;
    private static final int BULK_UPDATE_TIMEOUT = 60;
    private static final int BATCH_INSERT_TIMEOUT = 120;
    private static final int UPDATE_BATCH_TIMEOUT = 45;
    private static final int INDIVIDUAL_UPDATE_TIMEOUT = 15;
    private static final int MYSQL_LOCK_TIMEOUT = 120;

    private static final int PROGRESS_LOG_INTERVAL = 50;
    private static final int BATCH_TIMEOUT_WARNING = 30000;
    private static final int OVERALL_TIMEOUT_WARNING = 300000;
    private static final int UPDATE_TIMEOUT_WARNING = 60000;

    private static final int SMALL_DATASET_THRESHOLD = 1000;
    private static final int MEDIUM_DATASET_THRESHOLD = 10000;
    private static final int LARGE_DATASET_THRESHOLD_FOR_BATCH = 100000;
    private static final int LARGE_BATCH_SIZE = 500;

    private static final int MEMORY_DIVISOR = 1024 * 1024;
    private static final int MILLISECONDS_PER_SECOND = 1000;
    private static final int PRELOAD_ESTIMATED_SIZE_CAP = 50000;

    private static final int DEBUG_LOG_FIRST_FEW = 3;
    private static final int EXACT_MATCH_KEY_PARTS = 8;

    private static final String ALARM_STATUS_OPEN = "OPEN";
    private static final String DEFAULT_OCCURRENCE = "1";
    private static final String SQL_MODE_NO_AUTO_VALUE = "NO_AUTO_VALUE_ON_ZERO";

    private static final int RETRY_BACKOFF_MULTIPLIER = 1000;
    private static final int RETRY_SLEEP_MULTIPLIER = 1;

    private static class BatchResult {
        final int newInserts;
        final int duplicatesSkipped;
        final int updates;

        BatchResult(int newInserts, int duplicatesSkipped, int updates) {
            this.newInserts = newInserts;
            this.duplicatesSkipped = duplicatesSkipped;
            this.updates = updates;
        }
    }

    private static class AlarmInfo {
        final int occurrence;
        final String status;
        final String openTime;
        final String changeTime;

        AlarmInfo(int occurrence, String status, String openTime, String changeTime) {
            this.occurrence = occurrence;
            this.status = status;
            this.openTime = openTime;
            this.changeTime = changeTime;
        }
    }

    public GenerateAlert() {
        super();
        logger.info("GenerateAlert Constructor Called!");
    }

    public GenerateAlert(Dataset<Row> dataFrame, Integer id, String processorName) {
        super(id, processorName);
        logger.info("GenerateAlert Constructor Called with Input DataFrame With ID: {} and Processor Name: {}",
                id,
                processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("Updated-07 Aug 12:50 GenerateAlert Execution Started!");

        Dataset<Row> inputDF = this.dataFrame;
        if (inputDF == null || inputDF.isEmpty()) {
            logger.info("Input DataFrame is Empty! Returning Empty DataFrame!");
            return inputDF;
        }

        List<Row> rows = inputDF.collectAsList();
        if (rows.isEmpty()) {
            logger.info("Rows are Empty! Returning Empty DataFrame!");
            return inputDF;
        }

        int adaptiveBatchSize = calculateAdaptiveBatchSize(rows.size());
        logger.info("Processing {} Rows In Batches Of {} (Duplicates Will Be Skipped)", rows.size(),
                adaptiveBatchSize);

        processAlertsInBatches(rows, jobContext, adaptiveBatchSize);

        logger.info("GenerateAlert Execution Completed Successfully! (Only New Alarms Were Inserted)");
        return this.dataFrame;
    }

    private void processAlertsInBatches(List<Row> rows, JobContext jobContext, int batchSize) {
        Connection connection = null;
        int totalNewInserts = 0;
        int totalDuplicatesSkipped = 0;
        int totalUpdates = 0;

        long overallStartTime = System.currentTimeMillis();
        long preloadStartTime = 0;
        long processingStartTime = 0;

        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        logger.info("Initial Memory Usage: {} MB", initialMemory / MEMORY_DIVISOR);

        try {
            connection = getDatabaseConnection("FMS", jobContext);
            connection.setAutoCommit(false);

            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), CONNECTION_TIMEOUT);

            preloadStartTime = System.currentTimeMillis();
            Map<String, AlarmInfo> existingAlarms;
            try {
                existingAlarms = preloadExistingAlarms(connection, rows);
            } catch (Exception e) {
                logger.info("Preload Failed Or Timed Out, Using Simple Insert Approach: {}", e.getMessage());
                simpleInsertAll(rows, connection);
                return;
            }

            if (!existingAlarms.isEmpty()) {
                logger.info("Found {} Existing Alarms - Using Proper Duplicate Checking", existingAlarms.size());
                try {
                    simplifiedProcessing(rows, existingAlarms, connection);
                    return;
                } catch (Exception e) {
                    logger.error("Optimized Processing Failed: {} - Falling Back To Simple Insert Approach",
                            e.getMessage());
                    logger.info("Falling Back To Simple Insert Approach To Avoid Timeout Issues");
                    simpleInsertAll(rows, connection);
                    return;
                }
            }

            logger.info("No Existing Alarms Found - Using Simple Insert Approach For New Data");

            int totalBatches = (rows.size() + batchSize - 1) / batchSize;
            int processedCount = 0;

            processingStartTime = System.currentTimeMillis();

            for (int i = 0; i < rows.size(); i += batchSize) {
                int endIndex = Math.min(i + batchSize, rows.size());
                List<Row> batch = rows.subList(i, endIndex);

                int batchNumber = (i / batchSize) + 1;
                long batchStartTime = System.currentTimeMillis();

                logger.info("Processing Batch {}/{} (Rows {}-{})", batchNumber, totalBatches, i + 1, endIndex);

                try {
                    BatchResult result = processBatch(batch, connection, existingAlarms, jobContext);
                    totalNewInserts += result.newInserts;
                    totalDuplicatesSkipped += result.duplicatesSkipped;
                    totalUpdates += result.updates;
                    processedCount += batch.size();

                    connection.commit();

                    long batchTime = System.currentTimeMillis() - batchStartTime;
                    long overallTime = System.currentTimeMillis() - overallStartTime;

                    logger.info("Batch {}/{} Completed In {}ms. Total Processed: {}/{} (Overall: {}ms)",
                            batchNumber, totalBatches, batchTime, processedCount, rows.size(), overallTime);

                    if (overallTime > OVERALL_TIMEOUT_WARNING) {
                        logger.info("Overall Processing Taking Longer Than Expected: {}ms For {} Rows", overallTime,
                                processedCount);
                    }

                } catch (Exception e) {
                    logger.error("Error Processing Batch {}/{}: {}", batchNumber, totalBatches, e.getMessage());
                    processedCount += batch.size();
                }
            }

            long totalTime = System.currentTimeMillis() - overallStartTime;
            long preloadTime = preloadStartTime > 0 ? preloadStartTime - overallStartTime : 0;
            long processingTime = processingStartTime > 0 ? System.currentTimeMillis() - processingStartTime : 0;

            long finalMemory = runtime.totalMemory() - runtime.freeMemory();
            long memoryUsed = finalMemory - initialMemory;
            logger.info("Final Memory Usage: {} MB (Change: {} MB)",
                    finalMemory / MEMORY_DIVISOR, memoryUsed / MEMORY_DIVISOR);

            logger.info(
                    "FINAL SUMMARY: {} New Alarms Inserted, {} Duplicates Skipped, {} Updates Out Of {} Total Rows",
                    totalNewInserts, totalDuplicatesSkipped, totalUpdates, rows.size());
            logger.info("PERFORMANCE: Total={}ms, Preload={}ms, Processing={}ms, Throughput={} Rows/Sec",
                    totalTime, preloadTime, processingTime,
                    totalTime > 0 ? (rows.size() * MILLISECONDS_PER_SECOND / totalTime) : 0);

            if (rows.size() > LARGE_DATASET_THRESHOLD) {
                logger.info("LARGE DATASET RECOMMENDATIONS:");
                logger.info("   - Consider Streaming Approach For 1M+ Records");
                logger.info("   - Monitor Memory Usage And Adjust Batch Sizes");
                logger.info("   - Consider Read Replicas For Heavy Read Operations");
            }

        } catch (Exception e) {
            logger.error("Error In Batch Processing: {}", e.getMessage(), e);
            if (connection != null) {
                try {
                    connection.rollback();
                    logger.info("Transaction Rolled Back Due To Error");
                } catch (SQLException rollbackEx) {
                    logger.error("Error During Rollback: {}", rollbackEx.getMessage());
                }
            }
            throw new RuntimeException("Failed To Process Alerts", e);
        } finally {
            closeConnection(connection);
        }
    }

    private Map<String, AlarmInfo> preloadExistingAlarms(Connection connection, List<Row> rows) {
        int estimatedSize = Math.min(rows.size(), PRELOAD_ESTIMATED_SIZE_CAP);
        Map<String, AlarmInfo> existingAlarms = new ConcurrentHashMap<>(estimatedSize);

        try {
            Set<String> uniqueKeys = new HashSet<>();
            for (Row row : rows) {
                try {
                    Map<String, String> alertMap = row.getJavaMap(row.fieldIndex("ALERT_MAP"));
                    if (alertMap == null) {
                        logger.info("Skipping Row With Null ALERT_MAP During Preload");
                        continue;
                    }

                    Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                    String key = buildAlarmKey(mutableAlertMap);
                    uniqueKeys.add(key);
                } catch (Exception e) {
                    logger.error("Error Processing Row During Preload: {}", e.getMessage());
                }
            }

            if (uniqueKeys.isEmpty()) {
                logger.info("No Unique Keys Found For Preloading");
                return existingAlarms;
            }

            logger.info("Found {} Unique Keys To Check Against Database", uniqueKeys.size());

            Set<String> alarmExternalIds = new HashSet<>();
            Set<String> alarmCodes = new HashSet<>();
            Set<String> subentities = new HashSet<>();
            Set<String> entityIds = new HashSet<>();
            Set<String> entityNames = new HashSet<>();
            Set<String> domains = new HashSet<>();
            Set<String> vendors = new HashSet<>();
            Set<String> technologies = new HashSet<>();

            if (uniqueKeys.size() > MAX_UNIQUE_KEYS_FOR_PRELOAD) {
                logger.info("Too Many Unique Keys ({}), Using Simple Insert Approach For Better Performance",
                        uniqueKeys.size());
                return existingAlarms;
            }

            for (String key : uniqueKeys) {
                String[] parts = key.split("\\|");
                if (parts.length >= EXACT_MATCH_KEY_PARTS) {
                    alarmExternalIds.add(parts[0]);
                    alarmCodes.add(parts[1]);
                    subentities.add(parts[2]);
                    entityIds.add(parts[3]);
                    entityNames.add(parts[4]);
                    domains.add(parts[5]);
                    vendors.add(parts[6]);
                    technologies.add(parts[7]);
                }
            }

            StringBuilder query = new StringBuilder();
            query.append(
                    "SELECT ALARM_EXTERNAL_ID, ALARM_CODE, SUBENTITY, ENTITY_ID, ENTITY_NAME, DOMAIN, VENDOR, TECHNOLOGY, OCCURRENCE, ALARM_STATUS, OPEN_TIME, CHANGE_TIME, PROBABLE_CAUSE ");
            query.append("FROM ALARM WHERE ");
            query.append("ALARM_EXTERNAL_ID IN (")
                    .append(String.join(",", Collections.nCopies(alarmExternalIds.size(), "?"))).append(") AND ");
            query.append("ALARM_CODE IN (").append(String.join(",", Collections.nCopies(alarmCodes.size(), "?")))
                    .append(") AND ");
            query.append("SUBENTITY IN (").append(String.join(",", Collections.nCopies(subentities.size(), "?")))
                    .append(") AND ");
            query.append("ENTITY_ID IN (").append(String.join(",", Collections.nCopies(entityIds.size(), "?")))
                    .append(") AND ");
            query.append("ENTITY_NAME IN (").append(String.join(",", Collections.nCopies(entityNames.size(), "?")))
                    .append(") AND ");
            query.append("DOMAIN IN (").append(String.join(",", Collections.nCopies(domains.size(), "?")))
                    .append(") AND ");
            query.append("VENDOR IN (").append(String.join(",", Collections.nCopies(vendors.size(), "?")))
                    .append(") AND ");
            query.append("TECHNOLOGY IN (").append(String.join(",", Collections.nCopies(technologies.size(), "?")))
                    .append(")");

            try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
                stmt.setQueryTimeout(PRELOAD_QUERY_TIMEOUT);

                int paramIndex = 1;
                for (String id : alarmExternalIds)
                    stmt.setString(paramIndex++, id);
                for (String code : alarmCodes)
                    stmt.setString(paramIndex++, code);
                for (String subentity : subentities)
                    stmt.setString(paramIndex++, subentity);
                for (String entityId : entityIds)
                    stmt.setString(paramIndex++, entityId);
                for (String entityName : entityNames)
                    stmt.setString(paramIndex++, entityName);
                for (String domain : domains)
                    stmt.setString(paramIndex++, domain);
                for (String vendor : vendors)
                    stmt.setString(paramIndex++, vendor);
                for (String technology : technologies)
                    stmt.setString(paramIndex++, technology);

                logger.info("Executing Single Optimized Query For {} Unique Keys...", uniqueKeys.size());

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String key = buildKeyFromResultSet(rs);
                        int occurrence = rs.getInt("OCCURRENCE");
                        String status = rs.getString("ALARM_STATUS");
                        String openTime = rs.getString("OPEN_TIME");
                        String changeTime = rs.getString("CHANGE_TIME");
                        existingAlarms.put(key, new AlarmInfo(occurrence, status, openTime, changeTime));
                    }
                }

                logger.info("Single Query Completed - Found {} Existing Alarms", existingAlarms.size());
            }

            logger.info("Preloaded {} Existing Alarms (OPEN And CLOSED) For Batch Processing",
                    existingAlarms.size());

        } catch (SQLException e) {
            logger.error("Error Preloading Existing Alarms: {}", e.getMessage(), e);
            throw new RuntimeException("Failed To Preload Existing Alarms", e);
        }

        return existingAlarms;
    }

    /**
     * Simple Insert Approach - ONLY For Truly New Data With No Existing Alarms
     * This Method Bypasses Duplicate Checking And Should Only Be Used When:
     * 1. Preload Failed/Timed Out
     * 2. No Existing Alarms Were Found In The Database
     */
    private void simpleInsertAll(List<Row> rows, Connection connection) {
        logger.info("Using Optimized Simple Insert Approach For {} Rows (NEW DATA ONLY)", rows.size());

        long overallStartTime = System.currentTimeMillis();

        int batchSize = SIMPLE_INSERT_BATCH_SIZE;
        int totalInserted = 0;

        for (int i = 0; i < rows.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, rows.size());
            List<Row> batch = rows.subList(i, endIndex);

            List<Map<String, String>> insertBatch = new ArrayList<>();

            for (Row row : batch) {
                try {
                    Map<String, String> alertMap = row.getJavaMap(row.fieldIndex("ALERT_MAP"));
                    if (alertMap != null) {
                        Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                        mutableAlertMap.put("OCCURRENCE", DEFAULT_OCCURRENCE);
                        insertBatch.add(mutableAlertMap);
                    }
                } catch (Exception e) {
                    logger.error("Error Processing Row For Simple Insert: {}", e.getMessage());
                }
            }

            if (!insertBatch.isEmpty()) {
                try {
                    batchInsertAlarms(insertBatch, connection);
                    connection.commit();
                    totalInserted += insertBatch.size();
                    logger.info("Simple Insert Batch {}/{} Completed: {} Alarms",
                            (i / batchSize) + 1, (rows.size() + batchSize - 1) / batchSize, insertBatch.size());
                } catch (Exception e) {
                    logger.error("Error In Simple Insert Batch: {}", e.getMessage());
                    try {
                        connection.rollback();
                    } catch (SQLException rollbackEx) {
                        logger.error("Error During Rollback: {}", rollbackEx.getMessage());
                    }
                }
            }
        }

        long totalTime = System.currentTimeMillis() - overallStartTime;
        logger.info("SIMPLE INSERT: Total={}ms, Throughput={} Rows/Sec",
                totalTime, totalTime > 0 ? (rows.size() * MILLISECONDS_PER_SECOND / totalTime) : 0);
        logger.info("Optimized Simple Insert Completed: {} Alarms Inserted Out Of {} Rows", totalInserted,
                rows.size());
    }

    private void simplifiedProcessing(List<Row> rows, Map<String, AlarmInfo> existingAlarms, Connection connection) {
        logger.info("Using Optimized Processing Approach For {} Rows With {} Existing Alarms", rows.size(),
                existingAlarms.size());

        long overallStartTime = System.currentTimeMillis();
        long categorizationStartTime = 0;
        long insertStartTime = 0;
        long updateStartTime = 0;

        int totalSkipped = 0;
        int totalUpdated = 0;
        int totalInserted = 0;

        int batchSize = OPTIMIZED_BATCH_SIZE;

        categorizationStartTime = System.currentTimeMillis();

        for (int i = 0; i < rows.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, rows.size());
            List<Row> batch = rows.subList(i, endIndex);

            logger.info("Processing Optimized Batch {}/{} (Rows {}-{})",
                    (i / batchSize) + 1, (rows.size() + batchSize - 1) / batchSize, i + 1, endIndex);

            List<Map<String, String>> inserts = new ArrayList<>();
            List<Map<String, String>> updates = new ArrayList<>();

            for (Row row : batch) {
                try {
                    Map<String, String> alertMap = row.getJavaMap(row.fieldIndex("ALERT_MAP"));
                    if (alertMap == null)
                        continue;

                    Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                    String alarmKey = buildAlarmKey(mutableAlertMap);
                    String exactMatchKey = buildExactMatchKey(mutableAlertMap);

                    if (existingAlarms.containsKey(alarmKey)) {
                        AlarmInfo existingAlarm = existingAlarms.get(alarmKey);

                        String newOpenTime = mutableAlertMap.get("OPEN_TIME");
                        String newChangeTime = mutableAlertMap.get("CHANGE_TIME");

                        boolean isExactMatch = false;

                        if (existingAlarm.openTime.equals(newOpenTime)
                                && existingAlarm.changeTime.equals(newChangeTime)) {
                            isExactMatch = true;
                        } else {
                            String normalizedExistingOpen = normalizeTimestamp(existingAlarm.openTime);
                            String normalizedNewOpen = normalizeTimestamp(newOpenTime);
                            String normalizedExistingChange = normalizeTimestamp(existingAlarm.changeTime);
                            String normalizedNewChange = normalizeTimestamp(newChangeTime);

                            isExactMatch = normalizedExistingOpen.equals(normalizedNewOpen) &&
                                    normalizedExistingChange.equals(normalizedNewChange);
                        }

                        if (existingAlarms.containsKey(alarmKey)
                                && totalSkipped + totalUpdated + totalInserted < DEBUG_LOG_LIMIT) {
                            logger.info(
                                    "Exact Match Check For {}: Existing_Open='{}' Vs New_Open='{}', Existing_Change='{}' Vs New_Change='{}', Match={}",
                                    alarmKey, existingAlarm.openTime, newOpenTime, existingAlarm.changeTime,
                                    newChangeTime, isExactMatch);
                        }

                        if (isExactMatch) {
                            totalSkipped++;
                            if (totalSkipped <= DEBUG_LOG_FIRST_FEW) {
                                logger.info("Skipping Exact Match: {}", exactMatchKey);
                            }
                        } else if (ALARM_STATUS_OPEN.equals(existingAlarm.status)) {
                            updates.add(mutableAlertMap);
                            if (totalUpdated < DEBUG_LOG_FIRST_FEW) {
                                logger.info("Will Update OPEN Alarm: {}", alarmKey);
                            }
                        } else {
                            inserts.add(mutableAlertMap);
                            if (totalInserted < DEBUG_LOG_FIRST_FEW) {
                                logger.info("Will Insert New Alarm (Existing Was CLOSED): {}", alarmKey);
                            }
                        }
                    } else {
                        inserts.add(mutableAlertMap);
                        if (totalInserted < DEBUG_LOG_FIRST_FEW) {
                            logger.info("Will Insert New Alarm: {}", alarmKey);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error In Optimized Processing: {}", e.getMessage());
                }
            }

            logger.info("Batch Categorization: {} Inserts, {} Updates, {} Skips",
                    inserts.size(), updates.size(), batch.size() - inserts.size() - updates.size());

            if (!inserts.isEmpty()) {
                insertStartTime = System.currentTimeMillis();
                try {
                    batchInsertAlarms(inserts, connection);
                    totalInserted += inserts.size();
                    long insertTime = System.currentTimeMillis() - insertStartTime;
                    logger.info("Batch Insert Completed: {} Alarms In {}ms", inserts.size(), insertTime);
                } catch (Exception e) {
                    logger.error("Error In Batch Insert: {}", e.getMessage());
                }
            }

            if (!updates.isEmpty()) {
                updateStartTime = System.currentTimeMillis();
                try {
                    // Group records by ADDITIONAL_DETAIL to preserve individual values
                    Map<String, List<Map<String, String>>> groupedByAdditionalDetail = new HashMap<>();
                    
                    for (Map<String, String> alertMap : updates) {
                        String additionalDetail = alertMap.get("ADDITIONAL_DETAIL");
                        if (additionalDetail == null) {
                            additionalDetail = ""; // Handle null values
                        }
                        groupedByAdditionalDetail.computeIfAbsent(additionalDetail, k -> new ArrayList<>()).add(alertMap);
                    }
                    
                    logger.info("Grouped {} update records into {} groups by ADDITIONAL_DETAIL for bulk updates", 
                               updates.size(), groupedByAdditionalDetail.size());
                    
                    for (Map.Entry<String, List<Map<String, String>>> entry : groupedByAdditionalDetail.entrySet()) {
                        String additionalDetail = entry.getKey();
                        List<Map<String, String>> group = entry.getValue();
                        
                        logger.info("Processing bulk update for group with ADDITIONAL_DETAIL='{}' containing {} alarms", 
                                   additionalDetail.isEmpty() ? "NULL/EMPTY" : additionalDetail, group.size());
                        
                        try {
                            int updateBatchSize = UPDATE_BATCH_SIZE;
                            for (int j = 0; j < group.size(); j += updateBatchSize) {
                                int updateEndIndex = Math.min(j + updateBatchSize, group.size());
                                List<Map<String, String>> updateBatch = group.subList(j, updateEndIndex);

                                logger.info("Processing Bulk Update Batch {}/{} With {} Items",
                                        (j / updateBatchSize) + 1, (group.size() + updateBatchSize - 1) / updateBatchSize,
                                        updateBatch.size());

                                String bulkUpdateQuery = buildBulkUpdateQuery(updateBatch);

                                try (PreparedStatement stmt = connection.prepareStatement(bulkUpdateQuery)) {
                                    stmt.setQueryTimeout(BULK_UPDATE_TIMEOUT);

                                    int paramIndex = 1;

                                    String commonChangeTime = updateBatch.get(0).get("CHANGE_TIME");
                                    String commonProbableCause = updateBatch.get(0).get("PROBABLE_CAUSE");
                                    String commonDescription = updateBatch.get(0).get("DESCRIPTION");
                                    String commonAdditionalDetail = updateBatch.get(0).get("ADDITIONAL_DETAIL");

                                    stmt.setString(paramIndex++, generateProcessingTime());
                                    stmt.setString(paramIndex++, commonChangeTime);
                                    stmt.setString(paramIndex++, commonProbableCause);
                                    stmt.setString(paramIndex++, commonDescription);
                                    stmt.setString(paramIndex++, commonAdditionalDetail);

                                    Set<String> alarmExternalIds = new HashSet<>();
                                    Set<String> alarmCodes = new HashSet<>();
                                    Set<String> subentities = new HashSet<>();
                                    Set<String> entityIds = new HashSet<>();
                                    Set<String> entityNames = new HashSet<>();
                                    Set<String> domains = new HashSet<>();
                                    Set<String> vendors = new HashSet<>();
                                    Set<String> technologies = new HashSet<>();

                                    for (Map<String, String> alertMap : updateBatch) {
                                        alarmExternalIds.add(alertMap.get("ALARM_EXTERNAL_ID"));
                                        alarmCodes.add(alertMap.get("ALARM_CODE"));
                                        subentities.add(alertMap.get("SUBENTITY"));
                                        entityIds.add(alertMap.get("ENTITY_ID"));
                                        entityNames.add(alertMap.get("ENTITY_NAME"));
                                        domains.add(alertMap.get("DOMAIN"));
                                        vendors.add(alertMap.get("VENDOR"));
                                        technologies.add(alertMap.get("TECHNOLOGY"));
                                    }

                                    for (String id : alarmExternalIds)
                                        stmt.setString(paramIndex++, id);
                                    for (String code : alarmCodes)
                                        stmt.setString(paramIndex++, code);
                                    for (String subentity : subentities)
                                        stmt.setString(paramIndex++, subentity);
                                    for (String entityId : entityIds)
                                        stmt.setString(paramIndex++, entityId);
                                    for (String entityName : entityNames)
                                        stmt.setString(paramIndex++, entityName);
                                    for (String domain : domains)
                                        stmt.setString(paramIndex++, domain);
                                    for (String vendor : vendors)
                                        stmt.setString(paramIndex++, vendor);
                                    for (String technology : technologies)
                                        stmt.setString(paramIndex++, technology);

                                    long startTime = System.currentTimeMillis();
                                    int affectedRows = stmt.executeUpdate();
                                    long updateTime = System.currentTimeMillis() - startTime;

                                    logger.info("Bulk Update Batch Completed: {} Alarms Updated In {}ms", affectedRows,
                                            updateTime);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Error in bulk update for group with ADDITIONAL_DETAIL='{}': {}", 
                                       additionalDetail.isEmpty() ? "NULL/EMPTY" : additionalDetail, e.getMessage());
                            // Fallback to individual updates for this group
                            logger.info("Falling back to individual updates for group with {} alarms", group.size());
                            performIndividualUpdates(group, connection);
                        }
                    }

                    long totalUpdateTime = System.currentTimeMillis() - updateStartTime;
                    logger.info("UPDATE OPERATIONS: Total Update Time={}ms, Throughput={} Rows/Sec",
                            totalUpdateTime, totalUpdateTime > 0 ? (updates.size() * MILLISECONDS_PER_SECOND / totalUpdateTime) : 0);

                } catch (Exception e) {
                    logger.error("Error In Bulk Update: {}", e.getMessage());
                    logger.info("Falling Back To Individual Updates...");
                    performIndividualUpdates(updates, connection);
                }
            }

            try {
                connection.commit();
                logger.info("Optimized Batch {}/{} Completed: {} Inserts, {} Updates, {} Skipped",
                        (i / batchSize) + 1, (rows.size() + batchSize - 1) / batchSize,
                        inserts.size(), updates.size(), batch.size() - inserts.size() - updates.size());
            } catch (SQLException e) {
                logger.error("Error Committing Optimized Batch: {}", e.getMessage());
            }
        }

        long totalTime = System.currentTimeMillis() - overallStartTime;
        long categorizationTime = categorizationStartTime > 0 ? System.currentTimeMillis() - categorizationStartTime
                : 0;

        Runtime runtime = Runtime.getRuntime();
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryUsed = finalMemory / MEMORY_DIVISOR;

        logger.info("Memory Usage: {} MB", memoryUsed);
        logger.info("DUPLICATE DETECTION: Categorization={}ms, Total={}ms, Throughput={} Rows/Sec",
                categorizationTime, totalTime, totalTime > 0 ? (rows.size() * MILLISECONDS_PER_SECOND / totalTime) : 0);
        logger.info("Optimized Processing Completed: {} Inserts, {} Updates, {} Skipped Out Of {} Total Rows",
                totalInserted, totalUpdated, totalSkipped, rows.size());
    }

    private BatchResult processBatch(List<Row> batch, Connection connection, Map<String, AlarmInfo> existingAlarms,
            JobContext jobContext) {
        List<Map<String, String>> inserts = new ArrayList<>();
        List<Map<String, String>> skippedDuplicates = new ArrayList<>();
        List<Map<String, String>> updates = new ArrayList<>();

        logger.info("Starting Categorization Of {} Rows In Batch", batch.size());

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < batch.size(); i++) {
            Row row = batch.get(i);

            if (i % PROGRESS_LOG_INTERVAL == 0 && i > 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                logger.info("Processed {}/{} Rows In Current Batch ({}ms Elapsed)", i, batch.size(), elapsed);

                if (elapsed > BATCH_TIMEOUT_WARNING) {
                    logger.info("Batch Processing Taking Longer Than Expected: {}ms For {} Rows", elapsed, i);
                }
            }

            try {
                Map<String, String> alertMap = row.getJavaMap(row.fieldIndex("ALERT_MAP"));
                if (alertMap == null) {
                    logger.info("Skipping Row With Null ALERT_MAP");
                    continue;
                }

                Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                String key = buildAlarmKey(mutableAlertMap);

                if (existingAlarms.containsKey(key)) {
                    AlarmInfo existingAlarm = existingAlarms.get(key);
                    String newOpenTime = mutableAlertMap.get("OPEN_TIME");
                    String newChangeTime = mutableAlertMap.get("CHANGE_TIME");

                    boolean isExactMatch = false;

                    if (existingAlarm.openTime.equals(newOpenTime) && existingAlarm.changeTime.equals(newChangeTime)) {
                        isExactMatch = true;
                    } else {
                        String normalizedExistingOpen = normalizeTimestamp(existingAlarm.openTime);
                        String normalizedNewOpen = normalizeTimestamp(newOpenTime);
                        String normalizedExistingChange = normalizeTimestamp(existingAlarm.changeTime);
                        String normalizedNewChange = normalizeTimestamp(newChangeTime);

                        isExactMatch = normalizedExistingOpen.equals(normalizedNewOpen)
                                && normalizedExistingChange.equals(normalizedNewChange);
                    }

                    if (i < 3) {
                        logger.info(
                                "Exact Match Check In ProcessBatch For {}: Existing_Open='{}' Vs New_Open='{}', Existing_Change='{}' Vs New_Change='{}', Match={}",
                                key, existingAlarm.openTime, newOpenTime, existingAlarm.changeTime, newChangeTime,
                                isExactMatch);
                    }

                    if (isExactMatch) {
                        skippedDuplicates.add(mutableAlertMap);
                        if (i < DEBUG_LOG_FIRST_FEW) {
                            logger.info("Skipping Exact Match In ProcessBatch: {}", key);
                        }
                    } else if (ALARM_STATUS_OPEN.equals(existingAlarm.status)) {
                        updates.add(mutableAlertMap);
                    } else {
                        inserts.add(mutableAlertMap);
                    }
                } else {
                    inserts.add(mutableAlertMap);
                }
            } catch (Exception e) {
                logger.error("Error Processing Row {}: {}", i, e.getMessage());
            }
        }

        long categorizationTime = System.currentTimeMillis() - startTime;
        logger.info("Completed Categorization In {}ms: {} Inserts, {} Skips, {} Updates",
                categorizationTime, inserts.size(), skippedDuplicates.size(), updates.size());

        if (!inserts.isEmpty()) {
            try {
                batchInsertAlarms(inserts, connection);
            } catch (Exception e) {
                logger.error("Error In Batch Insert: {}", e.getMessage());
            }
        }

        if (!updates.isEmpty()) {
            try {
                long updateStartTime = System.currentTimeMillis();
                batchUpdateAlarms(updates, existingAlarms, connection);
                long updateTime = System.currentTimeMillis() - updateStartTime;

                if (updateTime > UPDATE_TIMEOUT_WARNING) {
                    logger.info("Update Operation Took Longer Than Expected: {}ms", updateTime);
                }
            } catch (Exception e) {
                logger.error("Error In Batch Update: {}", e.getMessage());
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Batch Processed In {}ms: {} New Inserts, {} Duplicates Skipped, {} Updates",
                totalTime, inserts.size(), skippedDuplicates.size(), updates.size());

        return new BatchResult(inserts.size(), skippedDuplicates.size(), updates.size());
    }

    private void batchInsertAlarms(List<Map<String, String>> alertMaps, Connection connection) {
        if (alertMaps.isEmpty())
            return;

        String sqlQuery = "INSERT INTO ALARM (OPEN_TIME, CHANGE_TIME, REPORTING_TIME, FIRST_RECEPTION_TIME, FIRST_PROCESSING_TIME, LAST_PROCESSING_TIME, ALARM_EXTERNAL_ID, ALARM_CODE, ALARM_NAME, CLASSIFICATION, EVENT_TYPE, SEVERITY, ACTUAL_SEVERITY, ALARM_STATUS, ENTITY_NAME, ENTITY_ID, SUBENTITY, ENTITY_TYPE, ENTITY_STATUS, LATITUDE, LONGITUDE, SENDER_NAME, SENDER_IP, GEOGRAPHY_L1_NAME, GEOGRAPHY_L2_NAME, GEOGRAPHY_L3_NAME, GEOGRAPHY_L4_NAME, DOMAIN, VENDOR, TECHNOLOGY, PROBABLE_CAUSE, DESCRIPTION, SERVICE_AFFECTED, MANUALLY_CLOSEABLE, CORRELATION_FLAG, OCCURRENCE, ADDITIONAL_DETAIL, ALARM_GROUP) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.setQueryTimeout(BATCH_INSERT_TIMEOUT);

            for (Map<String, String> alertMap : alertMaps) {
                Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                mutableAlertMap.put("OCCURRENCE", DEFAULT_OCCURRENCE);
                setInsertParameters(preparedStatement, mutableAlertMap);
                preparedStatement.addBatch();
            }

            logger.info("Executing Optimized Batch Insert For {} Alarms...", alertMaps.size());
            int[] results = preparedStatement.executeBatch();
            int successCount = 0;
            for (int result : results) {
                if (result > 0)
                    successCount++;
            }

            logger.info("Optimized Batch Inserted {} Alarms Successfully", successCount);

        } catch (SQLException e) {
            logger.error("Error In Batch Insert: {}", e.getMessage(), e);
            throw new RuntimeException("Failed To Batch Insert Alarms", e);
        }
    }

    private void batchUpdateAlarms(List<Map<String, String>> alertMaps, Map<String, AlarmInfo> existingAlarms,
            Connection connection) {
        if (alertMaps.isEmpty())
            return;

        logger.info("Starting Batch Update For {} Alarms...", alertMaps.size());
        long startTime = System.currentTimeMillis();

        // Group records by ADDITIONAL_DETAIL to preserve individual values
        Map<String, List<Map<String, String>>> groupedByAdditionalDetail = new HashMap<>();
        
        for (Map<String, String> alertMap : alertMaps) {
            String additionalDetail = alertMap.get("ADDITIONAL_DETAIL");
            if (additionalDetail == null) {
                additionalDetail = ""; // Handle null values
            }
            groupedByAdditionalDetail.computeIfAbsent(additionalDetail, k -> new ArrayList<>()).add(alertMap);
        }
        
        logger.info("Grouped {} update records into {} groups by ADDITIONAL_DETAIL for batch updates", 
                   alertMaps.size(), groupedByAdditionalDetail.size());
        
        int totalUpdated = 0;
        for (Map.Entry<String, List<Map<String, String>>> entry : groupedByAdditionalDetail.entrySet()) {
            String additionalDetail = entry.getKey();
            List<Map<String, String>> group = entry.getValue();
            
            logger.info("Processing batch update for group with ADDITIONAL_DETAIL='{}' containing {} alarms", 
                       additionalDetail.isEmpty() ? "NULL/EMPTY" : additionalDetail, group.size());
            
            try {
                int updateBatchSize = UPDATE_BATCH_SIZE_FALLBACK;

                for (int i = 0; i < group.size(); i += updateBatchSize) {
                    int endIndex = Math.min(i + updateBatchSize, group.size());
                    List<Map<String, String>> batch = group.subList(i, endIndex);

                    String sqlQuery = "UPDATE ALARM SET OCCURRENCE = ?, LAST_PROCESSING_TIME = ?, CHANGE_TIME = ?, PROBABLE_CAUSE = ?, DESCRIPTION = ?, ADDITIONAL_DETAIL = ? "
                            +
                            "WHERE ALARM_STATUS = 'OPEN' AND ALARM_EXTERNAL_ID = ? AND ALARM_CODE = ? AND SUBENTITY = ? AND ENTITY_ID = ? AND ENTITY_NAME = ? "
                            +
                            "AND DOMAIN = ? AND VENDOR = ? AND TECHNOLOGY = ?";

                    try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                        preparedStatement.setQueryTimeout(UPDATE_BATCH_TIMEOUT);

                        for (Map<String, String> alertMap : batch) {
                            String key = buildAlarmKey(alertMap);
                            AlarmInfo existingAlarm = existingAlarms.get(key);

                            setUpdateParameters(preparedStatement, alertMap, existingAlarm.occurrence + 1);
                            preparedStatement.addBatch();
                        }

                        logger.info("Executing Update Batch {}/{} For {} Alarms...",
                                (i / updateBatchSize) + 1, (group.size() + updateBatchSize - 1) / updateBatchSize,
                                batch.size());

                        int[] results = preparedStatement.executeBatch();
                        int successCount = 0;
                        for (int result : results) {
                            if (result > 0)
                                successCount++;
                        }
                        totalUpdated += successCount;

                        logger.info("Update Batch Completed: {} Alarms Updated", successCount);
                    }

                }
            } catch (SQLException e) {
                logger.error("Error In Update Batch For Group With ADDITIONAL_DETAIL='{}': {}",
                           additionalDetail.isEmpty() ? "NULL/EMPTY" : additionalDetail, e.getMessage());
            }
        }

        long updateTime = System.currentTimeMillis() - startTime;
        logger.info("Batch Update Completed In {}ms: {} Alarms Updated Successfully", updateTime, totalUpdated);
    }

    private String buildAlarmKey(Map<String, String> alertMap) {
        if (alertMap == null) {
            throw new IllegalArgumentException("Alert Map Cannot Be Null");
        }
        return String.join("|",
                alertMap.getOrDefault("ALARM_EXTERNAL_ID", ""),
                alertMap.getOrDefault("ALARM_CODE", ""),
                alertMap.getOrDefault("SUBENTITY", ""),
                alertMap.getOrDefault("ENTITY_ID", ""),
                alertMap.getOrDefault("ENTITY_NAME", ""),
                alertMap.getOrDefault("DOMAIN", ""),
                alertMap.getOrDefault("VENDOR", ""),
                alertMap.getOrDefault("TECHNOLOGY", ""));
    }

    private String buildExactMatchKey(Map<String, String> alertMap) {
        if (alertMap == null) {
            throw new IllegalArgumentException("Alert Map Cannot Be Null");
        }
        return String.join("|",
                alertMap.getOrDefault("OPEN_TIME", ""),
                alertMap.getOrDefault("CHANGE_TIME", ""),
                alertMap.getOrDefault("ALARM_EXTERNAL_ID", ""),
                alertMap.getOrDefault("ALARM_CODE", ""),
                alertMap.getOrDefault("ENTITY_ID", ""),
                alertMap.getOrDefault("ENTITY_NAME", ""),
                alertMap.getOrDefault("SUBENTITY", ""));
    }

    private String buildKeyFromResultSet(ResultSet rs) throws SQLException {
        return String.join("|",
                rs.getString("ALARM_EXTERNAL_ID"),
                rs.getString("ALARM_CODE"),
                rs.getString("SUBENTITY"),
                rs.getString("ENTITY_ID"),
                rs.getString("ENTITY_NAME"),
                rs.getString("DOMAIN"),
                rs.getString("VENDOR"),
                rs.getString("TECHNOLOGY"));
    }

    private void setInsertParameters(PreparedStatement stmt, Map<String, String> alertMap) throws SQLException {
        if (alertMap == null) {
            throw new IllegalArgumentException("Alert Map Cannot Be Null");
        }

        int index = 1;
        stmt.setString(index++, alertMap.get("OPEN_TIME"));
        stmt.setString(index++, alertMap.get("CHANGE_TIME"));
        stmt.setString(index++, alertMap.get("REPORTING_TIME"));
        stmt.setString(index++, alertMap.get("FIRST_RECEPTION_TIME"));
        stmt.setString(index++, alertMap.get("FIRST_PROCESSING_TIME"));
        stmt.setString(index++, alertMap.get("LAST_PROCESSING_TIME"));
        stmt.setString(index++, alertMap.get("ALARM_EXTERNAL_ID"));
        stmt.setString(index++, alertMap.get("ALARM_CODE"));
        stmt.setString(index++, alertMap.get("ALARM_NAME"));
        stmt.setString(index++, alertMap.get("CLASSIFICATION"));
        stmt.setString(index++, alertMap.get("EVENT_TYPE"));
        stmt.setString(index++, alertMap.get("SEVERITY"));
        stmt.setString(index++, alertMap.get("ACTUAL_SEVERITY"));
        stmt.setString(index++, alertMap.get("ALARM_STATUS"));
        stmt.setString(index++, alertMap.get("ENTITY_NAME"));
        stmt.setString(index++, alertMap.get("ENTITY_ID"));
        stmt.setString(index++, alertMap.get("SUBENTITY"));
        stmt.setString(index++, alertMap.get("ENTITY_TYPE"));
        stmt.setString(index++, alertMap.get("ENTITY_STATUS"));
        stmt.setString(index++, alertMap.get("LATITUDE"));
        stmt.setString(index++, alertMap.get("LONGITUDE"));
        stmt.setString(index++, alertMap.get("SENDER_NAME"));
        stmt.setString(index++, alertMap.get("SENDER_IP"));
        stmt.setString(index++, alertMap.get("GEOGRAPHY_L1_NAME"));
        stmt.setString(index++, alertMap.get("GEOGRAPHY_L2_NAME"));
        stmt.setString(index++, alertMap.get("GEOGRAPHY_L3_NAME"));
        stmt.setString(index++, alertMap.get("GEOGRAPHY_L4_NAME"));
        stmt.setString(index++, alertMap.get("DOMAIN"));
        stmt.setString(index++, alertMap.get("VENDOR"));
        stmt.setString(index++, alertMap.get("TECHNOLOGY"));
        stmt.setString(index++, alertMap.get("PROBABLE_CAUSE"));
        stmt.setString(index++, alertMap.get("DESCRIPTION"));
        stmt.setString(index++, alertMap.get("SERVICE_AFFECTED"));
        stmt.setString(index++, alertMap.get("MANUALLY_CLOSEABLE"));
        stmt.setString(index++, alertMap.get("CORRELATION_FLAG"));
        stmt.setString(index++, alertMap.get("OCCURRENCE"));
        stmt.setString(index++, alertMap.get("ADDITIONAL_DETAIL"));
        stmt.setString(index++, alertMap.get("ALARM_GROUP"));
    }

    private void setUpdateParameters(PreparedStatement stmt, Map<String, String> alertMap, int newOccurrence)
            throws SQLException {
        if (alertMap == null) {
            throw new IllegalArgumentException("Alert Map Cannot Be Null");
        }

        int index = 1;
        stmt.setInt(index++, newOccurrence);
        stmt.setString(index++, generateProcessingTime());
        stmt.setString(index++, alertMap.get("CHANGE_TIME"));
        stmt.setString(index++, alertMap.get("PROBABLE_CAUSE"));
        stmt.setString(index++, alertMap.get("DESCRIPTION"));
        stmt.setString(index++, alertMap.get("ADDITIONAL_DETAIL"));
        stmt.setString(index++, alertMap.get("ALARM_EXTERNAL_ID"));
        stmt.setString(index++, alertMap.get("ALARM_CODE"));
        stmt.setString(index++, alertMap.get("SUBENTITY"));
        stmt.setString(index++, alertMap.get("ENTITY_ID"));
        stmt.setString(index++, alertMap.get("ENTITY_NAME"));
        stmt.setString(index++, alertMap.get("DOMAIN"));
        stmt.setString(index++, alertMap.get("VENDOR"));
        stmt.setString(index++, alertMap.get("TECHNOLOGY"));
    }

    private static String generateProcessingTime() {
        return TIMESTAMP_FORMAT.format(new Date());
    }

    /**
     * Calculate Adaptive Batch Size Based On Dataset Size.
     * Larger Datasets Get Larger Batches For Better Performance.
     * 
     * @param totalRows The Total Number Of Rows To Process
     * @return Optimal Batch Size For The Given Dataset Size
     */
    private int calculateAdaptiveBatchSize(int totalRows) {
        if (totalRows <= SMALL_DATASET_THRESHOLD) {
            return MIN_BATCH_SIZE;
        } else if (totalRows <= MEDIUM_DATASET_THRESHOLD) {
            return BATCH_SIZE;
        } else if (totalRows <= LARGE_DATASET_THRESHOLD_FOR_BATCH) {
            return LARGE_BATCH_SIZE;
        } else {
            return MAX_BATCH_SIZE;
        }
    }

    /**
     * Normalize Timestamp By Removing Milliseconds Precision Differences
     * Converts '2025-01-16 00:00:00.0' And '2025-01-16 00:00:00.000' To
     * '2025-01-16 00:00:00'
     */
    private static String normalizeTimestamp(String timestamp) {
        if (timestamp == null || timestamp.trim().isEmpty()) {
            return timestamp;
        }

        // Remove Milliseconds Part (Everything After The Last Dot In Seconds)
        int lastDotIndex = timestamp.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return timestamp.substring(0, lastDotIndex);
        }

        return timestamp;
    }

    /**
     * Build Bulk UPDATE Query Using IN Clauses For Better Performance
     * This Reduces Round Trips From N To 1 For Each Batch
     */
    private String buildBulkUpdateQuery(List<Map<String, String>> updateBatch) {
        // Extract Unique Values For Each Field To Build Efficient IN Queries
        Set<String> alarmExternalIds = new HashSet<>();
        Set<String> alarmCodes = new HashSet<>();
        Set<String> subentities = new HashSet<>();
        Set<String> entityIds = new HashSet<>();
        Set<String> entityNames = new HashSet<>();
        Set<String> domains = new HashSet<>();
        Set<String> vendors = new HashSet<>();
        Set<String> technologies = new HashSet<>();

        for (Map<String, String> alertMap : updateBatch) {
            alarmExternalIds.add(alertMap.get("ALARM_EXTERNAL_ID"));
            alarmCodes.add(alertMap.get("ALARM_CODE"));
            subentities.add(alertMap.get("SUBENTITY"));
            entityIds.add(alertMap.get("ENTITY_ID"));
            entityNames.add(alertMap.get("ENTITY_NAME"));
            domains.add(alertMap.get("DOMAIN"));
            vendors.add(alertMap.get("VENDOR"));
            technologies.add(alertMap.get("TECHNOLOGY"));
        }

        StringBuilder query = new StringBuilder();
        query.append("UPDATE ALARM SET ");
        query.append("OCCURRENCE = OCCURRENCE + 1, ");
        query.append("LAST_PROCESSING_TIME = ?, ");
        query.append("CHANGE_TIME = ?, ");
        query.append("PROBABLE_CAUSE = ?, ");
        query.append("DESCRIPTION = ?, ");
        query.append("ADDITIONAL_DETAIL = ? ");
        query.append("WHERE ALARM_STATUS = 'OPEN' AND ");
        query.append("ALARM_EXTERNAL_ID IN (")
                .append(String.join(",", Collections.nCopies(alarmExternalIds.size(), "?"))).append(") AND ");
        query.append("ALARM_CODE IN (").append(String.join(",", Collections.nCopies(alarmCodes.size(), "?")))
                .append(") AND ");
        query.append("SUBENTITY IN (").append(String.join(",", Collections.nCopies(subentities.size(), "?")))
                .append(") AND ");
        query.append("ENTITY_ID IN (").append(String.join(",", Collections.nCopies(entityIds.size(), "?")))
                .append(") AND ");
        query.append("ENTITY_NAME IN (").append(String.join(",", Collections.nCopies(entityNames.size(), "?")))
                .append(") AND ");
        query.append("DOMAIN IN (").append(String.join(",", Collections.nCopies(domains.size(), "?"))).append(") AND ");
        query.append("VENDOR IN (").append(String.join(",", Collections.nCopies(vendors.size(), "?"))).append(") AND ");
        query.append("TECHNOLOGY IN (").append(String.join(",", Collections.nCopies(technologies.size(), "?")))
                .append(")");

        return query.toString();
    }

    /**
     * Fallback Method For Individual Updates If Bulk Update Fails
     */
    private void performIndividualUpdates(List<Map<String, String>> updates, Connection connection) {
        logger.info("Performing Individual Updates For {} Alarms", updates.size());

        int batchSize = INDIVIDUAL_UPDATE_BATCH_SIZE;
        int totalUpdated = 0;

        for (int i = 0; i < updates.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, updates.size());
            List<Map<String, String>> batch = updates.subList(i, endIndex);

            String sqlQuery = "UPDATE ALARM SET OCCURRENCE = OCCURRENCE + 1, LAST_PROCESSING_TIME = ?, CHANGE_TIME = ?, PROBABLE_CAUSE = ?, DESCRIPTION = ?, ADDITIONAL_DETAIL = ? "
                    +
                    "WHERE ALARM_STATUS = 'OPEN' AND ALARM_EXTERNAL_ID = ? AND ALARM_CODE = ? AND SUBENTITY = ? AND ENTITY_ID = ? AND ENTITY_NAME = ? "
                    +
                    "AND DOMAIN = ? AND VENDOR = ? AND TECHNOLOGY = ?";

            try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
                stmt.setQueryTimeout(INDIVIDUAL_UPDATE_TIMEOUT);

                for (Map<String, String> alertMap : batch) {
                    int paramIndex = 1;
                    stmt.setString(paramIndex++, generateProcessingTime());
                    stmt.setString(paramIndex++, alertMap.get("CHANGE_TIME"));
                    stmt.setString(paramIndex++, alertMap.get("PROBABLE_CAUSE"));
                    stmt.setString(paramIndex++, alertMap.get("DESCRIPTION"));
                    stmt.setString(paramIndex++, alertMap.get("ADDITIONAL_DETAIL"));
                    stmt.setString(paramIndex++, alertMap.get("ALARM_EXTERNAL_ID"));
                    stmt.setString(paramIndex++, alertMap.get("ALARM_CODE"));
                    stmt.setString(paramIndex++, alertMap.get("SUBENTITY"));
                    stmt.setString(paramIndex++, alertMap.get("ENTITY_ID"));
                    stmt.setString(paramIndex++, alertMap.get("ENTITY_NAME"));
                    stmt.setString(paramIndex++, alertMap.get("DOMAIN"));
                    stmt.setString(paramIndex++, alertMap.get("VENDOR"));
                    stmt.setString(paramIndex++, alertMap.get("TECHNOLOGY"));
                    stmt.addBatch();
                }

                int[] results = stmt.executeBatch();
                int successCount = 0;
                for (int result : results) {
                    if (result > 0)
                        successCount++;
                }
                totalUpdated += successCount;

                logger.info("Individual Update Batch {}/{} Completed: {} Alarms Updated",
                        (i / batchSize) + 1, (updates.size() + batchSize - 1) / batchSize, successCount);

            } catch (SQLException e) {
                logger.error("Error In Individual Update Batch {}/{}: {}",
                        (i / batchSize) + 1, (updates.size() + batchSize - 1) / batchSize, e.getMessage());
            }
        }

        logger.info("Individual Updates Completed: {} Alarms Updated Out Of {}", totalUpdated, updates.size());
    }

    private static Connection getDatabaseConnection(String dbName, JobContext jobContext) {
        Connection connection = null;
        int retryCount = 0;

        while (retryCount < MAX_RETRY_ATTEMPTS) {
            try {
                if (dbName.contains("FMS")) {
                    String SPARK_FM_JDBC_DRIVER = jobContext.getParameter("SPARK_FM_JDBC_DRIVER");
                    String SPARK_FM_JDBC_URL = jobContext.getParameter("SPARK_FM_JDBC_URL");
                    String SPARK_FM_JDBC_USERNAME = jobContext.getParameter("SPARK_FM_JDBC_USERNAME");
                    String SPARK_FM_JDBC_PASSWORD = jobContext.getParameter("SPARK_FM_JDBC_PASSWORD");

                    Class.forName(SPARK_FM_JDBC_DRIVER);
                    connection = DriverManager.getConnection(SPARK_FM_JDBC_URL, SPARK_FM_JDBC_USERNAME,
                            SPARK_FM_JDBC_PASSWORD);

                } else if (dbName.contains("PERFORMANCE")) {
                    String SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                    String SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                    String SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                    String SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");

                    Class.forName(SPARK_PM_JDBC_DRIVER);
                    connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                            SPARK_PM_JDBC_PASSWORD);
                }

                if (connection != null && !connection.isClosed()) {
                    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    connection.setAutoCommit(false);

                    try {
                        connection.createStatement()
                                .execute("SET SESSION innodb_lock_wait_timeout = " + MYSQL_LOCK_TIMEOUT);
                        connection.createStatement().execute("SET SESSION sql_mode = '" + SQL_MODE_NO_AUTO_VALUE + "'");
                    } catch (SQLException e) {
                        logger.debug("Could Not Set MySQL Session Variables: {}", e.getMessage());
                    }

                    logger.info("Database Connection Established Successfully");
                    return connection;
                }

            } catch (ClassNotFoundException e) {
                logger.error("JDBC Driver Not Found | Attempt {}/{} | Message: {}", retryCount + 1,
                        MAX_RETRY_ATTEMPTS, e.getMessage());
            } catch (SQLException e) {
                logger.error("Database Connection Error | Attempt {}/{} | Message: {}", retryCount + 1,
                        MAX_RETRY_ATTEMPTS, e.getMessage());
            } catch (Exception e) {
                logger.error("Unexpected Exception | Attempt {}/{} | Message: {}", retryCount + 1,
                        MAX_RETRY_ATTEMPTS, e.getMessage());
            }

            retryCount++;
            if (retryCount < MAX_RETRY_ATTEMPTS) {
                try {
                    Thread.sleep(RETRY_BACKOFF_MULTIPLIER * retryCount * RETRY_SLEEP_MULTIPLIER);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        throw new RuntimeException("Failed To Establish Database Connection After " + MAX_RETRY_ATTEMPTS + " Attempts");
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                    logger.info("Database Connection Closed Successfully");
                }
            } catch (SQLException e) {
                logger.error("Error Closing Database Connection: {}", e.getMessage());
            }
        }
    }
}
