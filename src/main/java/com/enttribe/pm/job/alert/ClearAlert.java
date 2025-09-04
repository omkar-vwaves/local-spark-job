package com.enttribe.pm.job.alert;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClearAlert extends Processor {
    private static final Logger logger = LoggerFactory.getLogger(ClearAlert.class);
    private static final ThreadPoolExecutor connectionPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static {
        dateFormat.setLenient(false); // Strict date parsing
    }

    public ClearAlert() {
        logger.info("ClearAlert No Argument Constructor Called!");
    }

    public ClearAlert(Dataset<Row> dataFrame, Integer id, String processorName) {
        super(id, processorName);
        logger.info("ClearAlert Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("ClearAlert Execution Started!");
        Dataset<Row> inputDF = this.dataFrame;
        if (inputDF != null && !inputDF.isEmpty()) {
            logger.info("Input DataFrame Preview (First 5 rows):");
            inputDF.show(5, false);
            List<Row> rows = inputDF.collectAsList();
            if (rows.isEmpty()) {
                logger.info("Rows are Empty! Returning Empty DataFrame!");
                return inputDF;
            } else {
                int adaptiveBatchSize = this.calculateAdaptiveBatchSize(rows.size());
                logger.info("Processing {} Rows In Batches Of {} (Only OPEN Alarms Will Be Updated)", rows.size(),
                        adaptiveBatchSize);
                this.processClearAlertsInBatches(rows, jobContext, adaptiveBatchSize);
                logger.info("ClearAlert Execution Completed Successfully! (Only OPEN Alarms Were Updated)");
                return this.dataFrame;
            }
        } else {
            logger.info("Input DataFrame is Empty! Returning Empty DataFrame!");
            return inputDF;
        }
    }

    private void processClearAlertsInBatches(List<Row> rows, JobContext jobContext, int batchSize) {
        Connection connection = null;
        AtomicInteger totalUpdated = new AtomicInteger(0);
        AtomicInteger totalSkipped = new AtomicInteger(0);
        AtomicInteger totalNotFound = new AtomicInteger(0);
        AtomicInteger totalInvalidTime = new AtomicInteger(0);
        long overallStartTime = System.currentTimeMillis();
        long preloadStartTime = 0L;
        long processingStartTime = 0L;
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        logger.info("Initial Memory Usage: {} MB", initialMemory / 1048576L);

        try {
            connection = getDatabaseConnection("FMS", jobContext);
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(2);
            connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 15000);
            preloadStartTime = System.currentTimeMillis();
            Map<String, String> existingOpenAlarmsWithTimes = this.preloadExistingOpenAlarms(connection, rows);
            logger.info("Found {} Existing OPEN Alarms To Process", existingOpenAlarmsWithTimes.size());
            if (existingOpenAlarmsWithTimes.isEmpty()) {
                logger.info("No OPEN Alarms Found - Nothing To Clear");
                return;
            }

            int totalBatches = (rows.size() + batchSize - 1) / batchSize;
            AtomicInteger processedCount = new AtomicInteger(0);
            processingStartTime = System.currentTimeMillis();
            if (rows.size() > 5000) {
                this.processBatchesInParallel(rows, existingOpenAlarmsWithTimes, jobContext, batchSize, totalBatches,
                        totalUpdated, totalSkipped, totalNotFound, totalInvalidTime, processedCount);
            } else {
                this.processBatchesSequentially(rows, connection, existingOpenAlarmsWithTimes, jobContext, batchSize,
                        totalBatches, totalUpdated, totalSkipped, totalNotFound, totalInvalidTime, processedCount);
            }

            long totalTime = System.currentTimeMillis() - overallStartTime;
            long preloadTime = preloadStartTime > 0L ? preloadStartTime - overallStartTime : 0L;
            long processingTime = processingStartTime > 0L ? System.currentTimeMillis() - processingStartTime : 0L;
            long finalMemory = runtime.totalMemory() - runtime.freeMemory();
            long memoryUsed = finalMemory - initialMemory;
            logger.info("Final Memory Usage: {} MB (Change: {} MB)", finalMemory / 1048576L,
                    memoryUsed / 1048576L);
            logger.info(
                    "FINAL SUMMARY: {} Alarms Updated, {} Skipped (Not OPEN), {} Not Found, {} Invalid Time Out Of {} Total Rows",
                    new Object[] { totalUpdated.get(), totalSkipped.get(), totalNotFound.get(), totalInvalidTime.get(), rows.size() });
            logger.info("PERFORMANCE: Total={}ms, Preload={}ms, Processing={}ms, Throughput={} Rows/Sec",
                    new Object[] { totalTime, preloadTime, processingTime,
                            totalTime > 0L ? (long) (rows.size() * 1000) / totalTime : 0L });
        } catch (Exception var35) {
            logger.error("Error In Batch Processing: {}", var35.getMessage(), var35);
            if (connection != null) {
                try {
                    connection.rollback();
                    logger.info("Transaction Rolled Back Due To Error");
                } catch (SQLException var34) {
                    logger.error("Error During Rollback: {}", var34.getMessage());
                }
            }

            throw new RuntimeException("Failed To Process Clear Alerts", var35);
        } finally {
            closeConnection(connection);
        }

    }

    private void processBatchesInParallel(List<Row> rows, Map<String, String> existingOpenAlarmsWithTimes, JobContext jobContext,
            int batchSize, int totalBatches, AtomicInteger totalUpdated, AtomicInteger totalSkipped,
            AtomicInteger totalNotFound, AtomicInteger totalInvalidTime, AtomicInteger processedCount) {
        logger.info("Starting Parallel Processing with {} threads", 4);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < rows.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, rows.size());
            List<Row> batch = rows.subList(i, endIndex);
            int batchNumber = i / batchSize + 1;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Connection batchConnection = getDatabaseConnection("FMS", jobContext);

                    try {
                        batchConnection.setAutoCommit(false);
                        batchConnection.setTransactionIsolation(2);
                        ClearAlert.BatchResult result = this.processClearBatch(batch, batchConnection,
                                existingOpenAlarmsWithTimes, jobContext);
                        totalUpdated.addAndGet(result.updatedAlarms);
                        totalSkipped.addAndGet(result.skippedAlarms);
                        totalNotFound.addAndGet(result.notFoundAlarms);
                        totalInvalidTime.addAndGet(result.invalidTimeAlarms);
                        processedCount.addAndGet(batch.size());
                        batchConnection.commit();
                        logger.info("Parallel Batch {}/{} Completed In {}ms. Updated: {}, Processed: {}/{}",
                                new Object[] { batchNumber, totalBatches, result.processingTime, result.updatedAlarms,
                                        processedCount.get(), rows.size() });
                    } catch (Throwable var15) {
                        if (batchConnection != null) {
                            try {
                                batchConnection.close();
                            } catch (Throwable var14) {
                                var15.addSuppressed(var14);
                            }
                        }

                        throw var15;
                    }

                    if (batchConnection != null) {
                        batchConnection.close();
                    }
                } catch (Exception var16) {
                    logger.error("Error In Parallel Batch {}/{}: {}",
                            new Object[] { batchNumber, totalBatches, var16.getMessage() });
                }

            }, connectionPool);
            futures.add(future);
        }

        CompletableFuture.allOf((CompletableFuture[]) futures.toArray(new CompletableFuture[0])).join();
        logger.info("All Parallel Batches Completed");
    }

    private void processBatchesSequentially(List<Row> rows, Connection connection, Map<String, String> existingOpenAlarmsWithTimes,
            JobContext jobContext, int batchSize, int totalBatches, AtomicInteger totalUpdated,
            AtomicInteger totalSkipped, AtomicInteger totalNotFound, AtomicInteger totalInvalidTime, AtomicInteger processedCount) {
        for (int i = 0; i < rows.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, rows.size());
            List<Row> batch = rows.subList(i, endIndex);
            int batchNumber = i / batchSize + 1;
            long batchStartTime = System.currentTimeMillis();
            logger.info("Processing Batch {}/{} (Rows {}-{})",
                    new Object[] { batchNumber, totalBatches, i + 1, endIndex });

            try {
                ClearAlert.BatchResult result = this.processClearBatch(batch, connection, existingOpenAlarmsWithTimes,
                        jobContext);
                totalUpdated.addAndGet(result.updatedAlarms);
                totalSkipped.addAndGet(result.skippedAlarms);
                totalNotFound.addAndGet(result.notFoundAlarms);
                totalInvalidTime.addAndGet(result.invalidTimeAlarms);
                processedCount.addAndGet(batch.size());
                connection.commit();
                long batchTime = System.currentTimeMillis() - batchStartTime;
                long overallTime = System.currentTimeMillis() - batchStartTime;
                logger.info("Batch {}/{} Completed In {}ms. Total Processed: {}/{} (Overall: {}ms)", new Object[] {
                        batchNumber, totalBatches, batchTime, processedCount.get(), rows.size(), overallTime });
                if (overallTime > 150000L) {
                    logger.info("Overall Processing Taking Longer Than Expected: {}ms For {} Rows", overallTime,
                            processedCount.get());
                }
            } catch (Exception var22) {
                logger.error("Error Processing Batch {}/{}: {}",
                        new Object[] { batchNumber, totalBatches, var22.getMessage() });
                processedCount.addAndGet(batch.size());
            }
        }

    }

    private Map<String, String> preloadExistingOpenAlarms(Connection connection, List<Row> rows) {
        Map<String, String> existingOpenAlarmsWithTimes = new ConcurrentHashMap<>();

        try {
            Set<String> uniqueKeys = new HashSet<>();
            Iterator<Row> rowsIterator = rows.iterator();

            while (rowsIterator.hasNext()) {
                Row row = rowsIterator.next();

                try {
                    Map<String, String> alertMap = row.getJavaMap(row.fieldIndex("ALERT_MAP"));
                    if (alertMap == null) {
                        logger.info("Skipping Row With Null ALERT_MAP During Preload");
                    } else {
                        Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                        String key = this.buildAlarmKey(mutableAlertMap);
                        uniqueKeys.add(key);
                    }
                } catch (Exception var18) {
                    logger.error("Error Processing Row During Preload: {}", var18.getMessage());
                }
            }

            if (uniqueKeys.isEmpty()) {
                logger.info("No Unique Keys Found For Preloading");
                return existingOpenAlarmsWithTimes;
            } else {
                logger.info("Found {} Unique Keys To Check Against Database", uniqueKeys.size());
                Set<String> alarmExternalIds = new HashSet<>();
                Set<String> alarmCodes = new HashSet<>();
                Set<String> subentities = new HashSet<>();
                Set<String> entityIds = new HashSet<>();
                Set<String> entityNames = new HashSet<>();
                Iterator<String> uniqueKeysIterator = uniqueKeys.iterator();

                while (uniqueKeysIterator.hasNext()) {
                    String key = uniqueKeysIterator.next();
                    String[] parts = key.split("\\|");
                    if (parts.length >= 5) {
                        alarmExternalIds.add(parts[0]);
                        alarmCodes.add(parts[1]);
                        subentities.add(parts[2]);
                        entityIds.add(parts[3]);
                        entityNames.add(parts[4]);
                    }
                }

                StringBuilder query = new StringBuilder();
                query.append(
                        "SELECT ALARM_EXTERNAL_ID, ALARM_CODE, SUBENTITY, ENTITY_ID, ENTITY_NAME, ALARM_STATUS, OPEN_TIME, CHANGE_TIME ");
                query.append("FROM ALARM WHERE ALARM_STATUS = 'OPEN' AND ");
                query.append("ALARM_EXTERNAL_ID IN (")
                        .append(String.join(",", Collections.nCopies(alarmExternalIds.size(), "?"))).append(") AND ");
                query.append("ALARM_CODE IN (").append(String.join(",", Collections.nCopies(alarmCodes.size(), "?")))
                        .append(") AND ");
                query.append("SUBENTITY IN (").append(String.join(",", Collections.nCopies(subentities.size(), "?")))
                        .append(") AND ");
                query.append("ENTITY_ID IN (").append(String.join(",", Collections.nCopies(entityIds.size(), "?")))
                        .append(") AND ");
                query.append("ENTITY_NAME IN (").append(String.join(",", Collections.nCopies(entityNames.size(), "?")))
                        .append(")");
                PreparedStatement stmt = connection.prepareStatement(query.toString());

                try {
                    stmt.setQueryTimeout(30);
                    stmt.setFetchSize(1000);
                    int paramIndex = 1;
                    Iterator<String> alarmExternalIdsIterator = alarmExternalIds.iterator();

                    String key;
                    while (alarmExternalIdsIterator.hasNext()) {
                        key = alarmExternalIdsIterator.next();
                        stmt.setString(paramIndex++, key);
                    }

                    Iterator<String> alarmCodesIterator = alarmCodes.iterator();

                    while (alarmCodesIterator.hasNext()) {
                        key = alarmCodesIterator.next();
                        stmt.setString(paramIndex++, key);
                    }

                    Iterator<String> subentitiesIterator = subentities.iterator();

                    while (subentitiesIterator.hasNext()) {
                        key = subentitiesIterator.next();
                        stmt.setString(paramIndex++, key);
                    }

                    Iterator<String> entityIdsIterator = entityIds.iterator();

                    while (entityIdsIterator.hasNext()) {
                        key = entityIdsIterator.next();
                        stmt.setString(paramIndex++, key);
                    }

                    Iterator<String> entityNamesIterator = entityNames.iterator();

                    while (entityNamesIterator.hasNext()) {
                        key = entityNamesIterator.next();
                        stmt.setString(paramIndex++, key);
                    }

                    logger.info("Executing Single Optimized Query For {} Unique Keys...",
                            uniqueKeys.size());
                    ResultSet rs = stmt.executeQuery();

                    try {
                        while (rs.next()) {
                            key = this.buildKeyFromResultSet(rs);
                            String openTime = rs.getString("OPEN_TIME");
                            existingOpenAlarmsWithTimes.put(key, openTime);
                        }
                    } catch (Throwable var19) {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (Throwable var17) {
                                var19.addSuppressed(var17);
                            }
                        }

                        throw var19;
                    }

                    if (rs != null) {
                        rs.close();
                    }

                    logger.info("Single Query Completed - Found {} Existing OPEN Alarms",
                            existingOpenAlarmsWithTimes.size());
                } catch (Throwable var20) {
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (Throwable var16) {
                            var20.addSuppressed(var16);
                        }
                    }

                    throw var20;
                }

                if (stmt != null) {
                    stmt.close();
                }

                return existingOpenAlarmsWithTimes;
            }
        } catch (SQLException var21) {
            logger.error("Error Preloading Existing OPEN Alarms: {}", var21.getMessage(), var21);
            throw new RuntimeException("Failed To Preload Existing OPEN Alarms", var21);
        }
    }

    private ClearAlert.BatchResult processClearBatch(List<Row> batch, Connection connection,
            Map<String, String> existingOpenAlarmsWithTimes, JobContext jobContext) {
        List<Map<String, String>> updates = new ArrayList<>();
        List<Map<String, String>> skippedAlarms = new ArrayList<>();
        List<Map<String, String>> notFoundAlarms = new ArrayList<>();
        List<Map<String, String>> invalidTimeAlarms = new ArrayList<>();
        logger.info("Starting Categorization Of {} Rows In Batch", batch.size());
        long startTime = System.currentTimeMillis();

        long elapsed;
        for (int i = 0; i < batch.size(); ++i) {
            Row row = batch.get(i);
            if (i % 100 == 0 && i > 0) {
                elapsed = System.currentTimeMillis() - startTime;
                logger.info("Processed {}/{} Rows In Current Batch ({}ms Elapsed)",
                        new Object[] { i, batch.size(), elapsed });
                if (elapsed > 15000L) {
                    logger.info("Batch Processing Taking Longer Than Expected: {}ms For {} Rows", elapsed, i);
                }
            }

            try {
                Map<String, String> alertMap = row.getJavaMap(row.fieldIndex("ALERT_MAP"));
                if (alertMap == null) {
                    logger.info("Skipping Row With Null ALERT_MAP");
                } else {
                    Map<String, String> mutableAlertMap = new HashMap<>(alertMap);
                    String key = this.buildAlarmKey(mutableAlertMap);
                    if (existingOpenAlarmsWithTimes.containsKey(key)) {
                        // Validate CLOSURE_TIME >= OPEN_TIME
                        String openTime = existingOpenAlarmsWithTimes.get(key);
                        String closureTime = mutableAlertMap.get("CLOSURE_TIME");
                        
                        if (isValidClosureTime(openTime, closureTime, mutableAlertMap.get("ALARM_EXTERNAL_ID"))) {
                            updates.add(mutableAlertMap);
                            if (i < 3) {
                                logger.info("Will Update OPEN Alarm: {}", key);
                            }
                        } else {
                            invalidTimeAlarms.add(mutableAlertMap);
                            if (i < 3) {
                                logger.info("Invalid Closure Time - Skipping Alarm: {}", key);
                            }
                        }
                    } else {
                        notFoundAlarms.add(mutableAlertMap);
                        if (i < 3) {
                            logger.info("Alarm Not Found: {}", key);
                        }
                    }
                }
            } catch (Exception var16) {
                logger.error("Error Processing Row {}: {}", i, var16.getMessage());
            }
        }

        long categorizationTime = System.currentTimeMillis() - startTime;
        logger.info("Completed Categorization In {}ms: {} Updates, {} Skipped, {} Not Found, {} Invalid Time",
                new Object[] { categorizationTime, updates.size(), skippedAlarms.size(), notFoundAlarms.size(), invalidTimeAlarms.size() });
        if (!updates.isEmpty()) {
            try {
                this.batchUpdateAlarmsForClosure(updates, connection);
            } catch (Exception var15) {
                logger.error("Error In Batch Update: {}", var15.getMessage());
            }
        }

        elapsed = System.currentTimeMillis() - startTime;
        logger.info("Batch Processed In {}ms: {} Updates, {} Skipped, {} Not Found, {} Invalid Time",
                new Object[] { elapsed, updates.size(), skippedAlarms.size(), notFoundAlarms.size(), invalidTimeAlarms.size() });
        return new ClearAlert.BatchResult(updates.size(), skippedAlarms.size(), notFoundAlarms.size(), invalidTimeAlarms.size(), elapsed);
    }

    /**
     * Validates that CLOSURE_TIME >= OPEN_TIME
     * @param openTime The OPEN_TIME from the database
     * @param closureTime The CLOSURE_TIME from the alert
     * @param alarmExternalId The alarm external ID for logging
     * @return true if CLOSURE_TIME >= OPEN_TIME, false otherwise
     */
    private boolean isValidClosureTime(String openTime, String closureTime, String alarmExternalId) {
        if (openTime == null || openTime.trim().isEmpty()) {
            logger.warn("[CloseAlertValidation] Invalid closure attempt: OPEN_TIME is null or empty. AlarmID: {}", alarmExternalId);
            return false;
        }
        
        if (closureTime == null || closureTime.trim().isEmpty()) {
            logger.warn("[CloseAlertValidation] Invalid closure attempt: CLOSURE_TIME is null or empty. AlarmID: {}", alarmExternalId);
            return false;
        }
        
        try {
            long openTimeMillis = parseTimeString(openTime.trim());
            long closureTimeMillis = parseTimeString(closureTime.trim());
            
            if (closureTimeMillis < openTimeMillis) {
                logger.warn("[CloseAlertValidation] Invalid closure attempt: ClosureTime < OpenTime. AlarmID: {}, OpenTime: {}, ClosureTime: {}", 
                           alarmExternalId, openTime, closureTime);
                return false;
            }
            
            return true;
        } catch (ParseException e) {
            logger.error("[CloseAlertValidation] Error parsing time format. AlarmID: {}, OpenTime: {}, ClosureTime: {}, Error: {}", 
                       alarmExternalId, openTime, closureTime, e.getMessage());
            return false;
        }
    }

    /**
     * Parses time string with support for multiple formats
     * @param timeString The time string to parse
     * @return milliseconds since epoch
     * @throws ParseException if parsing fails
     */
    private long parseTimeString(String timeString) throws ParseException {
        // Try the standard format first
        try {
            return dateFormat.parse(timeString).getTime();
        } catch (ParseException e) {
            // Try alternative formats if standard format fails
            String[] alternativeFormats = {
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            };
            
            for (String format : alternativeFormats) {
                try {
                    SimpleDateFormat altFormat = new SimpleDateFormat(format);
                    altFormat.setLenient(false);
                    return altFormat.parse(timeString).getTime();
                } catch (ParseException ignored) {
                    // Continue to next format
                }
            }
            
            // If all formats fail, re-throw the original exception
            throw e;
        }
    }

    private void batchUpdateAlarmsForClosure(List<Map<String, String>> alertMaps, Connection connection) {
        if (!alertMaps.isEmpty()) {
            // Group records by ADDITIONAL_DETAIL to perform multiple bulk updates
            Map<String, List<Map<String, String>>> groupedByAdditionalDetail = new HashMap<>();
            
            for (Map<String, String> alertMap : alertMaps) {
                String additionalDetail = alertMap.get("ADDITIONAL_DETAIL");
                if (additionalDetail == null) {
                    additionalDetail = ""; // Handle null values
                }
                groupedByAdditionalDetail.computeIfAbsent(additionalDetail, k -> new ArrayList<>()).add(alertMap);
            }
            
            logger.info("Grouped {} alarms into {} groups by ADDITIONAL_DETAIL for bulk updates", 
                       alertMaps.size(), groupedByAdditionalDetail.size());
            
            int totalUpdated = 0;
            for (Map.Entry<String, List<Map<String, String>>> entry : groupedByAdditionalDetail.entrySet()) {
                String additionalDetail = entry.getKey();
                List<Map<String, String>> group = entry.getValue();
                
                logger.info("Processing bulk update for group with ADDITIONAL_DETAIL='{}' containing {} alarms", 
                           additionalDetail.isEmpty() ? "NULL/EMPTY" : additionalDetail, group.size());
                
                try {
                    this.performOptimizedBulkUpdate(group, connection);
                    totalUpdated += group.size();
                } catch (Exception e) {
                    logger.error("Error in bulk update for group with ADDITIONAL_DETAIL='{}': {}", 
                               additionalDetail.isEmpty() ? "NULL/EMPTY" : additionalDetail, e.getMessage());
                    // Fallback to individual updates for this group
                    logger.info("Falling back to individual updates for group with {} alarms", group.size());
                    this.performIndividualUpdatesForClosure(group, connection);
                    totalUpdated += group.size();
                }
            }
            
            logger.info("Completed bulk updates for {} alarms across {} groups", totalUpdated, groupedByAdditionalDetail.size());
        }
    }
    
    private void performOptimizedBulkUpdate(List<Map<String, String>> alertMaps, Connection connection) {
        logger.info("Starting Optimized Bulk Update For Closure Of {} Alarms...", alertMaps.size());
        long startTime = System.currentTimeMillis();
        String bulkUpdateQuery = this.buildBulkUpdateQueryForClosure(alertMaps);

        try {
            PreparedStatement stmt = connection.prepareStatement(bulkUpdateQuery);

            try {
                stmt.setQueryTimeout(30);
                stmt.setFetchSize(1000);
                int paramIndex = 1;
                String commonClosureReceptionTime = (String) alertMaps.get(0).get("CLOSURE_RECEPTION_TIME");
                String commonClosureTime = (String) alertMaps.get(0).get("CLOSURE_TIME");
                String commonClosureProcessingTime = (String) alertMaps.get(0)
                        .get("CLOSURE_PROCESSING_TIME");
                String commonLastProcessingTime = (String) alertMaps.get(0).get("LAST_PROCESSING_TIME");
                String commonProbableCause = (String) alertMaps.get(0).get("PROBABLE_CAUSE");
                String commonDescription = (String) alertMaps.get(0).get("DESCRIPTION");
                String commonAdditionalDetail = (String) alertMaps.get(0).get("ADDITIONAL_DETAIL");
                int var29 = paramIndex + 1;
                stmt.setString(paramIndex, commonClosureReceptionTime);
                stmt.setString(var29++, commonClosureTime);
                stmt.setString(var29++, commonClosureProcessingTime);
                stmt.setString(var29++, commonLastProcessingTime);
                stmt.setString(var29++, commonProbableCause);
                stmt.setString(var29++, commonDescription);
                stmt.setString(var29++, commonAdditionalDetail);
                Set<String> alarmExternalIds = new HashSet<>();
                Set<String> alarmCodes = new HashSet<>();
                Set<String> subentities = new HashSet<>();
                Set<String> entityIds = new HashSet<>();
                Set<String> entityNames = new HashSet<>();
                Iterator<Map<String, String>> alertMapsIterator = alertMaps.iterator();

                while (alertMapsIterator.hasNext()) {
                    Map<String, String> alertMap = alertMapsIterator.next();
                    alarmExternalIds.add((String) alertMap.get("ALARM_EXTERNAL_ID"));
                    alarmCodes.add((String) alertMap.get("ALARM_CODE"));
                    subentities.add((String) alertMap.get("SUBENTITY"));
                    entityIds.add((String) alertMap.get("ENTITY_ID"));
                    entityNames.add((String) alertMap.get("ENTITY_NAME"));
                }

                Iterator<String> alarmExternalIdsIterator = alarmExternalIds.iterator();

                String entityName;
                while (alarmExternalIdsIterator.hasNext()) {
                    entityName = alarmExternalIdsIterator.next();
                    stmt.setString(var29++, entityName);
                }

                Iterator<String> alarmCodesIterator = alarmCodes.iterator();

                while (alarmCodesIterator.hasNext()) {
                    entityName = alarmCodesIterator.next();
                    stmt.setString(var29++, entityName);
                }

                Iterator<String> subentitiesIterator = subentities.iterator();

                while (subentitiesIterator.hasNext()) {
                    entityName = subentitiesIterator.next();
                    stmt.setString(var29++, entityName);
                }

                Iterator<String> entityIdsIterator = entityIds.iterator();

                while (entityIdsIterator.hasNext()) {
                    entityName = entityIdsIterator.next();
                    stmt.setString(var29++, entityName);
                }

                Iterator<String> entityNamesIterator = entityNames.iterator();

                while (true) {
                    if (!entityNamesIterator.hasNext()) {
                        long queryStartTime = System.currentTimeMillis();
                        int affectedRows = stmt.executeUpdate();
                        long queryTime = System.currentTimeMillis() - queryStartTime;
                        long totalTime = System.currentTimeMillis() - startTime;
                        logger.info(
                                "Optimized Bulk Update Completed In {}ms (Query: {}ms): {} Alarms Updated Successfully",
                                new Object[] { totalTime, queryTime, affectedRows });
                        break;
                    }

                    entityName = entityNamesIterator.next();
                    stmt.setString(var29++, entityName);
                }
            } catch (Throwable var27) {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Throwable var26) {
                        var27.addSuppressed(var26);
                    }
                }

                throw var27;
            }

            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException var28) {
            logger.error("Error In Optimized Bulk Update: {}", var28.getMessage(), var28);
            logger.info("Falling Back To Individual Updates...");
            this.performIndividualUpdatesForClosure(alertMaps, connection);
        }
    }

    private String buildBulkUpdateQueryForClosure(List<Map<String, String>> alertMaps) {
        Set<String> alarmExternalIds = new HashSet<>();
        Set<String> alarmCodes = new HashSet<>();
        Set<String> subentities = new HashSet<>();
        Set<String> entityIds = new HashSet<>();
        Set<String> entityNames = new HashSet<>();
        Iterator<Map<String, String>> alertMapsIterator = alertMaps.iterator();

        while (alertMapsIterator.hasNext()) {
            Map<String, String> alertMap = alertMapsIterator.next();
            alarmExternalIds.add((String) alertMap.get("ALARM_EXTERNAL_ID"));
            alarmCodes.add((String) alertMap.get("ALARM_CODE"));
            subentities.add((String) alertMap.get("SUBENTITY"));
            entityIds.add((String) alertMap.get("ENTITY_ID"));
            entityNames.add((String) alertMap.get("ENTITY_NAME"));
        }

        StringBuilder query = new StringBuilder();
        query.append("UPDATE ALARM SET ");
        query.append("CLOSURE_RECEPTION_TIME = ?, ");
        query.append("CLOSURE_TIME = ?, ");
        query.append("CLOSURE_PROCESSING_TIME = ?, ");
        query.append("LAST_PROCESSING_TIME = ?, ");
        query.append("PROBABLE_CAUSE = ?, ");
        query.append("DESCRIPTION = ?, ");
        query.append("ADDITIONAL_DETAIL = ?, ");
        query.append("ALARM_STATUS = 'CLOSED', ");
        query.append("SEVERITY = 'CLEARED' ");
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
                .append(")");
        return query.toString();
    }

    private void performIndividualUpdatesForClosure(List<Map<String, String>> alertMaps, Connection connection) {
        logger.info("Performing Individual Updates For {} Alarms", alertMaps.size());
        String sqlQuery = "UPDATE ALARM SET CLOSURE_RECEPTION_TIME = ?, CLOSURE_TIME = ?, CLOSURE_PROCESSING_TIME = ?, LAST_PROCESSING_TIME = ?, PROBABLE_CAUSE = ?, DESCRIPTION = ?, ADDITIONAL_DETAIL = ?, ALARM_STATUS = 'CLOSED', SEVERITY = 'CLEARED' WHERE ALARM_STATUS = 'OPEN' AND ALARM_EXTERNAL_ID = ? AND ALARM_CODE = ? AND SUBENTITY = ? AND ENTITY_ID = ? AND ENTITY_NAME = ?";

        int batchSize = 100; // Process in smaller batches
        int totalUpdated = 0;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < alertMaps.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, alertMaps.size());
            List<Map<String, String>> batch = alertMaps.subList(i, endIndex);
            
            logger.info("Processing individual update batch {}/{} with {} alarms", 
                       (i / batchSize) + 1, (alertMaps.size() + batchSize - 1) / batchSize, batch.size());

            try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
                stmt.setQueryTimeout(60);
                stmt.setFetchSize(1000);

                for (Map<String, String> alertMap : batch) {
                    int paramIndex = 1;
                    stmt.setString(paramIndex++, (String) alertMap.get("CLOSURE_RECEPTION_TIME"));
                    stmt.setString(paramIndex++, (String) alertMap.get("CLOSURE_TIME"));
                    stmt.setString(paramIndex++, (String) alertMap.get("CLOSURE_PROCESSING_TIME"));
                    stmt.setString(paramIndex++, (String) alertMap.get("LAST_PROCESSING_TIME"));
                    stmt.setString(paramIndex++, (String) alertMap.get("PROBABLE_CAUSE"));
                    stmt.setString(paramIndex++, (String) alertMap.get("DESCRIPTION"));
                    stmt.setString(paramIndex++, (String) alertMap.get("ADDITIONAL_DETAIL"));
                    stmt.setString(paramIndex++, (String) alertMap.get("ALARM_EXTERNAL_ID"));
                    stmt.setString(paramIndex++, (String) alertMap.get("ALARM_CODE"));
                    stmt.setString(paramIndex++, (String) alertMap.get("SUBENTITY"));
                    stmt.setString(paramIndex++, (String) alertMap.get("ENTITY_ID"));
                    stmt.setString(paramIndex++, (String) alertMap.get("ENTITY_NAME"));
                    stmt.addBatch();
                }

                int[] results = stmt.executeBatch();
                int successCount = 0;
                for (int result : results) {
                    if (result > 0) {
                        successCount++;
                    }
                }
                totalUpdated += successCount;
                
                long batchTime = System.currentTimeMillis() - startTime;
                logger.info("Individual update batch completed: {} alarms updated in {}ms", successCount, batchTime);

            } catch (SQLException e) {
                logger.error("Error in individual update batch {}/{}: {}", 
                           (i / batchSize) + 1, (alertMaps.size() + batchSize - 1) / batchSize, e.getMessage());
                throw new RuntimeException("Failed To Perform Individual Updates", e);
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Individual Updates Completed: {} Alarms Updated Out Of {} in {}ms", 
                   totalUpdated, alertMaps.size(), totalTime);
    }

    private String buildAlarmKey(Map<String, String> alertMap) {
        if (alertMap == null) {
            throw new IllegalArgumentException("Alert Map Cannot Be Null");
        } else {
            return String.join("|", (CharSequence) alertMap.getOrDefault("ALARM_EXTERNAL_ID", ""),
                    (CharSequence) alertMap.getOrDefault("ALARM_CODE", ""),
                    (CharSequence) alertMap.getOrDefault("SUBENTITY", ""),
                    (CharSequence) alertMap.getOrDefault("ENTITY_ID", ""),
                    (CharSequence) alertMap.getOrDefault("ENTITY_NAME", ""));
        }
    }

    private String buildKeyFromResultSet(ResultSet rs) throws SQLException {
        return String.join("|", rs.getString("ALARM_EXTERNAL_ID"), rs.getString("ALARM_CODE"),
                rs.getString("SUBENTITY"), rs.getString("ENTITY_ID"), rs.getString("ENTITY_NAME"));
    }

    private int calculateAdaptiveBatchSize(int totalRows) {
        if (totalRows <= 1000) {
            return 1000;
        } else if (totalRows <= 10000) {
            return 2000;
        } else {
            return totalRows <= 100000 ? 3000 : 5000;
        }
    }

    private static Connection getDatabaseConnection(String dbName, JobContext jobContext) {
        Connection connection = null;
        int retryCount = 0;

        while (retryCount < 3) {
            try {
                String SPARK_PM_JDBC_DRIVER;
                String SPARK_PM_JDBC_URL;
                String SPARK_PM_JDBC_USERNAME;
                String SPARK_PM_JDBC_PASSWORD;
                if (dbName.contains("FMS")) {
                    SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_FM_JDBC_DRIVER");
                    SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_FM_JDBC_URL");
                    SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_FM_JDBC_USERNAME");
                    SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_FM_JDBC_PASSWORD");
                    Class.forName(SPARK_PM_JDBC_DRIVER);
                    connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                            SPARK_PM_JDBC_PASSWORD);
                } else if (dbName.contains("PERFORMANCE")) {
                    SPARK_PM_JDBC_DRIVER = jobContext.getParameter("SPARK_PM_JDBC_DRIVER");
                    SPARK_PM_JDBC_URL = jobContext.getParameter("SPARK_PM_JDBC_URL");
                    SPARK_PM_JDBC_USERNAME = jobContext.getParameter("SPARK_PM_JDBC_USERNAME");
                    SPARK_PM_JDBC_PASSWORD = jobContext.getParameter("SPARK_PM_JDBC_PASSWORD");
                    Class.forName(SPARK_PM_JDBC_DRIVER);
                    connection = DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME,
                            SPARK_PM_JDBC_PASSWORD);
                }

                if (connection != null && !connection.isClosed()) {
                    connection.setTransactionIsolation(2);
                    connection.setAutoCommit(false);

                    try {
                        connection.createStatement().execute("SET SESSION innodb_lock_wait_timeout = 60");
                        connection.createStatement().execute("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'");
                    } catch (SQLException var8) {
                        logger.info("Could Not Set MySQL Session Variables: {}", var8.getMessage());
                    }

                    logger.info("Database Connection Established Successfully!");
                    return connection;
                }
            } catch (ClassNotFoundException var9) {
                logger.error("JDBC Driver Not Found | Attempt {}/{} | Message: {}",
                        new Object[] { retryCount + 1, 3, var9.getMessage() });
            } catch (SQLException var10) {
                logger.error("Database Connection Error | Attempt {}/{} | Message: {}",
                        new Object[] { retryCount + 1, 3, var10.getMessage() });
            } catch (Exception var11) {
                logger.error("Unexpected Exception | Attempt {}/{} | Message: {}",
                        new Object[] { retryCount + 1, 3, var11.getMessage() });
            }

            ++retryCount;
            if (retryCount < 3) {
                try {
                    Thread.sleep((long) (500 * retryCount * 1));
                } catch (InterruptedException var12) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        throw new RuntimeException("Failed To Establish Database Connection After 3 Attempts");
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                    logger.info("Database Connection Closed Successfully!");
                }
            } catch (SQLException var2) {
                logger.error("Error Closing Database Connection: {}", var2.getMessage());
            }
        }

    }

    public static void shutdown() {
        if (connectionPool != null && !connectionPool.isShutdown()) {
            connectionPool.shutdown();

            try {
                if (!connectionPool.awaitTermination(30L, TimeUnit.SECONDS)) {
                    connectionPool.shutdownNow();
                }

                logger.info("Connection Pool Shutdown Successfully");
            } catch (InterruptedException var1) {
                connectionPool.shutdownNow();
                Thread.currentThread().interrupt();
                logger.error("Error During Connection Pool Shutdown: {}", var1.getMessage());
            }
        }

    }

    private static class BatchResult {
        final int updatedAlarms;
        final int skippedAlarms;
        final int notFoundAlarms;
        final int invalidTimeAlarms;
        final long processingTime;

        BatchResult(int updatedAlarms, int skippedAlarms, int notFoundAlarms, int invalidTimeAlarms, long processingTime) {
            this.updatedAlarms = updatedAlarms;
            this.skippedAlarms = skippedAlarms;
            this.notFoundAlarms = notFoundAlarms;
            this.invalidTimeAlarms = invalidTimeAlarms;
            this.processingTime = processingTime;
        }
    }
}
