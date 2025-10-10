package com.enttribe.pm.job.quarterly.common;
 
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.databind.ObjectMapper;
 
import java.util.Map;
import java.util.HashMap;
 
public class AggregateToFifteenMinCustomProcessor extends Processor {
 
    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(AggregateToFifteenMinCustomProcessor.class);
 
    public JobContext jobContext;
 
    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
 
        int sequenceno = Integer.valueOf(jobContext.getParameter("sequenceno"));
        int aggregationWindow = 15;
        String orcFilesPath = jobContext.getParameter("ORCFilesPath");
        if (orcFilesPath != null && !orcFilesPath.trim().isEmpty()) {
            logger.info("Reading ORC files from comma-separated paths: {}", orcFilesPath);
            this.dataFrame = readORCFilesFromPaths(jobContext, orcFilesPath);
        }
 
        if (this.dataFrame == null) {
            logger.warn("No ORC data loaded - dataFrame is null. Returning empty dataset.");
            // Return an empty dataset instead of null to prevent downstream errors
            return jobContext.sqlctx().createDataFrame(
                java.util.Collections.emptyList(), 
                org.apache.spark.sql.types.DataTypes.createStructType(
                    java.util.Arrays.asList(
                        org.apache.spark.sql.types.DataTypes.createStructField("finalKey", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("DateKey", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("HourKey", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("QuarterKey", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("fiveminutekey", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("pmemsid", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("nename", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("neid", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("interfacename", org.apache.spark.sql.types.DataTypes.StringType, true)
                    )
                )
            );
        }
 
        logger.debug("Processing {} counter columns (C1 to C{}) with {}-minute aggregation window",
                sequenceno, sequenceno, aggregationWindow);
 
        // Check if dataset is empty
        if (this.dataFrame.count() == 0) {
            logger.warn("Input dataset is empty - no data to aggregate");
            return this.dataFrame;
        }

        this.dataFrame.createOrReplaceTempView("inputData");
        Map<String, String> sequenceToTimeAggrMap = getSequenceToTimeAggregationMap(jobContext);
 
        StringBuilder dynamicCounterColumns = new StringBuilder();
        for (int i = 1; i <= sequenceno; i++) {
            if (i > 1) {
                dynamicCounterColumns.append(", ");
            }
 
            String aggregationFunction = getAggregationFunctionForCounter(i, sequenceToTimeAggrMap);
            dynamicCounterColumns.append(aggregationFunction).append(" AS `C").append(i).append("`");
        }
 
        String timeGroupingLogic = getTimeGroupingLogic(aggregationWindow);
 
        String groupByColumns = "CONCAT(SUBSTRING(finalKey, 1, LENGTH(finalKey) - 4), " + timeGroupingLogic + "), " +
                "DateKey, " +
                "HourKey, " +
                "CONCAT(SUBSTRING(QuarterKey, 1, LENGTH(QuarterKey) - 4), " + timeGroupingLogic + "), " +
                "CONCAT(SUBSTRING(fiveminutekey, 1, LENGTH(fiveminutekey) - 4), " + timeGroupingLogic + "), " +
                "pmemsid, " +
                "nename, " +
                "neid, " +
                "interfacename, " +
                "SUBSTRING(QuarterKey, 9, 4)";
 
        String aggregateSQL = "SELECT " +
                "CONCAT(SUBSTRING(finalKey, 1, LENGTH(finalKey) - 4), " + timeGroupingLogic + ") AS finalKey, " +
                "DateKey, " +
                "HourKey, " +
                "CONCAT(SUBSTRING(QuarterKey, 1, LENGTH(QuarterKey) - 4), " + timeGroupingLogic + ") AS QuarterKey, " +
                "CONCAT(SUBSTRING(fiveminutekey, 1, LENGTH(fiveminutekey) - 4), " + timeGroupingLogic
                + ") AS fiveminutekey, " +
                "pmemsid, " +
                "nename, " +
                "neid, " +
                "interfacename, " +
                dynamicCounterColumns.toString() + " " +
                "FROM inputData " +
                "GROUP BY " + groupByColumns;
 
        logger.debug("Generated {}-minute aggregation SQL with {} counters", aggregationWindow, sequenceno);
        logger.debug("Aggregation SQL: {}", aggregateSQL);
 
        Dataset<Row> aggregatedData = jobContext.sqlctx().sql(aggregateSQL);
 
        logger.debug("Sample aggregated data:");
        // aggregatedData.show(200, false);
 
        logger.debug("{}-minute aggregation completed successfully! ✅", aggregationWindow);
 
        return aggregatedData;
    }
 
    private String getTimeGroupingLogic(Integer aggregationWindow) {
        // Return HHMM format for 15-minute windows
        return "CONCAT(" +
                "SUBSTRING(RIGHT(fiveminutekey, 4), 1, 2), " + // Hours part (HH)
                "CASE " +
                "WHEN CAST(SUBSTRING(RIGHT(fiveminutekey, 4), 3, 2) AS INT) < " + aggregationWindow + " THEN '00' " +
                "WHEN CAST(SUBSTRING(RIGHT(fiveminutekey, 4), 3, 2) AS INT) < " + (aggregationWindow * 2) + " THEN '"
                + String.format("%02d", aggregationWindow) + "' " +
                "WHEN CAST(SUBSTRING(RIGHT(fiveminutekey, 4), 3, 2) AS INT) < " + (aggregationWindow * 3) + " THEN '"
                + String.format("%02d", aggregationWindow * 2) + "' " +
                "ELSE '" + String.format("%02d", aggregationWindow * 3) + "' " +
                "END)";
    }
 
    private Dataset<Row> readORCFilesFromPaths(JobContext jobContext, String orcFilesPath) throws Exception {
        String[] paths = orcFilesPath.split(",");
        String[] processedPaths = new String[paths.length];
        int validPathCount = 0;

        logger.info("Processing ORC file paths : {}", orcFilesPath);

        for (String path : paths) {
            if (path.isEmpty()) {
                continue;
            }
            String trimmedPath = path.trim();
            if (!trimmedPath.endsWith("/")) {
                trimmedPath += "/";
            }
            // Use the correct wildcard pattern for the actual file structure
            trimmedPath += "ptime=*/categoryname=*/part-*.snappy.orc";
            processedPaths[validPathCount] = trimmedPath;
            validPathCount++;
        }

        // Create a new array with only valid paths
        String[] finalPaths = new String[validPathCount];
        System.arraycopy(processedPaths, 0, finalPaths, 0, validPathCount);

        logger.info("Reading ORC files from {} paths with wildcards:", finalPaths.length);
        for (String path : finalPaths) {
            logger.info("  - {}", path);
        }

        Dataset<Row> pathData = null;
        Dataset<Row> combinedData = null;
        int successfulLoads = 0;
        int failedLoads = 0;

        // Load each path separately and union them
        for (String path : finalPaths) {
            try {
                logger.info("Loading ORC files from: {}", path);
                Dataset<Row> currentData = jobContext.getFileReader()
                        .format("orc")
                        .option("mergeSchema", "true")
                        .option("basePath", "s3a://performance/JOB/ORC/")
                        .load(path);
                
                // Check if the dataset is not empty
                if (currentData != null) {
                    if (combinedData == null) {
                        combinedData = currentData;
                    } else {
                        combinedData = combinedData.union(currentData);
                    }
                    successfulLoads++;
                    logger.info("Successfully loaded ORC data from: {}", path);
                } else {
                    logger.warn("No data found in path: {}", path);
                    failedLoads++;
                }
                
            } catch (Exception e) {
                logger.warn("Failed to load ORC data from path: {} - {}", path, e.getMessage());
                failedLoads++;
                // Continue with other paths instead of failing completely
            }
        }

        if (combinedData == null) {
            logger.error("Failed to load any ORC data from {} paths ({} successful, {} failed)", 
                    finalPaths.length, successfulLoads, failedLoads);
            return null;
        }

        pathData = combinedData;
        logger.info("Successfully loaded ORC data from {}/{} paths ({} failed)", 
                successfulLoads, finalPaths.length, failedLoads);

        logger.info("Successfully read and combined ORC data from {} paths! ✅", finalPaths.length);
        logger.info("Combined data schema:");
        pathData.show(20, false);

        return pathData;
    }
 
    private Map<String, String> getSequenceToTimeAggregationMap(JobContext jobContext) {
        Map<String, String> sequenceToTimeAggrMap = new HashMap<>();
 
        try {
            String sequenceToTimeAggrJson = jobContext.getParameter("SEQUENCE_TO_TIME_AGGR_MAP");
            if (sequenceToTimeAggrJson != null && !sequenceToTimeAggrJson.trim().isEmpty()) {
                ObjectMapper mapper = new ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, String> tempMap = mapper.readValue(sequenceToTimeAggrJson, Map.class);
                sequenceToTimeAggrMap = tempMap;
                logger.debug("Loaded sequence to time aggregation map with {} entries", sequenceToTimeAggrMap.size());
            } else {
                logger.debug("SEQUENCE_TO_TIME_AGGR_MAP parameter not found, using default AVG aggregation");
            }
        } catch (Exception e) {
            logger.error("Failed to parse SEQUENCE_TO_TIME_AGGR_MAP: {}", e.getMessage());
        }
 
        return sequenceToTimeAggrMap;
    }
 
    private String getAggregationFunctionForCounter(int sequenceNo, Map<String, String> sequenceToTimeAggrMap) {
        String timeAggregation = "AVG";
 
        String timeAggr = sequenceToTimeAggrMap.get(String.valueOf(sequenceNo));
        if (timeAggr != null) {
            timeAggregation = timeAggr;
            logger.debug("Found aggregation for C{}: {}", sequenceNo, timeAggregation);
        } else {
            logger.debug("No aggregation found for sequence {}, using default AVG", sequenceNo);
        }
 
        logger.debug("Counter C{} using {} aggregation", sequenceNo, timeAggregation);
 
        return buildAggregationSQL(sequenceNo, timeAggregation);
    }
 
    private String buildAggregationSQL(int sequenceNo, String aggregationType) {
        if (aggregationType == null || aggregationType.trim().isEmpty()) {
            aggregationType = "AVG";
        }
 
        String columnName = "`C" + sequenceNo + "`";
        String castExpression = "CAST(" + columnName + " AS DOUBLE)";
 
        if ("EXCLUDE_ZERO_AVG".equalsIgnoreCase(aggregationType)) {
            return "AVG(CASE WHEN " + castExpression + " = 0 THEN NULL ELSE " + castExpression + " END)";
        }
 
        String upperAggrType = aggregationType.toUpperCase();
        logger.debug("Using dynamic aggregation function: {} for C{}", upperAggrType, sequenceNo);
 
        return upperAggrType + "(" + castExpression + ")";
    }
}