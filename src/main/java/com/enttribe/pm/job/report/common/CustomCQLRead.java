package com.enttribe.pm.job.report.common;

import com.enttribe.sparkrunner.processors.Processor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomCQLRead extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(CustomCQLRead.class);
    private static Map<String, String> jobContextMap = new HashMap<>();

    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";
    private static String SPARK_CASSANDRA_KEYSPACE_PM = "SPARK_CASSANDRA_KEYSPACE_PM";
    private static String SPARK_CASSANDRA_HOST = "SPARK_CASSANDRA_HOST";
    private static String SPARK_CASSANDRA_PORT = "SPARK_CASSANDRA_PORT";
    private static String SPARK_CASSANDRA_DATACENTER = "SPARK_CASSANDRA_DATACENTER";
    private static String SPARK_CASSANDRA_USERNAME = "SPARK_CASSANDRA_USERNAME";
    private static String SPARK_CASSANDRA_PASSWORD = "SPARK_CASSANDRA_PASSWORD";

    public CustomCQLRead() {
        super();
        logger.info("CustomCQLRead No Argument Constructor Called!");
    }

    public CustomCQLRead(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("CustomCQLRead Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public CustomCQLRead(Integer id, String processorName) {
        super(id, processorName);
        logger.info("CustomCQLRead Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("[CustomCQLRead] Execution Started!");

        long startTime = System.currentTimeMillis();

        jobContextMap = jobContext.getParameters();
        SPARK_PM_JDBC_DRIVER = jobContextMap.get(SPARK_PM_JDBC_DRIVER);
        SPARK_PM_JDBC_URL = jobContextMap.get(SPARK_PM_JDBC_URL);
        SPARK_PM_JDBC_USERNAME = jobContextMap.get(SPARK_PM_JDBC_USERNAME);
        SPARK_PM_JDBC_PASSWORD = jobContextMap.get(SPARK_PM_JDBC_PASSWORD);
        SPARK_CASSANDRA_KEYSPACE_PM = jobContextMap.get(SPARK_CASSANDRA_KEYSPACE_PM);
        SPARK_CASSANDRA_HOST = jobContextMap.get(SPARK_CASSANDRA_HOST);
        SPARK_CASSANDRA_PORT = jobContextMap.get(SPARK_CASSANDRA_PORT);
        SPARK_CASSANDRA_DATACENTER = jobContextMap.get(SPARK_CASSANDRA_DATACENTER);
        SPARK_CASSANDRA_USERNAME = jobContextMap.get(SPARK_CASSANDRA_USERNAME);
        SPARK_CASSANDRA_PASSWORD = jobContextMap.get(SPARK_CASSANDRA_PASSWORD);

        logger.info("JDBC Credentials: Driver={}, URL={}, User={}, Password={}", SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);
        logger.info("Cassandra Credentials: Keyspace={}, Host={}, Port={}, Datacenter={}, Username={}, Password={}",
                SPARK_CASSANDRA_KEYSPACE_PM, SPARK_CASSANDRA_HOST, SPARK_CASSANDRA_PORT, SPARK_CASSANDRA_DATACENTER,
                SPARK_CASSANDRA_USERNAME, SPARK_CASSANDRA_PASSWORD);

        String reportWidgetDetails = null;
        String nodeAndAggregationDetails = null;
        String extraParameters = null;
        String metaColumns = null;
        String kpiMapDetails = null;
        String ragConfiguration = null;
        String kpiGroupMapJson = null;

        if (this.dataFrame == null) {
            throw new IllegalStateException(
                    "DataFrame Is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        List<Row> rowList = this.dataFrame.collectAsList();
        if (rowList == null || rowList.isEmpty()) {
            throw new IllegalStateException(
                    "No Rows Found in DataFrame. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        Row row = rowList.get(0);
        if (row == null) {
            throw new IllegalStateException(
                    "First Row is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        Object reportWidgetDetailsObj = row.getAs("REPORT_WIDGET_DETAILS");
        reportWidgetDetails = reportWidgetDetailsObj != null ? reportWidgetDetailsObj.toString() : null;
        if (reportWidgetDetails == null) {
            throw new IllegalStateException(
                    "REPORT_WIDGET_DETAILS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        jobContext.setParameters("REPORT_WIDGET_DETAILS", reportWidgetDetails);
        logger.info("[CustomCQLRead] REPORT_WIDGET_DETAILS Set to Job Context Successfully!");

        Object nodeAndAggregationDetailsObj = row.getAs("NODE_AND_AGGREGATION_DETAILS");
        nodeAndAggregationDetails = nodeAndAggregationDetailsObj != null ? nodeAndAggregationDetailsObj.toString()
                : null;
        if (nodeAndAggregationDetails == null) {
            throw new IllegalStateException(
                    "NODE_AND_AGGREGATION_DETAILS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        jobContext.setParameters("NODE_AND_AGGREGATION_DETAILS", nodeAndAggregationDetails);
        logger.info("[CustomCQLRead] NODE_AND_AGGREGATION_DETAILS Set to Job Context Successfully!");

        Object extraParametersObj = row.getAs("EXTRA_PARAMETERS");
        extraParameters = extraParametersObj != null ? extraParametersObj.toString() : null;
        if (extraParameters == null) {
            throw new IllegalStateException(
                    "EXTRA_PARAMETERS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        jobContext.setParameters("EXTRA_PARAMETERS", extraParameters);
        logger.info("[CustomCQLRead] EXTRA_PARAMETERS Set to Job Context Successfully!");

        Object metaColumnsObj = row.getAs("META_COLUMNS");
        metaColumns = metaColumnsObj != null ? metaColumnsObj.toString() : null;
        if (metaColumns == null) {
            throw new IllegalStateException(
                    "META_COLUMNS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        jobContext.setParameters("META_COLUMNS", metaColumns);
        logger.info("[CustomCQLRead] META_COLUMNS Set to Job Context Successfully!");

        Object kpiMapDetailsObj = row.getAs("KPI_MAP");
        kpiMapDetails = kpiMapDetailsObj != null ? kpiMapDetailsObj.toString() : null;
        if (kpiMapDetails == null) {
            throw new IllegalStateException(
                    "KPI_MAP is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        jobContext.setParameters("KPI_MAP", kpiMapDetails);
        logger.info("[CustomCQLRead] KPI_MAP Set to Job Context Successfully!");

        Object ragConfigurationObj = row.getAs("RAG_CONFIGURATION");
        ragConfiguration = ragConfigurationObj != null ? ragConfigurationObj.toString() : null;
        if (ragConfiguration == null) {
            throw new IllegalStateException(
                    "RAG_CONFIGURATION is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        jobContext.setParameters("RAG_CONFIGURATION", ragConfiguration);
        logger.info("[CustomCQLRead] RAG_CONFIGURATION Set to Job Context Successfully!");

        Object kpiGroupMapJsonObj = row.getAs("KPI_GROUP_MAP");
        kpiGroupMapJson = kpiGroupMapJsonObj != null ? kpiGroupMapJsonObj.toString() : null;
        if (kpiGroupMapJson == null) {
            throw new IllegalStateException(
                    "KPI_GROUP_MAP is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }
        jobContext.setParameters("KPI_GROUP_MAP", kpiGroupMapJson);
        logger.info("[CustomCQLRead] KPI_GROUP_MAP Set to Job Context Successfully!");

        Map<String, String> reportWidgetDetailsMap = new ObjectMapper().readValue(reportWidgetDetails,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(nodeAndAggregationDetails,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extraParameters,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> metaColumnsMap = new ObjectMapper().readValue(metaColumns,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> kpiMap = new ObjectMapper().readValue(kpiMapDetails,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, Map<String, String>> ragConfigurationMap = new ObjectMapper().readValue(ragConfiguration,
                new TypeReference<Map<String, Map<String, String>>>() {
                });

        Map<String, List<String>> kpiGroupMap = new ObjectMapper().readValue(kpiGroupMapJson,
                new TypeReference<Map<String, List<String>>>() {
                });

        logger.info("[CustomCQLRead] Report Widget Details: {}", reportWidgetDetailsMap);
        logger.info("[CustomCQLRead] Node and Aggregation Details: {}", nodeAndAggregationDetailsMap);
        logger.info("[CustomCQLRead] Extra Parameters: {}", extraParametersMap);
        logger.info("[CustomCQLRead] Meta Columns: {}", metaColumnsMap);
        logger.info("[CustomCQLRead] KPI Map: {}", kpiMap);
        logger.info("[CustomCQLRead] RAG Configuration: {}", ragConfigurationMap);
        logger.info("[CustomCQLRead] KPI Group Map: {}", kpiGroupMap);
        jobContext = setSparkConf(jobContext);

        String reportMeasure = reportWidgetDetailsMap.get("reportMeasure");
        jobContext.setParameters("REPORT_MEASURE", reportMeasure);
        logger.info("[CustomCQLRead] REPORT_MEASURE '{}' Set to Job Context Successfully!", reportMeasure);

        Dataset<Row> cqlResultDataFrame = getResultOfNodeAndAggregationDetails(nodeAndAggregationDetailsMap,
                reportWidgetDetailsMap, extraParametersMap, jobContext);

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        if (cqlResultDataFrame != null && !cqlResultDataFrame.isEmpty()) {
            cqlResultDataFrame.createOrReplaceTempView("CQL_RESULT_DATAFRAME");
            cqlResultDataFrame = jobContext.sqlctx().sql("SELECT * FROM CQL_RESULT_DATAFRAME ORDER BY timestamp ASC");
            // cqlResultDataFrame.show(5);
            logger.info("+++[CustomCQLRead] Final DataFrame Displayed Successfully!");
        } else {
            StructType schema = new StructType()
                    .add("domain", DataTypes.StringType, true)
                    .add("vendor", DataTypes.StringType, true)
                    .add("technology", DataTypes.StringType, true)
                    .add("datalevel", DataTypes.StringType, true)
                    .add("date", DataTypes.StringType, true)
                    .add("nodename", DataTypes.StringType, true)
                    .add("timestamp", DataTypes.TimestampType, true)
                    .add("networktype", DataTypes.StringType, true)
                    .add("kpijson", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true)
                    .add("metajson", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);

            cqlResultDataFrame = jobContext.createDataFrame(Collections.emptyList(), schema);
            // cqlResultDataFrame.show(5);
            logger.info("+++[CustomCQLRead] Empty DataFrame Created Successfully!");

        }

        // long recordCount = cqlResultDataFrame.count();

        // int numPartitions;
        // if (recordCount <= 100000) {
        // numPartitions = 50;
        // } else if (recordCount <= 500000) {
        // numPartitions = 100;
        // } else if (recordCount <= 2000000) {
        // numPartitions = 200;
        // } else {
        // numPartitions = 400;
        // }
        int before = cqlResultDataFrame.rdd().getNumPartitions();
        logger.info("Number of Partitions Before Repartition: {}", before);

        cqlResultDataFrame = cqlResultDataFrame.repartition(100);

        int after = cqlResultDataFrame.rdd().getNumPartitions();
        logger.info("Number of Partitions After Repartition: {}", after);

        logger.info("[CustomCQLRead] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes, seconds);
        return cqlResultDataFrame;
    }

    private static String[] getCellArrayFromNodeArray(String[] nodeArray, JobContext jobContext) {
        if (nodeArray == null || nodeArray.length == 0) {
            return new String[] { "NO_INTERFACE_FOUND" };
        }

        String inClause = Stream.of(nodeArray)
                .map(neId -> "'" + neId + "'")
                .collect(Collectors.joining(","));
        String parentPkQuery = "SELECT NETWORK_ELEMENT_ID_PK FROM NETWORK_ELEMENT WHERE NE_ID IN (" + inClause + ")";
        Dataset<Row> parentDf = executeQuery(parentPkQuery, jobContext);
        List<String> parentPks = parentDf.collectAsList().stream()
                .map(row -> String.valueOf(row.get(0)))
                .collect(Collectors.toList());
        if (parentPks.isEmpty()) {
            return new String[] { "NO_INTERFACE_FOUND" };
        }
        String pkInClause = parentPks.stream()
                .map(pk -> pk)
                .collect(Collectors.joining(","));

        String childNeidQuery = "SELECT NE_ID FROM NETWORK_ELEMENT WHERE PARENT_NE_ID_FK IN (" + pkInClause + ")";
        Dataset<Row> childDf = executeQuery(childNeidQuery, jobContext);
        List<String> childNeIds = new ArrayList<>();
        for (Row row : childDf.collectAsList()) {
            childNeIds.add(row.getString(0));
        }
        if (childNeIds.isEmpty()) {
            return new String[] { "NO_INTERFACE_FOUND" };
        }
        return childNeIds.toArray(new String[0]);
    }

    private static Dataset<Row> getResultOfNodeAndAggregationDetails(
            Map<String, String> nodeAndAggregationDetails, Map<String, String> reportWidgetDetails,
            Map<String, String> extraParameters, JobContext jobContext) {

        Dataset<Row> cqlResultDataFrame = null;

        String aggregationLevel = null;

        try {

            String geoL1 = nodeAndAggregationDetails.get("geoL1");
            if (geoL1 == null)
                geoL1 = "";
            String geoL2 = nodeAndAggregationDetails.get("geoL2");
            if (geoL2 == null)
                geoL2 = "";
            String geoL3 = nodeAndAggregationDetails.get("geoL3");
            if (geoL3 == null)
                geoL3 = "";
            String geoL4 = nodeAndAggregationDetails.get("geoL4");
            if (geoL4 == null)
                geoL4 = "";
            String node = nodeAndAggregationDetails.get("node");
            if (node == null)
                node = "";
            String netype = nodeAndAggregationDetails.get("netype");
            if (netype == null)
                netype = "";
            boolean isGeoL1MultiSelect = false;
            boolean isGeoL2MultiSelect = false;
            boolean isGeoL3MultiSelect = false;
            boolean isGeoL4MultiSelect = false;
            try {
                String geoL1MultiSelectStr = nodeAndAggregationDetails.get("isGeoL1MultiSelect");
                if (geoL1MultiSelectStr != null) {
                    isGeoL1MultiSelect = Boolean.parseBoolean(geoL1MultiSelectStr);
                }
            } catch (Exception e) {
                isGeoL1MultiSelect = false;
            }
            try {
                String geoL2MultiSelectStr = nodeAndAggregationDetails.get("isGeoL2MultiSelect");
                if (geoL2MultiSelectStr != null) {
                    isGeoL2MultiSelect = Boolean.parseBoolean(geoL2MultiSelectStr);
                }
            } catch (Exception e) {
                isGeoL2MultiSelect = false;
            }
            try {
                String geoL3MultiSelectStr = nodeAndAggregationDetails.get("isGeoL3MultiSelect");
                if (geoL3MultiSelectStr != null) {
                    isGeoL3MultiSelect = Boolean.parseBoolean(geoL3MultiSelectStr);
                }
            } catch (Exception e) {
                isGeoL3MultiSelect = false;
            }
            try {
                String geoL4MultiSelectStr = nodeAndAggregationDetails.get("isGeoL4MultiSelect");
                if (geoL4MultiSelectStr != null) {
                    isGeoL4MultiSelect = Boolean.parseBoolean(geoL4MultiSelectStr);
                }
            } catch (Exception e) {
                isGeoL4MultiSelect = false;
            }
            String geoL1List = nodeAndAggregationDetails.get("geoL1List");
            String geoL2List = nodeAndAggregationDetails.get("geoL2List");
            String geoL3List = nodeAndAggregationDetails.get("geoL3List");
            String geoL4List = nodeAndAggregationDetails.get("geoL4List");

            String[] geoL1ListArray = new String[0];
            if (geoL1List != null && !geoL1List.trim().isEmpty()) {
                try {
                    String cleanedGeoL1List = geoL1List.replace("[", "").replace("]", "");
                    if (!cleanedGeoL1List.trim().isEmpty()) {
                        geoL1ListArray = Arrays.stream(cleanedGeoL1List.split(","))
                                .map(String::trim)
                                .filter(s -> !s.isEmpty())
                                .map(String::toUpperCase)
                                .toArray(String[]::new);
                    }
                } catch (Exception e) {
                    geoL1ListArray = new String[0];
                }
            }

            String[] geoL2ListArray = new String[0];
            if (geoL2List != null && !geoL2List.trim().isEmpty()) {
                try {
                    String cleanedGeoL2List = geoL2List.replace("[", "").replace("]", "");
                    if (!cleanedGeoL2List.trim().isEmpty()) {
                        geoL2ListArray = Arrays.stream(cleanedGeoL2List.split(","))
                                .map(String::trim)
                                .filter(s -> !s.isEmpty())
                                .map(String::toUpperCase)
                                .toArray(String[]::new);
                    }
                } catch (Exception e) {
                    geoL2ListArray = new String[0];
                }
            }

            String[] geoL3ListArray = new String[0];
            if (geoL3List != null && !geoL3List.trim().isEmpty()) {
                try {
                    String cleanedGeoL3List = geoL3List.replace("[", "").replace("]", "");
                    if (!cleanedGeoL3List.trim().isEmpty()) {
                        geoL3ListArray = Arrays.stream(cleanedGeoL3List.split(","))
                                .map(String::trim)
                                .filter(s -> !s.isEmpty())
                                .map(String::toUpperCase)
                                .toArray(String[]::new);
                    }
                } catch (Exception e) {
                    geoL3ListArray = new String[0];
                }
            }

            String[] geoL4ListArray = new String[0];
            if (geoL4List != null && !geoL4List.trim().isEmpty()) {
                try {
                    String cleanedGeoL4List = geoL4List.replace("[", "").replace("]", "");
                    if (!cleanedGeoL4List.trim().isEmpty()) {
                        geoL4ListArray = Arrays.stream(cleanedGeoL4List.split(","))
                                .map(String::trim)
                                .filter(s -> !s.isEmpty())
                                .map(String::toUpperCase)
                                .toArray(String[]::new);
                    }
                } catch (Exception e) {
                    geoL4ListArray = new String[0];
                }
            }

            // Added
            String mo = nodeAndAggregationDetails.get("mo");
            mo = mo != null ? mo.toUpperCase() : mo;
            boolean isMoEnabled = mo != null ? (mo.contains("INDIVIDUAL") ? true : false) : false;
            logger.info("Processing MO: {} And Is MO Enabled: {}", mo, isMoEnabled);
            String isNodeNameListEmpty = "";
            try {
                isNodeNameListEmpty = nodeAndAggregationDetails.get("isNodeNameListEmpty");
            } catch (Exception e) {
                logger.error("Exception While Fetching 'isNodeNameListEmpty' From nodeAndAggregationDetails: {}",
                        e.getMessage(), e);
                isNodeNameListEmpty = "true";
            }

            if ("false".equals(isNodeNameListEmpty)) {

                String nodeInfoMapJson = nodeAndAggregationDetails.get("nodeInfoMap");
                Map<String, String> nodeInfoMap = new ObjectMapper().readValue(nodeInfoMapJson,
                        new TypeReference<Map<String, String>>() {
                        });

                logger.info("[CustomCQLRead] Node Info Map: {}", nodeInfoMap);

                String[] nodeNameArray = nodeInfoMap.keySet().toArray(new String[0]);

                logger.info("[CustomCQLRead] Node Array: {}", Arrays.toString(nodeNameArray));
                if (isMoEnabled) {
                    String[] cellArray = getCellArrayFromNodeArray(nodeNameArray, jobContext);
                    logger.info("Cell Array: {}", Arrays.toString(cellArray));
                    nodeNameArray = cellArray;
                }

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;

                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodeNameArray,
                        jobContext, aggregationLevel, reportWidgetDetails, extraParameters);

                if (aggregationLevel != null && !aggregationLevel.isEmpty()) {
                    nodeAndAggregationDetails.put("aggregationLevel", aggregationLevel);
                    jobContext.setParameters("aggregationLevel", aggregationLevel);
                    logger.info("[CustomCQLRead] Aggregation Level '{}' Set to Job Context Successfully!",
                            aggregationLevel);
                }

                return cqlResultDataFrame;

            }

            if (geoL1.contains("CLUBBED") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 1: CLUBBED, CLUBBED, CLUBBED, CLUBBED, CLUBBED");

                aggregationLevel = "L0";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("CLUBBED") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 2: CLUBBED, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");

                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                // aggregationLevel = netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 3: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, CLUBBED");

                aggregationLevel = "L1";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 4: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 5: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED");

                aggregationLevel = "L2";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 6: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 7: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");

                aggregationLevel = "L3";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 8: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 9: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED");

                aggregationLevel = "L4";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 10: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 11: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, CLUBBED");

                aggregationLevel = "L1";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL1ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 12: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());

                if (nodesList.size() > 5000) {
                    logger.info(
                            "[CustomCQLRead] Nodes List Size is Greater than 5000. Splitting the List into Batches With Each Batch Size 5000");

                    List<List<String>> batches = splitListIntoBatches(nodesList, 5000);
                    logger.info("[CustomCQLRead] Batches Size: {}", batches.size());

                    for (int i = 0; i < batches.size(); i++) {
                        List<String> batch = batches.get(i);
                        String[] nodesArray = batch.toArray(new String[0]);

                        String batchNumber = String.format("BATCH-%02d", i + 1);
                        String banner = String.format("========== [ %s / %02d ] ==========",
                                batchNumber,
                                batches.size());

                        logger.info("\n{}", banner);
                        logger.info("[CustomCQLRead] Processing {} - Nodes Array Size: {}", batchNumber,
                                nodesArray.length);

                        Dataset<Row> subDf = getCQLDataOfSelectedNodenames(nodesArray, jobContext, netype,
                                reportWidgetDetails, extraParameters);

                        if (cqlResultDataFrame == null) {
                            cqlResultDataFrame = subDf;
                        } else {
                            cqlResultDataFrame = cqlResultDataFrame.union(subDf);
                        }
                    }

                } else {
                    logger.info("[CustomCQLRead] Nodes List Size is Less than 1000. Fetching Node Data.");

                    String[] nodesArray = nodesList.toArray(new String[0]);
                    cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesArray, jobContext, netype,
                            reportWidgetDetails,
                            extraParameters);
                }

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 13: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED");

                aggregationLevel = "L2";
                List<String> statesList = getAllStateOfSelectedRegions(geoL1ListArray, jobContext);
                logger.info("[CustomCQLRead] States List Size: {}", statesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(statesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 14: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 15: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");

                aggregationLevel = "L3";
                List<String> citiesList = getAllCityOfSelectedRegions(geoL1ListArray, jobContext);
                logger.info("[CustomCQLRead] Cities List Size: {}", citiesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(citiesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 16: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 17: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");

                aggregationLevel = "L4";
                List<String> clustersList = getAllCusterOfSelectedRegions(geoL1ListArray, jobContext);
                logger.info("[CustomCQLRead] Clusters List Size: {}", clustersList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(clustersList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 18: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 19: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, CLUBBED");

                aggregationLevel = "L2";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL2ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 20: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL2ListArray, jobContext, "L2",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 21: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED");

                aggregationLevel = "L3";
                List<String> citiesList = getAllCityOfSelectedStates(geoL2ListArray, jobContext);
                logger.info("[CustomCQLRead] Cities List Size: {}", citiesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(citiesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 22: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL2ListArray, jobContext, "L2",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 23: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED");

                aggregationLevel = "L3";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL3ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 24: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL3ListArray, jobContext, "L3",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 25: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED");

                aggregationLevel = "L4";
                List<String> clustersList = getAllCusterOfSelectedCity(geoL3ListArray, jobContext);
                logger.info("[CustomCQLRead] Clusters List Size: {}", clustersList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(clustersList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info(
                        "[CustomCQLRead] CASE 26: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL3ListArray, jobContext, "L3",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect && isGeoL4MultiSelect
                    && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 27: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED");

                aggregationLevel = "L4";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL4ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect && isGeoL4MultiSelect
                    && node.contains("INDIVIDUAL")) {

                logger.info(
                        "[CustomCQLRead] CASE 28: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL4ListArray, jobContext, "L4",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);
            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("[CustomCQLRead] CASE 29: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED");

                aggregationLevel = "L4";
                List<String> clustersList = getAllCusterOfSelectedStates(geoL2ListArray, jobContext);
                logger.info("[CustomCQLRead] Clusters List Size: {}", clustersList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(clustersList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info("[CustomCQLRead] CASE 30: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");

                // aggregationLevel = netype;
                aggregationLevel = isMoEnabled ? "INTERFACE" : netype;
                List<String> nodeList = getAllNodesForSelectedGeography(geoL2ListArray, jobContext, "L2",
                        reportWidgetDetails);
                logger.info("[CustomCQLRead] Nodes List Size: {}", nodeList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodeList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters);

            } else {
                logger.info("[CustomCQLRead] =========This Case In Not Implemented==========");
            }
        } catch (Exception e) {
            logger.error(
                    "[CustomCQLRead] Error in Getting Result of Node and Aggregation Details, Message: {}, Error: {}",
                    e.getMessage(), e);
        }

        if (aggregationLevel != null && !aggregationLevel.isEmpty()) {
            nodeAndAggregationDetails.put("aggregationLevel", aggregationLevel);
            jobContext.setParameters("aggregationLevel", aggregationLevel);
            logger.info("[CustomCQLRead] Aggregation Level '{}' Set to Job Context Successfully!", aggregationLevel);
        }

        return cqlResultDataFrame;
    }

    private static List<String> getAllCusterOfSelectedStates(String[] geoL2ListArray, JobContext jobContext) {

        try {

            String inClause = Arrays.stream(geoL2ListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String mysqlQuery = """
                        SELECT DISTINCT UPPER(l4.GEO_NAME) AS CLUSTER
                        FROM PRIMARY_GEO_L4 l4
                        JOIN PRIMARY_GEO_L3 l3 ON l4.PRIMARY_GEO_L3_ID_FK = l3.ID
                        JOIN PRIMARY_GEO_L2 l2 ON l3.PRIMARY_GEO_L2_ID_FK = l2.ID
                        WHERE UPPER(l2.GEO_NAME) IN (%s)
                    """.formatted(inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_CLUSTER_FOUND"), Encoders.STRING())
                        .toDF("CLUSTER");
            }

            List<String> clustersList = df.as(Encoders.STRING()).collectAsList();

            if (clustersList.isEmpty()) {
                clustersList.add("NO_CLUSTER_FOUND");
            }

            return clustersList;
        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All Cluster of Selected Regions, Message: {}, Error: {}",
                    e.getMessage(), e);
            return Collections.singletonList("NO_CLUSTER_FOUND");
        }
    }

    private static List<String> getAllCityOfSelectedStates(String[] geoL2ListArray, JobContext jobContext) {

        try {
            String inClause = Arrays.stream(geoL2ListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String mysqlQuery = """
                        SELECT DISTINCT UPPER(l3.GEO_NAME) AS CITY
                        FROM PRIMARY_GEO_L3 l3
                        JOIN PRIMARY_GEO_L2 l2 ON l3.PRIMARY_GEO_L2_ID_FK = l2.ID
                        WHERE UPPER(l2.GEO_NAME) IN (%s)
                    """.formatted(inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_CITY_FOUND"), Encoders.STRING())
                        .toDF("CITY");
            }

            List<String> citiesList = df.as(Encoders.STRING()).collectAsList();

            if (citiesList.isEmpty()) {
                citiesList.add("NO_CITY_FOUND");
            }

            return citiesList;

        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All City of Selected States, Message: {}, Error: {}",
                    e.getMessage(), e);
            return Collections.singletonList("NO_CITY_FOUND");
        }
    }

    private static List<String> getAllCusterOfSelectedRegions(String[] geoL1ListArray, JobContext jobContext) {

        try {

            String inClause = Arrays.stream(geoL1ListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String mysqlQuery = """
                        SELECT DISTINCT UPPER(l4.GEO_NAME) AS CLUSTER
                        FROM PRIMARY_GEO_L4 l4
                        JOIN PRIMARY_GEO_L3 l3 ON l4.PRIMARY_GEO_L3_ID_FK = l3.ID
                        JOIN PRIMARY_GEO_L2 l2 ON l3.PRIMARY_GEO_L2_ID_FK = l2.ID
                        JOIN PRIMARY_GEO_L1 l1 ON l2.PRIMARY_GEO_L1_ID_FK = l1.ID
                        WHERE UPPER(l1.GEO_NAME) IN (%s)
                    """.formatted(inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_CLUSTER_FOUND"), Encoders.STRING())
                        .toDF("CLUSTER");
            }

            List<String> clustersList = df.as(Encoders.STRING()).collectAsList();

            if (clustersList.isEmpty()) {
                clustersList.add("NO_CLUSTER_FOUND");
            }

            return clustersList;
        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All Cluster of Selected Regions, Message: {}, Error: {}",
                    e.getMessage(), e);
            return Collections.singletonList("NO_CLUSTER_FOUND");
        }
    }

    private static List<String> getAllCityOfSelectedRegions(String[] geoL1ListArray, JobContext jobContext) {

        try {

            String inClause = Arrays.stream(geoL1ListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String mysqlQuery = """
                        SELECT DISTINCT UPPER(l3.GEO_NAME) AS CITY
                        FROM PRIMARY_GEO_L3 l3
                        JOIN PRIMARY_GEO_L2 l2 ON l3.PRIMARY_GEO_L2_ID_FK = l2.ID
                        JOIN PRIMARY_GEO_L1 l1 ON l2.PRIMARY_GEO_L1_ID_FK = l1.ID
                        WHERE UPPER(l1.GEO_NAME) IN (%s)
                    """.formatted(inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_NODE_FOUND"), Encoders.STRING())
                        .toDF("NODE");
            }

            List<String> citiesList = df.as(Encoders.STRING()).collectAsList();

            if (citiesList.isEmpty()) {
                citiesList.add("NO_CITY_FOUND");
            }

            return citiesList;

        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All City of Selected Regions, Message: {}, Error: {}",
                    e.getMessage(), e);
            return Collections.singletonList("NO_CITY_FOUND");
        }
    }

    private static List<String> getAllCusterOfSelectedCity(String[] geoL3ListArray, JobContext jobContext) {
        try {
            String inClause = Arrays.stream(geoL3ListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String mysqlQuery = """
                        SELECT DISTINCT UPPER(l4.GEO_NAME) AS CLUSTER
                        FROM PRIMARY_GEO_L4 l4
                        JOIN PRIMARY_GEO_L3 l3 ON l4.PRIMARY_GEO_L3_ID_FK = l3.ID
                        WHERE UPPER(l3.GEO_NAME) IN (%s)
                    """.formatted(inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_CLUSTER_FOUND"), Encoders.STRING())
                        .toDF("CLUSTER");
            }

            List<String> clustersList = df.as(Encoders.STRING()).collectAsList();
            return clustersList;

        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All Cluster of Selected City, Message: {}, Error: {}",
                    e.getMessage(), e);
            return Collections.singletonList("NO_CLUSTER_FOUND");
        }
    }

    private static List<String> getAllStateOfSelectedRegions(String[] geoL1ListArray, JobContext jobContext) {
        try {
            String inClause = Arrays.stream(geoL1ListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String mysqlQuery = """
                        SELECT DISTINCT UPPER(l2.GEO_NAME) AS STATE
                        FROM PRIMARY_GEO_L2 l2
                        JOIN PRIMARY_GEO_L1 l1 ON l2.PRIMARY_GEO_L1_ID_FK = l1.ID
                        WHERE UPPER(l1.GEO_NAME) IN (%s)
                    """.formatted(inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_STATE_FOUND"), Encoders.STRING())
                        .toDF("STATE");
            }

            List<String> statesList = df.as(Encoders.STRING()).collectAsList();
            return statesList;

        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All State of Selected Regions, Message: {}, Error: {}",
                    e.getMessage(), e);
            return Collections.singletonList("NO_STATE_FOUND");
        }
    }

    private static List<String> getAllNodesForSelectedGeography(String[] geoListArray, JobContext jobContext,
            String aggregationLevel, Map<String, String> reportWidgetDetails) {

        String domain = reportWidgetDetails.get("domain");
        String vendor = reportWidgetDetails.get("vendor");
        String technology = reportWidgetDetails.get("technology");

        List<String> nodesList = new ArrayList<>();

        try {
            if (geoListArray == null || geoListArray.length == 0) {
                logger.error("[CustomCQLRead] Geo List is Empty. Skipping Node Fetch.");
                return Collections.singletonList("NO_NODE_FOUND");
            }

            String inClause = Arrays.stream(geoListArray)
                    .map(String::toUpperCase)
                    .map(name -> "'" + name + "'")
                    .collect(Collectors.joining(", "));

            String geoTable = null;
            String geoFkColumn = null;

            switch (aggregationLevel.toUpperCase()) {
                case "L1" -> {
                    geoTable = "PRIMARY_GEO_L1";
                    geoFkColumn = "GEOGRAPHY_L1_ID_FK";
                }
                case "L2" -> {
                    geoTable = "PRIMARY_GEO_L2";
                    geoFkColumn = "GEOGRAPHY_L2_ID_FK";
                }
                case "L3" -> {
                    geoTable = "PRIMARY_GEO_L3";
                    geoFkColumn = "GEOGRAPHY_L3_ID_FK";
                }
                case "L4" -> {
                    geoTable = "PRIMARY_GEO_L4";
                    geoFkColumn = "GEOGRAPHY_L4_ID_FK";
                }
                default -> {
                    logger.error("[CustomCQLRead] Invalid Aggregation Level: {}", aggregationLevel);
                    return Collections.singletonList("NO_NODE_FOUND");
                }
            }

            String mysqlQuery = String.format(
                    """
                                SELECT DISTINCT UPPER(ne.NE_ID) AS NODE
                                FROM NETWORK_ELEMENT ne
                                JOIN %s geo ON ne.%s = geo.ID
                                WHERE ne.DOMAIN = '%s' AND ne.VENDOR = '%s' AND ne.TECHNOLOGY = '%s' AND ne.NE_ID IS NOT NULL AND UPPER(geo.GEO_NAME) IN (%s)
                            """,
                    geoTable, geoFkColumn, domain, vendor, technology, inClause);

            logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_NODE_FOUND"), Encoders.STRING())
                        .toDF("NODE");
            }

            nodesList = df.as(Encoders.STRING()).collectAsList();

            return nodesList;

        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting All Nodes for Selected Geography, Message: {}, Error: {}",
                    e.getMessage(),
                    e);
            return Collections.singletonList("NO_NODE_FOUND");
        }

    }

    private static Dataset<Row> getCQLDataOfSelectedNodenames(String[] geoListArray, JobContext jobContext,
            String aggregationLevel, Map<String, String> reportWidgetDetails, Map<String, String> extraParameters) {

        String domain = reportWidgetDetails.get("domain").toUpperCase();
        String vendor = reportWidgetDetails.get("vendor").toUpperCase();
        String technology = reportWidgetDetails.get("technology").toUpperCase();
        String startTime = extraParameters.get("startTime");
        String endTime = extraParameters.get("endTime");
        String netype = reportWidgetDetails.get("netype");
        netype = (netype != null && !netype.isEmpty() && !netype.equals("null")) ? netype : aggregationLevel;
        String datalevel = "";

        StringBuilder mysqlQuery = new StringBuilder()
                .append("SELECT DISTINCT CONCAT(TECHNOLOGY, IF(ROWKEY_TECHNOLOGY IS NOT NULL, '_', ''), ")
                .append("COALESCE(NETWORK_TYPE, '')) AS rowKeyAppender ")
                .append("FROM PM_NODE_VENDOR ")
                .append("WHERE domain = '").append(domain).append("' ")
                .append("AND vendor = '").append(vendor).append("' ")
                .append("AND technology = '").append(technology).append("'");

        logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery.toString());

        Dataset<Row> df = executeQuery(mysqlQuery.toString(), jobContext);

        String dataLevelAppender = df.as(Encoders.STRING()).collectAsList().get(0);
        logger.info("[CustomCQLRead] Data Level Appender: {}", dataLevelAppender);

        if (aggregationLevel.equals("L0")) {
            datalevel = "L0" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L1")) {
            datalevel = "L1" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L2")) {
            datalevel = "L2" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L3")) {
            datalevel = "L3" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L4")) {
            datalevel = "L4" + "_" + dataLevelAppender;
        } else {
            datalevel = netype + "_" + dataLevelAppender;
        }

        if (vendor.equalsIgnoreCase("ALL")) {
            switch (aggregationLevel) {
                case "L0" -> datalevel = "L0" + "_" + technology;
                case "L1" -> datalevel = "L1" + "_" + technology;
                case "L2" -> datalevel = "L2" + "_" + technology;
                case "L3" -> datalevel = "L3" + "_" + technology;
                case "L4" -> datalevel = "L4" + "_" + technology;
                default -> datalevel = "L0" + "_" + technology;
            }
        }

        logger.info("[CustomCQLRead] Generated Data Level: {}", datalevel);

        List<String> dateList = getDateList(startTime, endTime, reportWidgetDetails);

        String inClause = Arrays.stream(geoListArray)
                .map(name -> "'" + name.replace("'", "''") + "'")
                .collect(Collectors.joining(", "));

        inClause = inClause.isBlank() ? "'NO_NODE_REQUESTED'" : inClause;

        String isSpecificDate = extraParameters.get("isSpecificDate");

        String cqlFilter = "";

        if (isSpecificDate.equals("true")) {

            String timestampList = extraParameters.get("dateList");

            List<String> dateListFromEpoch = getDateListFromEpoch(timestampList, reportWidgetDetails);
            logger.info("[CustomCQLRead] Date List From Epoch: {}", dateListFromEpoch);

            String dateInClause = dateListFromEpoch.stream()
                    .map(date -> "'" + date + "'")
                    .collect(Collectors.joining(", "));

            String formattedTimestampList = Arrays.stream(timestampList.split(","))
                    .map(timestamp -> String.format("TIMESTAMP '%s'",
                            LocalDateTime.ofEpochSecond(Long.parseLong(timestamp.trim()), 0, ZoneOffset.UTC)
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))
                    .collect(Collectors.joining(", "));

            cqlFilter = String.format(
                    "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND timestamp IN (%s) AND date IN (%s) AND nodename IN (%s)",
                    domain, vendor, technology, datalevel, formattedTimestampList, dateInClause, inClause);

        } else {

            String generatedType = reportWidgetDetails.get("generatedType");

            if (generatedType.equalsIgnoreCase("SCHEDULED")) {
                logger.info("[CustomCQLRead] Generated Type: {}", generatedType);

                String scheduleTimestampFilter = extraParameters.get("scheduleTimestampFilter");
                String scheduleDateFilter = extraParameters.get("scheduleDateFilter");

                logger.info("[CustomCQLRead] Schedule Timestamp Filter: {}", scheduleTimestampFilter);
                logger.info("[CustomCQLRead] Schedule Date Filter: {}", scheduleDateFilter);

                if (aggregationLevel.equals("L0")) {
                    String nodename = "India";
                    cqlFilter = String.format(
                            "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND nodename = '%s' AND timestamp IN (%s) AND date IN (%s)",
                            domain, vendor, technology, datalevel, nodename, scheduleTimestampFilter,
                            scheduleDateFilter);
                } else {
                    cqlFilter = String.format(
                            "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND timestamp IN (%s) AND date IN (%s) AND nodename IN (%s)",
                            domain, vendor, technology, datalevel, scheduleTimestampFilter, scheduleDateFilter,
                            inClause);
                }

            } else {
                String dateInClause = dateList.stream()
                        .map(date -> "'" + date + "'")
                        .collect(Collectors.joining(", "));

                cqlFilter = String.format(
                        "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND timestamp <= TIMESTAMP '%s' AND timestamp >= TIMESTAMP '%s' AND date IN (%s) AND nodename IN (%s)",
                        domain, vendor, technology, datalevel, endTime, startTime, dateInClause, inClause);
            }
        }

        logger.info("[CustomCQLRead-Updated] CQL Filter With Nodename: {}", cqlFilter);

        Dataset<Row> cqlDataDF = getCQLDataUsingSpark(cqlFilter, jobContext, reportWidgetDetails);

        return cqlDataDF;
    }

    private static Dataset<Row> getCQLDataForSelectedLevel(JobContext jobContext, String aggregationLevel,
            Map<String, String> reportWidgetDetails, Map<String, String> extraParameters,
            Map<String, String> nodeAndAggregationDetails) {

        Dataset<Row> df = null;

        String domain = reportWidgetDetails.get("domain").toUpperCase();
        String vendor = reportWidgetDetails.get("vendor").toUpperCase();
        String technology = reportWidgetDetails.get("technology").toUpperCase();
        String startTime = extraParameters.get("startTime");
        String endTime = extraParameters.get("endTime");
        String netype = nodeAndAggregationDetails.get("netype");

        logger.info(
                "[CustomCQLRead] Getting CQL Data for Selected Level with Parameters - Domain: {}, Vendor: {}, Technology: {}, Start Time: {}, End Time: {}",
                domain, vendor, technology, startTime, endTime);

        StringBuilder mysqlQuery = new StringBuilder()
                .append("SELECT DISTINCT CONCAT(TECHNOLOGY, IF(ROWKEY_TECHNOLOGY IS NOT NULL, '_', ''), ")
                .append("COALESCE(NETWORK_TYPE, '')) AS rowKeyAppender ")
                .append("FROM PM_NODE_VENDOR ")
                .append("WHERE domain = '").append(domain).append("' ")
                .append("AND vendor = '").append(vendor).append("' ")
                .append("AND technology = '").append(technology).append("'");

        logger.info("[CustomCQLRead] MySQL Query: {}", mysqlQuery.toString());

        Dataset<Row> rowKeyAppenderDF = executeQuery(mysqlQuery.toString(), jobContext);

        String dataLevelAppender = "";
        List<String> appenderList = rowKeyAppenderDF.as(Encoders.STRING()).collectAsList();
        if (appenderList != null && !appenderList.isEmpty()) {
            dataLevelAppender = appenderList.get(0);
        } else {
            logger.error(
                    "[CustomCQLRead] No Data Level Appender Found. Please Ensure Data Level Appender is Present in the DataFrame Before Processing.");
        }

        logger.info("[CustomCQLRead] Data Level Appender: {}", dataLevelAppender);

        String datalevel = "";
        logger.info("[CustomCQLRead] Aggregation Level: {}", aggregationLevel);

        if (aggregationLevel.equals("L0")) {
            datalevel = "L0" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L1")) {
            datalevel = "L1" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L2")) {
            datalevel = "L2" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L3")) {
            datalevel = "L3" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("L4")) {
            datalevel = "L4" + "_" + dataLevelAppender;
        } else if (aggregationLevel.equals("INTERFACE")) {
            datalevel = "INTERFACE" + "_" + dataLevelAppender;
        } else {
            datalevel = netype + "_" + dataLevelAppender;
        }

        if (vendor.equalsIgnoreCase("ALL")) {
            switch (aggregationLevel) {
                case "L0" -> datalevel = "L0" + "_" + technology;
                case "L1" -> datalevel = "L1" + "_" + technology;
                case "L2" -> datalevel = "L2" + "_" + technology;
                case "L3" -> datalevel = "L3" + "_" + technology;
                case "L4" -> datalevel = "L4" + "_" + technology;
                default -> datalevel = "L0" + "_" + technology;
            }
        }

        logger.info("[CustomCQLRead] Getting CQL Data for Selected Level with Data Level: {}", datalevel);

        try {

            String isSpecificDate = extraParameters.get("isSpecificDate");

            String cqlFilter = "";
            if (isSpecificDate.equals("true")) {

                String timestampList = extraParameters.get("dateList");

                List<String> dateListFromEpoch = getDateListFromEpoch(timestampList, reportWidgetDetails);
                logger.info("[CustomCQLRead] Date List From Epoch: {}", dateListFromEpoch);

                String dateInClause = dateListFromEpoch.stream()
                        .map(date -> "'" + date + "'")
                        .collect(Collectors.joining(", "));

                String formattedTimestampList = Arrays.stream(timestampList.split(","))
                        .map(timestamp -> String.format("TIMESTAMP '%s'",
                                LocalDateTime.ofEpochSecond(Long.parseLong(timestamp.trim()), 0, ZoneOffset.UTC)
                                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))
                        .collect(Collectors.joining(", "));

                cqlFilter = String.format(
                        "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND timestamp IN (%s) AND date IN (%s)",
                        domain, vendor, technology, datalevel, formattedTimestampList, dateInClause);

            } else {

                String generatedType = reportWidgetDetails.get("generatedType");

                if (generatedType.equalsIgnoreCase("SCHEDULED")) {

                    logger.info("[CustomCQLRead] Generated Type: {}", generatedType);

                    String scheduleTimestampFilter = extraParameters.get("scheduleTimestampFilter");
                    String scheduleDateFilter = extraParameters.get("scheduleDateFilter");

                    logger.info("[CustomCQLRead] Schedule Timestamp Filter: {}", scheduleTimestampFilter);
                    logger.info("[CustomCQLRead] Schedule Date Filter: {}", scheduleDateFilter);

                    if (aggregationLevel.equals("L0")) {
                        String nodename = "India";
                        cqlFilter = String.format(
                                "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND nodename = '%s' AND timestamp IN (%s) AND date IN (%s)",
                                domain, vendor, technology, datalevel, nodename, scheduleTimestampFilter,
                                scheduleDateFilter);
                    } else {
                        cqlFilter = String.format(
                                "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND timestamp IN (%s) AND date IN (%s)",
                                domain, vendor, technology, datalevel, scheduleTimestampFilter, scheduleDateFilter);
                    }

                } else {
                    List<String> dateList = getDateList(startTime, endTime, reportWidgetDetails);

                    String dateInClause = dateList.stream()
                            .map(date -> "'" + date + "'")
                            .collect(Collectors.joining(", "));

                    cqlFilter = String.format(
                            "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND timestamp <= TIMESTAMP '%s' AND timestamp >= TIMESTAMP '%s' AND date IN (%s)",
                            domain, vendor, technology, datalevel, endTime, startTime, dateInClause);
                }

            }

            logger.info("[CustomCQLRead] CQL Filter: {}", cqlFilter);

            df = getCQLDataUsingSpark(cqlFilter, jobContext, reportWidgetDetails);

        } catch (Exception e) {
            logger.error("[CustomCQLRead] Error in Getting CQL Data for Selected Level, Message: {}, Error: {}",
                    e.getMessage(), e);
        }

        return df;
    }

    private static Dataset<Row> executeQuery(String sqlQuery, JobContext jobContext) {

        Dataset<Row> resultDataset = null;

        try {
            resultDataset = jobContext.sqlctx().read()
                    .format("jdbc")
                    .option("driver", SPARK_PM_JDBC_DRIVER)
                    .option("url", SPARK_PM_JDBC_URL)
                    .option("user", SPARK_PM_JDBC_USERNAME)
                    .option("password", SPARK_PM_JDBC_PASSWORD)
                    .option("query", sqlQuery)
                    .load();

            return resultDataset;

        } catch (Exception e) {
            logger.error("Exception in Executing Query, Message: " + e.getMessage() + " | Error: " + e);
            return resultDataset;
        }

    }

    private JobContext setSparkConf(JobContext jobContext) {
        jobContext.sqlctx().setConf("spark.sql.caseSensitive", "true");
        jobContext.sqlctx().setConf("spark.cassandra.connection.localDC", SPARK_CASSANDRA_DATACENTER);
        jobContext.sqlctx().setConf("spark.cassandra.connection.host", SPARK_CASSANDRA_HOST);
        jobContext.sqlctx().setConf("spark.cassandra.connection.port", SPARK_CASSANDRA_PORT);
        jobContext.sqlctx().setConf("spark.cassandra.auth.username", SPARK_CASSANDRA_USERNAME);
        jobContext.sqlctx().setConf("spark.cassandra.auth.password", SPARK_CASSANDRA_PASSWORD);
        jobContext.sqlctx().setConf("spark.sql.catalog.ybcatalog",
                "com.datastax.spark.connector.datasource.CassandraCatalog");
        jobContext.sqlctx().setConf("spark.cassandra.output.ignoreNulls", "true");
        jobContext.sqlctx().setConf("spark.cassandra.input.consistency.level", "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.output.consistency.level", "ONE");
        jobContext.sqlctx().setConf("spark.cassandra.query.retry.count", "10");
        jobContext.sqlctx().setConf("spark.cassandra.output.batch.size.rows", "500");
        jobContext.sqlctx().setConf("spark.cassandra.output.concurrent.writes", "3");
        jobContext.sqlctx().setConf("spark.cassandra.connection.remoteConnectionsPerExecutor", "5");
        jobContext.sqlctx().setConf("spark.jdbc.url", SPARK_PM_JDBC_URL);
        jobContext.sqlctx().setConf("spark.jdbc.user", SPARK_PM_JDBC_USERNAME);
        jobContext.sqlctx().setConf("spark.jdbc.password", SPARK_PM_JDBC_PASSWORD);
        return jobContext;
    }

    private static Dataset<Row> getCQLDataUsingSpark(String cqlFilter, JobContext jobContext,
            Map<String, String> reportWidgetDetails) {

        String frequency = reportWidgetDetails.get("frequency");
        String cqlTableName = generateCQLTableNameBasedOnFrequency(frequency);
        logger.info("[CustomCQLRead] CQL Table Name: {}", cqlTableName);
        String cqlConsistencyLevel = "ONE";

        Dataset<Row> resultDataFrame = null;

        int maxRetries = 5;
        int currentRetry = 0;
        boolean success = false;

        while (!success && currentRetry < maxRetries) {
            try {

                if (currentRetry > 0) {
                    long backoffMs = (long) Math.pow(2, currentRetry) * 1000;
                    logger.info("[CustomCQLRead] Retry Attempt {} - Waiting {} Milliseconds Before Next Attempt",
                            currentRetry,
                            backoffMs);
                    Thread.sleep(backoffMs);
                }

                logger.info("[CustomCQLRead] Applied CQL Read Parameters: {}",
                        Map.of(
                                "table", cqlTableName,
                                "keyspace", SPARK_CASSANDRA_KEYSPACE_PM,
                                "pushdown", "true",
                                "consistency.level", cqlConsistencyLevel));

                resultDataFrame = jobContext.sqlctx().read()
                        .format("org.apache.spark.sql.cassandra")
                        .options(Map.of(
                                "table", cqlTableName,
                                "keyspace", SPARK_CASSANDRA_KEYSPACE_PM,
                                "pushdown", "true",
                                "consistency.level", cqlConsistencyLevel))
                        .load()
                        .filter(cqlFilter);

                // resultDataFrame = resultDataFrame.cache();
                success = true;

            } catch (Exception e) {
                currentRetry++;
                logger.error(
                        "[CustomCQLRead] Attempt {} Failed: Error in Getting CQL Data Using Spark, Message: {}, Error: {}",
                        currentRetry, e.getMessage(), e);

                if (currentRetry < maxRetries) {
                    logger.info("[CustomCQLRead] Retrying Query... (Attempt {}/{})", currentRetry + 1, maxRetries);
                } else {
                    logger.error("[CustomCQLRead] All Retry Attempts Failed for Query: {}", cqlFilter);
                    throw new RuntimeException("[CustomCQLRead] Failed to Execute Cassandra Query After " + maxRetries
                            + " Attempts",
                            e);
                }
            }
        }
        return resultDataFrame;
    }

    private static List<String> getDateList(String startTime, String endTime, Map<String, String> reportWidgetDetails) {

        logger.info("[CustomCQLRead] Getting Date List with Start Time: {}, End Time: {}", startTime, endTime);

        if (startTime.isEmpty() || endTime.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> dateList = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSX");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        if (reportWidgetDetails.get("frequency").equalsIgnoreCase("PERDAY")) {
            outputFormatter = DateTimeFormatter.ofPattern("yyMMdd");
        }

        ZonedDateTime start = ZonedDateTime.parse(startTime, formatter);
        ZonedDateTime end = ZonedDateTime.parse(endTime, formatter);

        if (start.isAfter(end)) {
            ZonedDateTime temp = start;
            start = end;
            end = temp;
        }

        LocalDate startDate = start.toLocalDate();
        LocalDate endDate = end.toLocalDate();
        while (!startDate.isAfter(endDate)) {
            dateList.add(startDate.format(outputFormatter));
            startDate = startDate.plusDays(1);
        }

        logger.info("[CustomCQLRead] Date List: {}", dateList);
        return dateList;
    }

    private static List<String> getDateListFromEpoch(String inputEpochList, Map<String, String> reportWidgetDetails) {

        logger.info("[CustomCQLRead] Getting Date List From Epoch: {}", inputEpochList);

        Set<String> uniqueDates = new HashSet<>();

        String[] epochList = inputEpochList.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        if (reportWidgetDetails.get("frequency").equalsIgnoreCase("PERDAY")) {
            formatter = DateTimeFormatter.ofPattern("yyMMdd");
        }

        for (String epoch : epochList) {
            String formattedDate = LocalDateTime.ofEpochSecond(Long.parseLong(epoch.trim()), 0, ZoneOffset.UTC)
                    .format(formatter);
            uniqueDates.add(formattedDate);
        }

        logger.info("[CustomCQLRead] Unique Dates: {}", uniqueDates);
        return new ArrayList<>(uniqueDates);
    }

    private static List<List<String>> splitListIntoBatches(List<String> list, int batchSize) {

        logger.info("[CustomCQLRead] Splitting List Into Batches: {}", list);

        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(list.size(), i + batchSize);
            batches.add(list.subList(i, end));
        }

        logger.info("[CustomCQLRead] Batches: {}", batches);
        return batches;
    }

    private static String generateCQLTableNameBasedOnFrequency(String frequency) {

        logger.info("[CustomCQLRead] Generating CQL Table Name Based On Frequency: {}", frequency);

        return switch (frequency.toUpperCase()) {
            case "5 MIN" -> "combine5minpm";
            case "FIVEMIN" -> "combine5minpm";
            case "15 MIN" -> "combinequarterlypm";
            case "QUARTERLY" -> "combinequarterlypm";
            case "HOURLY" -> "combinehourlypm";
            case "PERHOUR" -> "combinehourlypm";
            case "DAILY" -> "combinedailypm";
            case "PERDAY" -> "combinedailypm";
            default -> "combinequarterlypm";
        };
    }
}
