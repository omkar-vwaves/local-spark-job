package com.enttribe.pm.job.alert.loop;

import com.enttribe.sparkrunner.processors.Processor;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CloseAlertCQLRead extends Processor {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(CloseAlertCQLRead.class);
    private static Map<String, String> jobContextMap = new HashMap<>();

    private static final String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static final String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static final String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static final String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";
    private static final String SPARK_CASSANDRA_KEYSPACE_PM = "SPARK_CASSANDRA_KEYSPACE_PM"; //
    private static final String SPARK_CASSANDRA_HOST = "SPARK_CASSANDRA_HOST";
    private static final String SPARK_CASSANDRA_PORT = "SPARK_CASSANDRA_PORT";
    private static final String SPARK_CASSANDRA_DATACENTER = "SPARK_CASSANDRA_DATACENTER";
    private static final String SPARK_CASSANDRA_USERNAME = "SPARK_CASSANDRA_USERNAME";
    private static final String SPARK_CASSANDRA_PASSWORD = "SPARK_CASSANDRA_PASSWORD";

    private static String sparkPMJdbcDriver = null;
    private static String sparkPMJdbcUrl = null;
    private static String sparkPMJdbcUsername = null;
    private static String sparkPMJdbcPassword = null;
    private static String sparkCassandraKeyspacePM = null;
    private static String sparkCassandraHost = null;
    private static String sparkCassandraPort = null;
    private static String sparkCassandraDatacenter = null;
    private static String sparkCassandraUsername = null;
    private static String sparkCassandraPassword = null;

    public CloseAlertCQLRead() {
        super();
        logger.info("CloseAlertCQLRead No Argument Constructor Called!");
    }

    public CloseAlertCQLRead(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("CloseAlertCQLRead Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public CloseAlertCQLRead(Integer id, String processorName) {
        super(id, processorName);
        logger.info("CloseAlertCQLRead Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (this.dataFrame == null || this.dataFrame.isEmpty()) {
            return this.dataFrame;
        }

        long startTime = System.currentTimeMillis();

        logger.info("[CloseAlertCQLRead] Execution Started!");

        if (jobContext != null && jobContext.getParameters() != null) {
            for (Map.Entry<String, String> entry : jobContext.getParameters().entrySet()) {
                jobContextMap.put(entry.getKey(), entry.getValue());
            }
        }
        sparkPMJdbcDriver = jobContextMap.get(SPARK_PM_JDBC_DRIVER);
        sparkPMJdbcUrl = jobContextMap.get(SPARK_PM_JDBC_URL);
        sparkPMJdbcUsername = jobContextMap.get(SPARK_PM_JDBC_USERNAME);
        sparkPMJdbcPassword = jobContextMap.get(SPARK_PM_JDBC_PASSWORD);
        sparkCassandraKeyspacePM = jobContextMap.get(SPARK_CASSANDRA_KEYSPACE_PM);
        sparkCassandraHost = jobContextMap.get(SPARK_CASSANDRA_HOST);
        sparkCassandraPort = jobContextMap.get(SPARK_CASSANDRA_PORT);
        sparkCassandraDatacenter = jobContextMap.get(SPARK_CASSANDRA_DATACENTER);
        sparkCassandraUsername = jobContextMap.get(SPARK_CASSANDRA_USERNAME);
        sparkCassandraPassword = jobContextMap.get(SPARK_CASSANDRA_PASSWORD);

        logger.info("JDBC Credentials: Driver={}, URL={}, User={}, Password={}",
                sparkPMJdbcDriver,
                sparkPMJdbcUrl,
                sparkPMJdbcUsername,
                sparkPMJdbcPassword);

        logger.info("Cassandra Credentials: Keyspace={}, Host={}, Port={}, Datacenter={}, Username={}, Password={}",
                sparkCassandraKeyspacePM,
                sparkCassandraHost,
                sparkCassandraPort,
                sparkCassandraDatacenter,
                sparkCassandraUsername,
                sparkCassandraPassword);

        jobContext = setSparkConf(jobContext);

        String inputConfig = null;
        String extractedParameters = null;
        String nodeAndAggregationDetails = null;
        String kpiCodeNameMapJson = null;

        String CURRENT_COUNT = jobContext.getParameter("CURRENT_COUNT");
        logger.info("[CloseAlertCQLRead] CURRENT_COUNT={}", CURRENT_COUNT);

        inputConfig = jobContext.getParameter("INPUT_CONFIGURATIONS" + CURRENT_COUNT);
        nodeAndAggregationDetails = jobContext.getParameter("NODE_AND_AGGREGATION_DETAILS" + CURRENT_COUNT);
        extractedParameters = jobContext.getParameter("EXTRACTED_PARAMETERS" + CURRENT_COUNT);
        kpiCodeNameMapJson = jobContext.getParameter("KPI_CODE_NAME_MAP" + CURRENT_COUNT);

        Map<String, String> inputConfigMap = new ObjectMapper().readValue(inputConfig,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(nodeAndAggregationDetails,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extractedParameters,
                new TypeReference<Map<String, String>>() {
                });
        Map<String, String> kpiCodeNameMap = new ObjectMapper().readValue(kpiCodeNameMapJson,
                new TypeReference<Map<String, String>>() {
                });

        logger.info("[CloseAlertCQLRead] Input Config Map[{}]: {}", CURRENT_COUNT, inputConfigMap);
        logger.info("[CloseAlertCQLRead] Node And Aggregation Details Map[{}]: {}", CURRENT_COUNT,
                nodeAndAggregationDetailsMap);
        logger.info("[CloseAlertCQLRead] Extra Parameters Map[{}]: {}", CURRENT_COUNT, extraParametersMap);
        logger.info("[CloseAlertCQLRead] KPI Code Name Map[{}]: {}", CURRENT_COUNT, kpiCodeNameMap);

        Dataset<Row> cqlResultDataFrame = getResultOfNodeAndAggregationDetails(nodeAndAggregationDetailsMap,
                inputConfigMap, extraParametersMap, jobContext);

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        cqlResultDataFrame.show();
        logger.info("++++++[CloseAlertCQLRead] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes,
                seconds);

        return cqlResultDataFrame;
    }

    private static Dataset<Row> getResultOfNodeAndAggregationDetails(
            Map<String, String> nodeAndAggregationDetails, Map<String, String> reportWidgetDetails,
            Map<String, String> extraParameters, JobContext jobContext) {

        Dataset<Row> cqlResultDataFrame = null;

        String aggregationLevel = null;

        try {

            String geoL1 = nodeAndAggregationDetails.get("geoL1");
            String geoL2 = nodeAndAggregationDetails.get("geoL2");
            String geoL3 = nodeAndAggregationDetails.get("geoL3");
            String geoL4 = nodeAndAggregationDetails.get("geoL4");
            String node = nodeAndAggregationDetails.get("node");
            String netype = getNodeName(node);

            boolean isGeoL1MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("isGeoL1MultiSelect"));
            boolean isGeoL2MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("isGeoL2MultiSelect"));
            boolean isGeoL3MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("isGeoL3MultiSelect"));
            boolean isGeoL4MultiSelect = Boolean.parseBoolean(nodeAndAggregationDetails.get("isGeoL4MultiSelect"));
            String geoL1List = nodeAndAggregationDetails.get("geoL1List");
            String geoL2List = nodeAndAggregationDetails.get("geoL2List");
            String geoL3List = nodeAndAggregationDetails.get("geoL3List");
            String geoL4List = nodeAndAggregationDetails.get("geoL4List");

            geoL1List = geoL1List.replace("[", "").replace("]", "");
            String[] geoL1ListArray = geoL1List.split(",");
            geoL1ListArray = Arrays.stream(geoL1ListArray).map(String::trim).map(String::toUpperCase)
                    .toArray(String[]::new);

            geoL2List = geoL2List.replace("[", "").replace("]", "");
            String[] geoL2ListArray = geoL2List.split(",");
            geoL2ListArray = Arrays.stream(geoL2ListArray).map(String::trim).map(String::toUpperCase)
                    .toArray(String[]::new);

            geoL3List = geoL3List.replace("[", "").replace("]", "");
            String[] geoL3ListArray = geoL3List.split(",");
            geoL3ListArray = Arrays.stream(geoL3ListArray).map(String::trim).map(String::toUpperCase)
                    .toArray(String[]::new);

            geoL4List = geoL4List.replace("[", "").replace("]", "");
            String[] geoL4ListArray = geoL4List.split(",");
            geoL4ListArray = Arrays.stream(geoL4ListArray).map(String::trim).map(String::toUpperCase)
                    .toArray(String[]::new);

            if (geoL1.contains("INDIA")) {

                logger.info("CASE 0: INDIA");

                aggregationLevel = "L0";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("CLUBBED") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 1: CLUBBED, CLUBBED, CLUBBED, CLUBBED, CLUBBED");

                // CASE 1: CLUBBED, CLUBBED, CLUBBED, CLUBBED, CLUBBED

                aggregationLevel = "L0";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("CLUBBED") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 2: CLUBBED, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");

                // CASE 2: CLUBBED, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 3: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, CLUBBED");

                // CASE 3: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, CLUBBED

                aggregationLevel = "L1";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 4: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");

                // CASE 4: INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 5: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED");

                // CASE 5: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED

                aggregationLevel = "L2";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 6: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL");

                // CASE 6: INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 7: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");

                // CASE 7: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED

                aggregationLevel = "L3";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 8: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL");

                // CASE 8: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("CASE 9: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED");

                // CASE 9: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, CLUBBED

                aggregationLevel = "L4";
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (geoL1.contains("INDIVIDUAL") && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 10: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");

                // CASE 10: INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL

                aggregationLevel = netype;
                cqlResultDataFrame = getCQLDataForSelectedLevel(jobContext, aggregationLevel, reportWidgetDetails,
                        extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 11: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, CLUBBED");

                // CASE 11: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, CLUBBED

                aggregationLevel = "L1";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL1ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("CLUBBED") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 12: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL");

                // CASE 12: MULTI SELECT, CLUBBED, CLUBBED, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());

                if (nodesList.size() > 5000) {
                    logger.info(
                            "Nodes List Size is Greater than 5000. Splitting the List into Batches With Each Batch Size 5000");

                    List<List<String>> batches = splitListIntoBatches(nodesList, 5000);
                    logger.info("Batches Size: {}", batches.size());

                    for (int i = 0; i < batches.size(); i++) {
                        List<String> batch = batches.get(i);
                        String[] nodesArray = batch.toArray(new String[0]);

                        String batchNumber = String.format("BATCH-%02d", i + 1);
                        String banner = String.format("========== [ %s / %02d ] ==========",
                                batchNumber,
                                batches.size());

                        logger.info("\n{}", banner);
                        logger.info("🔥🔥🔥 Processing {} - Nodes Array Size: {} 🔥🔥🔥", batchNumber,
                                nodesArray.length);

                        Dataset<Row> subDf = getCQLDataOfSelectedNodenames(nodesArray, jobContext, netype,
                                reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

                        if (cqlResultDataFrame == null) {
                            cqlResultDataFrame = subDf;
                        } else {
                            cqlResultDataFrame = cqlResultDataFrame.union(subDf);
                        }
                    }

                } else {
                    logger.info("Nodes List Size is Less than 1000. Fetching Node Data.");

                    String[] nodesArray = nodesList.toArray(new String[0]);
                    cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesArray, jobContext, netype,
                            reportWidgetDetails,
                            extraParameters, nodeAndAggregationDetails);
                }

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 13: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED");

                // CASE 13: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, CLUBBED

                aggregationLevel = "L2";
                List<String> statesList = getAllStateOfSelectedRegions(geoL1ListArray, jobContext);
                logger.info("States List Size: {}", statesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(statesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 14: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL");

                // CASE 14: MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 15: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");

                // CASE 15: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED

                aggregationLevel = "L3";
                List<String> citiesList = getAllCityOfSelectedRegions(geoL1ListArray, jobContext);
                logger.info("Cities List Size: {}", citiesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(citiesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 16: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL");

                // CASE 16: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("CASE 17: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED");

                // CASE 17: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED, CLUBBED

                aggregationLevel = "L4";
                List<String> clustersList = getAllCusterOfSelectedRegions(geoL1ListArray, jobContext);
                logger.info("Clusters List Size: {}", clustersList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(clustersList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && geoL2.contains("INDIVIDUAL") && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 18: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");

                // CASE 18: MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL1ListArray, jobContext, "L1",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 19: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, CLUBBED");

                // CASE 19: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, CLUBBED

                aggregationLevel = "L2";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL2ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("CLUBBED")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 20: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, INDIVIDUAL");

                // CASE 20: MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL2ListArray, jobContext, "L2",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 21: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED");

                // CASE 21: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, CLUBBED

                aggregationLevel = "L3";
                List<String> citiesList = getAllCityOfSelectedStates(geoL2ListArray, jobContext);
                logger.info("Cities List Size: {}", citiesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(citiesList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 22: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, INDIVIDUAL");

                // CASE 22: MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL2ListArray, jobContext, "L2",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("CLUBBED") && node.contains("CLUBBED")) {

                logger.info("CASE 23: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED");

                // CASE 23: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, CLUBBED

                aggregationLevel = "L3";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL3ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("CLUBBED") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 24: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, INDIVIDUAL");

                // CASE 24: MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL3ListArray, jobContext, "L3",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                logger.info("CASE 25: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED");

                // CASE 25: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, CLUBBED

                aggregationLevel = "L4";
                List<String> clustersList = getAllCusterOfSelectedCity(geoL3ListArray, jobContext);
                logger.info("Clusters List Size: {}", clustersList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(clustersList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                logger.info("CASE 26: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL");

                // CASE 26: MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL

                aggregationLevel = netype;
                List<String> nodesList = getAllNodesForSelectedGeography(geoL3ListArray, jobContext, "L3",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect && isGeoL4MultiSelect
                    && node.contains("CLUBBED")) {

                logger.info("CASE 27: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED");

                // CASE 27: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, CLUBBED

                aggregationLevel = "L4";
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(geoL4ListArray, jobContext, aggregationLevel,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && isGeoL3MultiSelect && isGeoL4MultiSelect
                    && node.contains("INDIVIDUAL")) {

                logger.info("CASE 28: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL");

                // CASE 28: MULTI SELECT, MULTI SELECT, MULTI SELECT, MULTI SELECT, INDIVIDUAL

                aggregationLevel = netype;

                List<String> nodesList = getAllNodesForSelectedGeography(geoL4ListArray, jobContext, "L4",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodesList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodesList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);
            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("CLUBBED")) {

                // CASE 29: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED

                logger.info("CASE 29: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, CLUBBED");

                aggregationLevel = "L4";
                List<String> clustersList = getAllCusterOfSelectedStates(geoL2ListArray, jobContext);
                logger.info("Clusters List Size: {}", clustersList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(clustersList.toArray(new String[0]), jobContext,
                        aggregationLevel, reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else if (isGeoL1MultiSelect && isGeoL2MultiSelect && geoL3.contains("INDIVIDUAL")
                    && geoL4.contains("INDIVIDUAL") && node.contains("INDIVIDUAL")) {

                // CASE 30: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL

                logger.info("CASE 30: MULTI SELECT, MULTI SELECT, INDIVIDUAL, INDIVIDUAL, INDIVIDUAL");

                aggregationLevel = netype;
                List<String> nodeList = getAllNodesForSelectedGeography(geoL2ListArray, jobContext, "L2",
                        reportWidgetDetails);
                logger.info("Nodes List Size: {}", nodeList.size());
                cqlResultDataFrame = getCQLDataOfSelectedNodenames(nodeList.toArray(new String[0]), jobContext, netype,
                        reportWidgetDetails, extraParameters, nodeAndAggregationDetails);

            } else {
                logger.info("=========This Case In Not Implemented==========");
            }
        } catch (Exception e) {
            logger.error("Error in Getting Result of Node and Aggregation Details, Message: {}, Error: {}",
                    e.getMessage(), e);
        }

        if (aggregationLevel != null && !aggregationLevel.isEmpty()) {
            nodeAndAggregationDetails.put("aggregationLevel", aggregationLevel);
            jobContext.setParameters("aggregationLevel", aggregationLevel);
            logger.info("Aggregation Level '{}' Set to Job Context Successfully! ✅", aggregationLevel);
        }

        return cqlResultDataFrame;
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
            logger.error("Error in Getting All City of Selected States, Message: {}, Error: {}", e.getMessage(), e);
            return Collections.singletonList("NO_CITY_FOUND");
        }
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
            logger.error("Error in Getting All Cluster of Selected Regions, Message: {}, Error: {}", e.getMessage(), e);
            return Collections.singletonList("NO_CLUSTER_FOUND");
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
            logger.error("Error in Getting All Cluster of Selected Regions, Message: {}, Error: {}", e.getMessage(), e);
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

            logger.info("MySQL Query: {}", mysqlQuery);

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
            logger.error("Error in Getting All City of Selected Regions, Message: {}, Error: {}", e.getMessage(), e);
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

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_CLUSTER_FOUND"), Encoders.STRING())
                        .toDF("CLUSTER");
            }

            List<String> clustersList = df.as(Encoders.STRING()).collectAsList();
            return clustersList;

        } catch (Exception e) {
            logger.error("Error in Getting All Cluster of Selected City, Message: {}, Error: {}", e.getMessage(), e);
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

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_STATE_FOUND"), Encoders.STRING())
                        .toDF("STATE");
            }

            List<String> statesList = df.as(Encoders.STRING()).collectAsList();
            return statesList;

        } catch (Exception e) {
            logger.error("Error in Getting All State of Selected Regions, Message: {}, Error: {}", e.getMessage(), e);
            return Collections.singletonList("NO_STATE_FOUND");
        }
    }

    private static List<String> getAllNodesForSelectedGeography(String[] geoListArray, JobContext jobContext,
            String aggregationLevel, Map<String, String> reportWidgetDetails) {

        String domain = reportWidgetDetails.get("DOMAIN");
        String vendor = reportWidgetDetails.get("VENDOR");
        String technology = reportWidgetDetails.get("TECHNOLOGY");

        List<String> nodesList = new ArrayList<>();

        try {
            if (geoListArray == null || geoListArray.length == 0) {
                logger.error("Geo List is Empty. Skipping Node Fetch.");
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
                    logger.error("Invalid Aggregation Level: {}", aggregationLevel);
                    return Collections.singletonList("NO_NODE_FOUND");
                }
            }

            String mysqlQuery = String.format(
                    """
                                SELECT DISTINCT UPPER(ne.NE_ID) AS NODE
                                FROM NETWORK_ELEMENT ne
                                JOIN %s geo ON ne.%s = geo.ID
                                WHERE ne.DOMAIN = '%s' AND ne.VENDOR = '%s' AND ne.TECHNOLOGY = '%s' AND ne.NE_ID IS NOT NULL AND ne.PARENT_NE_ID_FK IS NULL AND UPPER(geo.GEO_NAME) IN (%s)
                            """,
                    geoTable, geoFkColumn, domain, vendor, technology, inClause);

            logger.info("MySQL Query: {}", mysqlQuery);

            Dataset<Row> df = executeQuery(mysqlQuery, jobContext);

            if (df == null || df.isEmpty()) {
                df = jobContext.sqlctx().createDataset(Collections.singletonList("NO_NODE_FOUND"), Encoders.STRING())
                        .toDF("NODE");
            }

            nodesList = df.as(Encoders.STRING()).collectAsList();

            return nodesList;

        } catch (Exception e) {
            logger.error("Error in Getting All Nodes for Selected Geography, Message: {}, Error: {}", e.getMessage(),
                    e);
            return Collections.singletonList("NO_NODE_FOUND");
        }
    }

    private static Dataset<Row> getCQLDataOfSelectedNodenames(String[] geoListArray, JobContext jobContext,
            String aggregationLevel, Map<String, String> reportWidgetDetails, Map<String, String> extraParameters,
            Map<String, String> nodeAndAggregationDetails) {

        String domain = reportWidgetDetails.get("DOMAIN");
        String vendor = reportWidgetDetails.get("VENDOR");
        String technology = reportWidgetDetails.get("TECHNOLOGY");
        String timestamp = jobContext.getParameter("TIMESTAMP"); // 2025-01-19 00:00:00.000000+0000
        // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd
        // HH:mm:ss.SSSSSSxxxx");
        // OffsetDateTime odt = OffsetDateTime.parse(timestamp, formatter);
        // String date = odt.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
        OffsetDateTime odt = OffsetDateTime.parse(timestamp, formatter);
        OffsetDateTime utcTime = odt.withOffsetSameInstant(ZoneOffset.UTC);
        String date = utcTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String utcTimestampStr = utcTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String node = nodeAndAggregationDetails.get("node");
        String netype = getNodeName(node);

        String datalevel = "";

        StringBuilder mysqlQuery = new StringBuilder()
                .append("SELECT DISTINCT CONCAT(TECHNOLOGY, IF(ROWKEY_TECHNOLOGY IS NOT NULL, '_', ''), ")
                .append("COALESCE(NETWORK_TYPE, '')) AS rowKeyAppender ")
                .append("FROM PM_NODE_VENDOR ")
                .append("WHERE domain = '").append(domain).append("' ")
                .append("AND vendor = '").append(vendor).append("' ")
                .append("AND technology = '").append(technology).append("'");

        logger.info("MySQL Query: {}", mysqlQuery.toString());

        Dataset<Row> df = executeQuery(mysqlQuery.toString(), jobContext);

        String dataLevelAppender = df.as(Encoders.STRING()).collectAsList().get(0);
        logger.info("Data Level Appender: {}", dataLevelAppender);

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

        logger.info("Data Level: {}", datalevel);

        String inClause = Arrays.stream(geoListArray)
                .map(name -> "'" + name.replace("'", "''") + "'")
                .collect(Collectors.joining(", "));

        String cqlFilter = "";

        cqlFilter = String.format(
                "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s'  AND date = '%s' AND nodename IN (%s) AND timestamp = '%s'",
                domain, vendor, technology, datalevel, date, inClause, utcTimestampStr);

        logger.info("🔍 CQL Filter With Nodename: {}", cqlFilter);

        Dataset<Row> cqlDataDF = getCQLDataUsingSpark(cqlFilter, jobContext, reportWidgetDetails, extraParameters);

        return cqlDataDF;
    }

    public static String getNodeName(String nodeString) {

        if (nodeString == null) {
            return "";
        }

        int lastDashIndex = nodeString.lastIndexOf('-');
        String part = (lastDashIndex != -1) ? nodeString.substring(0, lastDashIndex) : nodeString;

        int spaceIndex = part.indexOf(' ');
        String node = (spaceIndex != -1) ? part.substring(spaceIndex + 1) : part;

        node = node.trim().toUpperCase();
        return node.replace(" ", "-");
    }

    private static Dataset<Row> getCQLDataForSelectedLevel(JobContext jobContext, String aggregationLevel,
            Map<String, String> reportWidgetDetails, Map<String, String> extraParameters,
            Map<String, String> nodeAndAggregationDetails) {

        Dataset<Row> df = null;

        String domain = reportWidgetDetails.get("DOMAIN");
        String vendor = reportWidgetDetails.get("VENDOR");
        String technology = reportWidgetDetails.get("TECHNOLOGY");
        String timestamp = jobContext.getParameter("TIMESTAMP");

        logger.info("Processing Parameters - Domain: {}, Vendor: {}, Technology: {}, Timestamp: {}", domain, vendor,
                technology, timestamp);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxxx");
        OffsetDateTime odt = OffsetDateTime.parse(timestamp, formatter);
        OffsetDateTime utcTime = odt.withOffsetSameInstant(ZoneOffset.UTC);
        String date = utcTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String node = nodeAndAggregationDetails.get("node");
        String netype = getNodeName(node);

        logger.info(
                "Getting CQL Data for Selected Level with Parameters - Domain: {}, Vendor: {}, Technology: {}, Timestamp: {}, Date: {}, NeType: {}, Node: {}, Aggregation Level: {}",
                domain, vendor, technology, timestamp, date, netype, node, aggregationLevel);

        StringBuilder mysqlQuery = new StringBuilder()
                .append("SELECT DISTINCT CONCAT(TECHNOLOGY, IF(ROWKEY_TECHNOLOGY IS NOT NULL, '_', ''), ")
                .append("COALESCE(NETWORK_TYPE, '')) AS rowKeyAppender ")
                .append("FROM PM_NODE_VENDOR ")
                .append("WHERE DOMAIN = '").append(domain).append("' ")
                .append("AND VENDOR = '").append(vendor).append("' ")
                .append("AND TECHNOLOGY = '").append(technology).append("'");

        logger.info("MySQL Query: {}", mysqlQuery.toString());

        Dataset<Row> rowKeyAppenderDF = executeQuery(mysqlQuery.toString(), jobContext);

        String dataLevelAppender = "";
        List<String> appenderList = rowKeyAppenderDF.as(Encoders.STRING()).collectAsList();
        if (appenderList != null && !appenderList.isEmpty()) {
            dataLevelAppender = appenderList.get(0);
        } else {
            logger.error(
                    "No Data Level Appender Found. Please Ensure Data Level Appender is Present in the DataFrame Before Processing.");
        }

        logger.info("Data Level Appender: {}", dataLevelAppender);

        String datalevel = "";
        String nodename = "";

        if (aggregationLevel.equals("L0")) {
            datalevel = "L0" + "_" + dataLevelAppender;
            nodename = "India";
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

        logger.info("Getting CQL Data for Selected Level with Data Level: {}", datalevel);

        try {

            String cqlFilter = "";

            if (aggregationLevel.equals("L0")) {

                cqlFilter = String.format(
                        "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s'  AND date = '%s' AND nodename = '%s' AND timestamp = '%s'",
                        domain, vendor, technology, datalevel, date, nodename, timestamp);

            } else {

                cqlFilter = String.format(
                        "domain = '%s' AND vendor = '%s' AND technology = '%s' AND datalevel = '%s' AND date = '%s' AND timestamp = '%s'",
                        domain, vendor, technology, datalevel, date, timestamp);

            }

            logger.info("CQL Filter: {}", cqlFilter);

            df = getCQLDataUsingSpark(cqlFilter, jobContext, reportWidgetDetails, extraParameters);

        } catch (Exception e) {
            logger.error("Error in Getting CQL Data for Selected Level, Message: {}, Error: {}", e.getMessage(), e);
        }

        return df;
    }

    private static Dataset<Row> executeQuery(String sqlQuery, JobContext jobContext) {

        Dataset<Row> resultDataset = null;

        try {
            resultDataset = jobContext.sqlctx().read()
                    .format("jdbc")
                    .option("driver", sparkPMJdbcDriver)
                    .option("url", sparkPMJdbcUrl)
                    .option("user", sparkPMJdbcUsername)
                    .option("password", sparkPMJdbcPassword)
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
        jobContext.sqlctx().setConf("spark.cassandra.connection.localDC", sparkCassandraDatacenter);
        jobContext.sqlctx().setConf("spark.cassandra.connection.host", sparkCassandraHost);
        jobContext.sqlctx().setConf("spark.cassandra.connection.port", sparkCassandraPort);
        jobContext.sqlctx().setConf("spark.cassandra.auth.username", sparkCassandraUsername);
        jobContext.sqlctx().setConf("spark.cassandra.auth.password", sparkCassandraPassword);
        jobContext.sqlctx().setConf("spark.sql.catalog.ybcatalog",
                "com.datastax.spark.connector.datasource.CassandraCatalog");
        jobContext.sqlctx().setConf("spark.cassandra.output.ignoreNulls", "true");
        jobContext.sqlctx().setConf("spark.cassandra.query.retry.count", "10");
        jobContext.sqlctx().setConf("spark.cassandra.output.batch.size.rows", "500");
        jobContext.sqlctx().setConf("spark.cassandra.output.concurrent.writes", "3");
        jobContext.sqlctx().setConf("spark.cassandra.connection.remoteConnectionsPerExecutor", "5");
        jobContext.sqlctx().setConf("spark.jdbc.url", sparkPMJdbcUrl);
        jobContext.sqlctx().setConf("spark.jdbc.user", sparkPMJdbcUsername);
        jobContext.sqlctx().setConf("spark.jdbc.password", sparkPMJdbcPassword);
        return jobContext;
    }

    private static Dataset<Row> getCQLDataUsingSpark(String cqlFilter, JobContext jobContext,
            Map<String, String> reportWidgetDetails, Map<String, String> extractedParametersMap) {

        String cqlTableName = extractedParametersMap.get("cqlTableName");
        jobContext.sqlctx().sparkSession().conf().set("spark.sql.session.timeZone", "UTC");

        Dataset<Row> resultDataFrame = null;

        int maxRetries = 5;
        int currentRetry = 0;
        boolean success = false;

        while (!success && currentRetry < maxRetries) {
            try {
                if (currentRetry > 0) {
                    long backoffMs = (long) Math.pow(2, currentRetry) * 1000;
                    logger.info("Retry Attempt {} - Waiting {} Milliseconds Before Next Attempt", currentRetry,
                            backoffMs);
                    Thread.sleep(backoffMs);
                }

                resultDataFrame = jobContext.sqlctx().read()
                        .format("org.apache.spark.sql.cassandra")
                        .options(Map.of(
                                "table", cqlTableName,
                                "keyspace", sparkCassandraKeyspacePM,
                                "pushdown", "true"))
                        .load()
                        .filter(cqlFilter);
                resultDataFrame = resultDataFrame.cache();
                success = true;

            } catch (Exception e) {
                currentRetry++;
                logger.error("Attempt {} Failed: Error in Getting CQL Data Using Spark, Message: {}, Error: {}",
                        currentRetry, e.getMessage(), e);

                if (currentRetry < maxRetries) {
                    logger.info("Retrying Query... (Attempt {}/{})", currentRetry + 1, maxRetries);
                } else {
                    logger.error("All Retry Attempts Failed for Query: {}", cqlFilter);
                    throw new RuntimeException("Failed to Execute Cassandra Query After " + maxRetries + " Attempts",
                            e);
                }
            }
        }
        return resultDataFrame;
    }

    private static List<List<String>> splitListIntoBatches(List<String> list, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(list.size(), i + batchSize);
            batches.add(list.subList(i, end));
        }
        return batches;
    }
}