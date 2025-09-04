package com.enttribe.pm.job.report.otf;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OTFReadMinioFiles extends Processor {

    public OTFReadMinioFiles() {
        super();
    }

    public OTFReadMinioFiles(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.error("OTFReadMinioFiles Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    public OTFReadMinioFiles(Integer id, String processorName) {
        super(id, processorName);
        logger.error("OTFReadMinioFiles Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.error("Updated OTFReadMinioFiles Execution Started !");

        try {

            /*
             * READ TRINO ORC
             */

            String trinoOrcFilePaths = jobContext.getParameter("TRINO_ORC_FILE_PATHS");
            String baseTrinoOrcPath = jobContext.getParameter("BASE_TRINO_ORC_PATH");

            Dataset<Row> trinoOrcDF = null;

            try {
                trinoOrcDF = readTrinoOrcFileFromMinio(trinoOrcFilePaths, baseTrinoOrcPath, jobContext);
                if (trinoOrcDF == null) {
                    Dataset<Row> emptyDF = getEmptyDF(jobContext, jobContext.getParameter("FINAL_COUNTER_INFO"));
                    emptyDF.createOrReplaceTempView("JOINED_RESULT");
                    emptyDF.show(5);
                    logger.error("++++++++++[READ MINIO FILES]++++++++++");
                    return emptyDF;
                }
                trinoOrcDF.createOrReplaceTempView("TRINO_ORC_DF");
                logger.error("Trino ORC DataFrame Loaded Successfully!");
            } catch (Exception e) {
                logger.error("Exception While Executing OTFReadMinioFiles: {}", e.getMessage());
                Dataset<Row> emptyDF = getEmptyDF(jobContext, jobContext.getParameter("FINAL_COUNTER_INFO"));
                emptyDF.createOrReplaceTempView("JOINED_RESULT");
                emptyDF.show(5);
                logger.error("++++++++++[READ MINIO FILES]++++++++++");
                return emptyDF;
            }

            /*
             * READ TRINO NE
             */

            Dataset<Row> trinoNeDF = null;
            String baseTrinoNePath = jobContext.getParameter("BASE_TRINO_NE_PATH");
            String fromDate = jobContext.getParameter("FROM_DATE");
            String toDate = jobContext.getParameter("TO_DATE");

            String reportWidgetDetailsJson = jobContext.getParameter("REPORT_WIDGET_DETAILS");
            Map<String, String> reportWidgetDetailsMap = new ObjectMapper().readValue(reportWidgetDetailsJson,
                    new TypeReference<Map<String, String>>() {
                    });

            String trinoNeFilePath = getTrinoNePath(baseTrinoNePath, fromDate, toDate, reportWidgetDetailsMap);
            logger.error("Trino NE File Path: {}", trinoNeFilePath);

            try {

                trinoNeDF = readTrinoNeFileFromMinio(trinoNeFilePath, baseTrinoNePath, jobContext);
                if (trinoNeDF == null) {
                    Dataset<Row> emptyDF = getEmptyDF(jobContext, jobContext.getParameter("FINAL_COUNTER_INFO"));
                    emptyDF.createOrReplaceTempView("JOINED_RESULT");
                    emptyDF.show(5);
                    logger.error("++++++++++[READ MINIO FILES]++++++++++");
                    return emptyDF;
                }
                trinoNeDF.createOrReplaceTempView("TRINO_NE_DF");

                trinoNeDF = filterTrinoNeDFBasedOnNodeAndAggregationDetails(trinoNeDF, jobContext);
                trinoNeDF.createOrReplaceTempView("TRINO_NE_DF");
                trinoNeDF.show(5);
                logger.error("++++++++++[READ MINIO FILES]++++++++++");

            } catch (Exception e) {
                logger.error("Exception While Executing OTFReadMinioFiles: {}", e.getMessage());
                Dataset<Row> emptyDF = getEmptyDF(jobContext, jobContext.getParameter("FINAL_COUNTER_INFO"));
                emptyDF.createOrReplaceTempView("JOINED_RESULT");
                emptyDF.show(5);
                logger.error("++++++++++[READ MINIO FILES]++++++++++");
                return emptyDF;
            }

            try {

                String[] trinoNeColumns = trinoNeDF.columns();
                String finalQueryMap = generateMetaDataMapQuery(trinoNeColumns);

                String counterInfoMap = jobContext.getParameter("COUNTER_INFO_MAP");
                String categoryInfoMap = jobContext.getParameter("CATEGORY_INFO_MAP");
                String categoryList = jobContext.getParameter("CATEGORY_LIST");

                String isKpiCodeListEmpty = jobContext.getParameter("IS_KPI_CODE_LIST_EMPTY");
                logger.error("Is KPI Code List Empty: {}", isKpiCodeListEmpty);

                if (StringUtils.isNotBlank(isKpiCodeListEmpty) && isKpiCodeListEmpty.equalsIgnoreCase("true")) {
                    Dataset<Row> joinedResult = getFinalResult(jobContext, null, null, null, finalQueryMap);
                    joinedResult.createOrReplaceTempView("JOINED_RESULT");
                    joinedResult.show(5);
                    logger.error("++++++++++[READ MINIO FILES]++++++++++");
                    return joinedResult;
                }

                @SuppressWarnings("unchecked")
                Map<String, Map<String, String>> counterInfoMapObject = new ObjectMapper().readValue(counterInfoMap,
                        Map.class);

                @SuppressWarnings("unchecked")
                Map<String, List<Map<String, String>>> categoryInfoMapObject = new ObjectMapper().readValue(
                        categoryInfoMap,
                        Map.class);

                Dataset<Row> joinedResult = getFinalResult(jobContext, categoryInfoMapObject,
                        categoryList, counterInfoMapObject, finalQueryMap);

                joinedResult.createOrReplaceTempView("JOINED_RESULT");
                joinedResult.show(5);
                logger.error("++++++++++[READ MINIO FILES]++++++++++");

                return joinedResult;

            } catch (Exception e) {
                logger.error("Exception While Executing OTFReadMinioFiles: {}", e.getMessage());
                Dataset<Row> emptyDF = getEmptyDF(jobContext, jobContext.getParameter("FINAL_COUNTER_INFO"));
                emptyDF.createOrReplaceTempView("JOINED_RESULT");
                emptyDF.show(5);
                logger.error("++++++++++[READ MINIO FILES]++++++++++");
                return emptyDF;
            }

        } catch (Exception e) {
            logger.error("Exception While Executing OTFReadMinioFiles: {}", e.getMessage(), e);
            Dataset<Row> emptyDF = getEmptyDF(jobContext, jobContext.getParameter("FINAL_COUNTER_INFO"));
            emptyDF.createOrReplaceTempView("JOINED_RESULT");
            emptyDF.show(5);
            logger.error("++++++++++[READ MINIO FILES]++++++++++");
            return emptyDF;
        }
    }

    private static Dataset<Row> filterTrinoNeDFBasedOnNodeAndAggregationDetails(Dataset<Row> trinoNeDF,
            JobContext jobContext) {

        String nodeAndAggregationDetails = jobContext.getParameter("NODE_AND_AGGREGATION_DETAILS");

        try {

            @SuppressWarnings("unchecked")
            Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper().readValue(nodeAndAggregationDetails,
                    Map.class);

            logger.error("Node And Aggregation Details: {}", nodeAndAggregationDetailsMap);

            String isGeoL1MultiSelect = nodeAndAggregationDetailsMap.get("IS_GEOGRAPHY_L1_MULTI_SELECT");
            String isGeoL2MultiSelect = nodeAndAggregationDetailsMap.get("IS_GEOGRAPHY_L2_MULTI_SELECT");
            String isGeoL3MultiSelect = nodeAndAggregationDetailsMap.get("IS_GEOGRAPHY_L3_MULTI_SELECT");
            String isGeoL4MultiSelect = nodeAndAggregationDetailsMap.get("IS_GEOGRAPHY_L4_MULTI_SELECT");

            logger.error("Is Geo L1 Multi Select: {}", isGeoL1MultiSelect);
            logger.error("Is Geo L2 Multi Select: {}", isGeoL2MultiSelect);
            logger.error("Is Geo L3 Multi Select: {}", isGeoL3MultiSelect);
            logger.error("Is Geo L4 Multi Select: {}", isGeoL4MultiSelect);

            // Use a list to collect filter conditions
            List<String> filterConditions = new ArrayList<>();

            // Build L1 filter condition
            if (StringUtils.isNotBlank(isGeoL1MultiSelect) && isGeoL1MultiSelect.equalsIgnoreCase("true")) {
                String geoL1List = nodeAndAggregationDetailsMap.get("GEOGRAPHY_L1_LIST");
                geoL1List = geoL1List.replace("[", "").replace("]", "");
                if (StringUtils.isNotBlank(geoL1List)) {
                    String[] geoL1ListArray = geoL1List.split(",");
                    String geoL1ListQuoted = Arrays.stream(geoL1ListArray)
                            .map(String::trim)
                            .filter(StringUtils::isNotBlank)
                            .map(e -> "'" + e.toUpperCase() + "'")
                            .collect(Collectors.joining(","));
                    if (StringUtils.isNotBlank(geoL1ListQuoted)) {
                        filterConditions.add("UPPER(L1) IN (" + geoL1ListQuoted + ")");
                    }
                }
            }

            // Build L2 filter condition
            if (StringUtils.isNotBlank(isGeoL2MultiSelect) && isGeoL2MultiSelect.equalsIgnoreCase("true")) {
                String geoL2List = nodeAndAggregationDetailsMap.get("GEOGRAPHY_L2_LIST");
                geoL2List = geoL2List.replace("[", "").replace("]", "");
                if (StringUtils.isNotBlank(geoL2List)) {
                    String[] geoL2ListArray = geoL2List.split(",");
                    String geoL2ListQuoted = Arrays.stream(geoL2ListArray)
                            .map(String::trim)
                            .filter(StringUtils::isNotBlank)
                            .map(e -> "'" + e.toUpperCase() + "'")
                            .collect(Collectors.joining(","));
                    if (StringUtils.isNotBlank(geoL2ListQuoted)) {
                        filterConditions.add("UPPER(L2) IN (" + geoL2ListQuoted + ")");
                    }
                }
            }

            // Build L3 filter condition
            if (StringUtils.isNotBlank(isGeoL3MultiSelect) && isGeoL3MultiSelect.equalsIgnoreCase("true")) {
                String geoL3List = nodeAndAggregationDetailsMap.get("GEOGRAPHY_L3_LIST");
                geoL3List = geoL3List.replace("[", "").replace("]", "");
                if (StringUtils.isNotBlank(geoL3List)) {
                    String[] geoL3ListArray = geoL3List.split(",");
                    String geoL3ListQuoted = Arrays.stream(geoL3ListArray)
                            .map(String::trim)
                            .filter(StringUtils::isNotBlank)
                            .map(e -> "'" + e.toUpperCase() + "'")
                            .collect(Collectors.joining(","));
                    if (StringUtils.isNotBlank(geoL3ListQuoted)) {
                        filterConditions.add("UPPER(L3) IN (" + geoL3ListQuoted + ")");
                    }
                }
            }

            // Build L4 filter condition
            if (StringUtils.isNotBlank(isGeoL4MultiSelect) && isGeoL4MultiSelect.equalsIgnoreCase("true")) {
                String geoL4List = nodeAndAggregationDetailsMap.get("GEOGRAPHY_L4_LIST");
                geoL4List = geoL4List.replace("[", "").replace("]", "");
                if (StringUtils.isNotBlank(geoL4List)) {
                    String[] geoL4ListArray = geoL4List.split(",");
                    String geoL4ListQuoted = Arrays.stream(geoL4ListArray)
                            .map(String::trim)
                            .filter(StringUtils::isNotBlank)
                            .map(e -> "'" + e.toUpperCase() + "'")
                            .collect(Collectors.joining(","));
                    if (StringUtils.isNotBlank(geoL4ListQuoted)) {
                        filterConditions.add("UPPER(L4) IN (" + geoL4ListQuoted + ")");
                    }
                }
            }

            // Upload Custom Filter Condition
            String isNodeNameListEmpty = nodeAndAggregationDetailsMap.get("IS_NODE_MULTI_SELECT");

            if (StringUtils.isNotBlank(isNodeNameListEmpty) && isNodeNameListEmpty.equalsIgnoreCase("true")) {
                // String nodeInfoMapJson = nodeAndAggregationDetailsMap.get("NODE_INFO_MAP");
                String nodeNameList = nodeAndAggregationDetailsMap.get("NODE_NAME_LIST");

                // @SuppressWarnings("unchecked")
                // Map<String, String> nodeInfoMapObject = new
                // ObjectMapper().readValue(nodeInfoMapJson, Map.class);

                // List<String> upperNames = nodeInfoMapObject.values().stream()
                // .map(name -> "'" + name.toUpperCase() + "'")
                // .collect(Collectors.toList());

                // if (!upperNames.isEmpty()) {
                // String inClause = String.join(",", upperNames);
                // filterConditions.add("UPPER(H1) IN (" + inClause + ")");
                // }
                filterConditions.add("UPPER(H1) IN (" + nodeNameList + ")");
            }

            // Build final filter query
            String filterQuery = "";
            if (!filterConditions.isEmpty()) {
                filterQuery = String.join(" AND ", filterConditions);
                logger.error("Final Filter Query: {}", filterQuery);

                trinoNeDF = trinoNeDF.where(filterQuery);
                // trinoNeDF.show(5, false);
                logger.error("Trino NE DataFrame Filtered Successfully!");
            } else {
                logger.error("No Filter Query Applied (No Valid Filter Conditions Found)");
            }

            return trinoNeDF;
        } catch (Exception e) {
            logger.error(
                    "Exception While Filtering Trino NE DataFrame Based On Node And Aggregation Details. Input={}, Message={}, Error={}",
                    nodeAndAggregationDetails, e.getMessage(), e);
            return trinoNeDF;
        }
    }

    private static Dataset<Row> readTrinoNeFileFromMinio(String trinoNeFilePath, String baseTrinoNePath,
            JobContext jobContext) {
        logger.error("Starting to read Trino NE files from MinIO. File Paths: {}", trinoNeFilePath);

        // Validate input parameters
        if (StringUtils.isBlank(trinoNeFilePath)) {
            throw new IllegalArgumentException("Trino NE File Paths Parameter Cannot Be Null or Empty!");
        }
        if (StringUtils.isBlank(baseTrinoNePath)) {
            throw new IllegalArgumentException("Base Trino NE Path Parameter Cannot Be Null or Empty!");
        }

        try {
            String endpointUrl = jobContext.getParameter("SPARK_MINIO_ENDPOINT_URL");
            String accessKey = jobContext.getParameter("SPARK_MINIO_ACCESS_KEY");
            String secretKey = jobContext.getParameter("SPARK_MINIO_SECRET_KEY");
            String bucketName = jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM");

            validateMinioParameters(endpointUrl, accessKey, secretKey, bucketName);

            logger.error("Spark MinIO Endpoint URL: {}", endpointUrl);
            logger.error("Spark MinIO Bucket Name: {}", bucketName);

            configureMinioParameters(jobContext, endpointUrl, accessKey, secretKey);
            logger.error("MinIO/S3A Configuration Set Successfully!");

            validateOrcPath(trinoNeFilePath);
            logger.error("Path Format Validated Successfully!");

            // Prepare reading options
            Map<String, String> optionsMap = new HashMap<>();
            optionsMap.put("basePath", baseTrinoNePath);
            optionsMap.put("mergeSchema", "true");

            List<String> validPaths = new ArrayList<>();
            String[] pathArray = trinoNeFilePath.split(",");

            for (String rawPath : pathArray) {
                String trimmedPath = rawPath.trim();
                if (StringUtils.isBlank(trimmedPath)) {
                    continue;
                }
                if (trimmedPath.startsWith("s3a://")) {
                    validPaths.add(trimmedPath);
                } else {
                    logger.error("Invalid Path Format (must start with s3a://): {}", trimmedPath);
                }
            }

            if (validPaths.isEmpty()) {
                throw new RuntimeException("No Valid File Paths Found In The Provided Paths: " + trinoNeFilePath);
            }

            logger.error("Found {} Valid File Patterns To Process", validPaths.size());

            // Check file existence and list existing files
            List<String> existingFilePaths = checkAndListExistingFiles(jobContext, validPaths);

            if (existingFilePaths.isEmpty()) {
                // throw new RuntimeException("No ORC Files Found In Any Of The Provided Paths:
                // " + trinoNeFilePath);
                return null;
            }

            logger.error("Found {} ORC Files To Read", existingFilePaths.size());

            // Read all existing files in a single shot
            Dataset<Row> orcDataFrame = readExistingFilesInSingleShot(jobContext, existingFilePaths, optionsMap, null);

            if (orcDataFrame != null) {
                orcDataFrame = orcDataFrame.cache();
                logger.error("DataFrame Cached Successfully. Row Count: {}", orcDataFrame.count());
            } else {
                throw new RuntimeException("Failed To Read Any Data From The Provided File Paths");
            }

            return orcDataFrame;

        } catch (IllegalArgumentException e) {
            logger.error("Configuration Error: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Unknown error";

            if (errorMsg.contains("File does not exist") || errorMsg.contains("No such file")) {
                throw new RuntimeException("Files not found at path: " + trinoNeFilePath, e);
            } else if (errorMsg.contains("Access Denied") || errorMsg.contains("Permission denied")) {
                throw new RuntimeException("Access denied to files at: " + trinoNeFilePath, e);
            } else if (errorMsg.contains("Connection") || errorMsg.contains("timeout")) {
                throw new RuntimeException("Connection failed to MinIO at: " + trinoNeFilePath, e);
            } else if (errorMsg.contains("Invalid ORC")) {
                throw new RuntimeException("Invalid ORC file format in: " + trinoNeFilePath, e);
            } else {
                logger.error("Critical error in readTrinoNeFileFromMinio: {}", errorMsg, e);
                throw new RuntimeException("Failed to read files from MinIO: " + errorMsg, e);
            }
        }
    }

    private static String getTrinoNePath(String baseTrinoNePath, String fromDate, String toDate,
            Map<String, String> reportWidgetDetailsMap) {
        logger.error("Base Trino NE Path: {}", baseTrinoNePath);
        logger.error("From Date: {}", fromDate);
        logger.error("To Date: {}", toDate);

        // Extract metadata from job context
        String domain = reportWidgetDetailsMap.getOrDefault("DOMAIN", "NA");
        String vendor = reportWidgetDetailsMap.getOrDefault("VENDOR", "NA");
        String emstype = reportWidgetDetailsMap.getOrDefault("EMSTYPE", "NA");
        String technology = reportWidgetDetailsMap.getOrDefault("TECHNOLOGY", "NA");

        // DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("MMM dd,yyyy
        // H:mm", Locale.ENGLISH);
        DateTimeFormatter[] inputFormats = new DateTimeFormatter[] {
                DateTimeFormatter.ofPattern("MMM d,yyyy H:mm", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM dd,yyyy H:mm", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM d,yyyy HH:mm", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM dd,yyyy HH:mm", Locale.ENGLISH)
        };
        DateTimeFormatter dateFolderFormat = DateTimeFormatter.ofPattern("yyyyMMdd");

        // LocalDateTime start = LocalDateTime.parse(fromDate, inputFormat);
        // LocalDateTime end = LocalDateTime.parse(toDate, inputFormat);
        LocalDateTime start = parseFlexibleDate(fromDate, inputFormats);
        LocalDateTime end = parseFlexibleDate(toDate, inputFormats);

        Set<String> uniqueDateFolders = new LinkedHashSet<>();

        while (!start.isAfter(end)) {
            String dateFolder = start.format(dateFolderFormat);
            String fullPath = String.format(
                    "%sd=%s/v=%s/emstype=%s/t=%s/date=%s",
                    baseTrinoNePath, domain, vendor, emstype, technology, dateFolder);
            uniqueDateFolders.add(fullPath);
            start = start.plusDays(1).withHour(0).withMinute(0); // jump to next date
        }

        return String.join(",", uniqueDateFolders);
    }

    private static LocalDateTime parseFlexibleDate(String dateStr, DateTimeFormatter[] formatters) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                return LocalDateTime.parse(dateStr, formatter);
            } catch (Exception e) {
                // Try next format
            }
        }
        throw new IllegalArgumentException("Unable to parse date: " + dateStr);
    }

    private static Dataset<Row> getFinalResult(JobContext jobContext,
            Map<String, List<Map<String, String>>> categoryInfoMapObject, String categoryList,
            Map<String, Map<String, String>> counterInfoMapObject, String finalQueryMap) {

        String counterIds = jobContext.getParameter("FINAL_COUNTER_INFO");
        logger.error("Final Counter IDs: {}", counterIds);

        Dataset<Row> emptyDF = getEmptyDF(jobContext, counterIds);
        emptyDF.createOrReplaceTempView("EMPTY_DF");
        logger.error("Empty DataFrame Created Successfully!");

        try {

            Set<String> sequenceNoList = new HashSet<>();
            sequenceNoList.add("fiveMinuteKey");
            sequenceNoList.add("quarterKey");
            sequenceNoList.add("dateKey");
            sequenceNoList.add("hourKey");
            sequenceNoList.add("pmemsid");
            sequenceNoList.add("categoryname");
            sequenceNoList.add("interfacename");

            for (String counterID : counterIds.split(",")) {
                String sequenceno = counterID.split("#")[0];
                sequenceNoList.add(sequenceno);
            }

            logger.error("Sequence No List={}", sequenceNoList);

            String frequency = jobContext.getParameter("FREQUENCY");

            String selectBuilder = "";

            String baseDateExpr = "TO_DATE(CAST(dateKey AS STRING), 'yyyyMMdd')";

            switch (frequency.toUpperCase()) {
                case "5 MIN":
                case "FIVEMIN":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, fiveMinuteKey AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                case "15 MIN":
                case "QUARTERLY":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, quarterKey AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                case "DAILY":
                case "PERDAY":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, dateKey AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                case "HOURLY":
                case "PERHOUR":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, hourKey AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                case "WEEKLY":
                case "PERWEEK":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, CONCAT('W', LPAD(WEEKOFYEAR("
                            + baseDateExpr
                            + "), 2, '0'), '-', YEAR(" + baseDateExpr
                            + ")) AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                case "MONTHLY":
                case "PERMONTH":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, CONCAT(DATE_FORMAT("
                            + baseDateExpr
                            + ", 'MMM'), '-', YEAR("
                            + baseDateExpr + ")) AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                case "YEARLY":
                case "PERYEAR":
                    selectBuilder = "SELECT fiveMinuteKey, quarterKey, dateKey, hourKey, YEAR(" + baseDateExpr
                            + ") AS finalKey, interfacename, interfacename AS pmemsid, categoryname, ";
                    break;

                default:
                    throw new IllegalArgumentException("Invalid Frequency: " + frequency);
            }

            logger.error("Select Builder: {}", selectBuilder);
            String isKpiCodeListEmpty = jobContext.getParameter("IS_KPI_CODE_LIST_EMPTY");
            logger.error("Is KPI Code List Empty: {}", isKpiCodeListEmpty);

            String rawCounterList = "";
            String rawCounters = "";
            String[] sequenceColumns = null;

            if (StringUtils.isNotBlank(isKpiCodeListEmpty) && isKpiCodeListEmpty.equalsIgnoreCase("true")) {

                String counterDetailsMapJson = jobContext.getParameter("COUNTER_DETAILS");
                logger.error("Counter Details Map JSON: {}", counterDetailsMapJson);
                List<Map<String, String>> counterDetailsList = getCounterDetailsList(counterDetailsMapJson);
                logger.error("Counter Details List: {}", counterDetailsList);

                Set<String> kpiCounterIdPkSet = new HashSet<>();

                for (Map<String, String> counterDetails : counterDetailsList) {
                    String sequenceNo = counterDetails.get("SEQUENCE_NO");
                    String kpiCounterIdPk = counterDetails.get("KPI_COUNTER_ID_PK");

                    logger.error("Sequence No: {}", sequenceNo);
                    logger.error("KPI Counter ID PK: {}", kpiCounterIdPk);

                    if (!kpiCounterIdPkSet.contains(kpiCounterIdPk)) {
                        String counter = "C" + sequenceNo + "#" + kpiCounterIdPk;
                        if (!selectBuilder.contains(counter)) {
                            selectBuilder = selectBuilder + " (`C" + sequenceNo
                                    + "`) AS `C" + sequenceNo + "#"
                                    + kpiCounterIdPk + "` ,";

                            kpiCounterIdPkSet.add(kpiCounterIdPk);
                        }
                        rawCounterList = rawCounterList + "c.`C" + sequenceNo + "#" + kpiCounterIdPk
                                + "`,";

                        rawCounters = rawCounters + "c.`C" + sequenceNo + "#" + kpiCounterIdPk + "`,";

                        kpiCounterIdPkSet.add(kpiCounterIdPk);
                    }
                }
                sequenceColumns = sequenceNoList.toArray(new String[sequenceNoList.size()]);

            } else {

                String[] categoryListArray = categoryList.split(",");

                for (String category : categoryListArray) {

                    if (category == null || category.isEmpty() || category.equalsIgnoreCase("null")) {
                        continue;
                    }

                    if (categoryInfoMapObject.containsKey(category)) {

                        List<Map<String, String>> catInfoList = categoryInfoMapObject.get(category);

                        Set<String> pmCounterVariableIdPkSet = new HashSet<>();
                        String subCategorySequenceColumn = "";

                        for (Map<String, String> counterInfo : catInfoList) {

                            String pmCounterVariableIdPk = counterInfo.get("PM_COUNTER_VARIABLE_ID_PK");
                            String sequenceNo = counterInfo.get("SEQUENCE_NO");
                            String subCategory = getSubCategoryCriteria(counterInfo);
                            subCategorySequenceColumn = subCategory;

                            if (!pmCounterVariableIdPkSet.contains(pmCounterVariableIdPk)) {

                                String counter = "C" + sequenceNo + "#" + pmCounterVariableIdPk;

                                if (!selectBuilder.contains(counter)) {

                                    if (subCategory != null) {
                                        selectBuilder = selectBuilder + " (CASE WHEN " + subCategory + " THEN (`C"
                                                + sequenceNo + "`) ELSE NULL END) AS `C"
                                                + sequenceNo + "#"
                                                + pmCounterVariableIdPk + "`,";

                                    } else {

                                        selectBuilder = selectBuilder + " (`C" + sequenceNo
                                                + "`) AS `C" + sequenceNo + "#"
                                                + pmCounterVariableIdPk + "` ,";

                                    }

                                    rawCounterList = rawCounterList + "c.`C" + sequenceNo + "#" + pmCounterVariableIdPk
                                            + "`,";
                                    rawCounters = rawCounters + "c.`C" + sequenceNo + "#" + pmCounterVariableIdPk
                                            + "`,";

                                    pmCounterVariableIdPkSet.add(pmCounterVariableIdPk);
                                }
                            }
                        }

                        if (subCategorySequenceColumn != null && !subCategorySequenceColumn.isEmpty()) {
                            sequenceNoList.addAll(Arrays.asList(subCategorySequenceColumn.split("AND")).stream()
                                    .map(e -> StringUtils.substringBetween(e, "`", "`"))
                                    .collect(Collectors.toList()));
                        }

                        sequenceColumns = sequenceNoList.toArray(new String[sequenceNoList.size()]);
                    }
                }
            }

            List<String> sequenceColumnsList = Arrays.asList(sequenceColumns);
            logger.error("Sequence Columns List: {}", sequenceColumnsList);

            int lastCommaIndex = selectBuilder.lastIndexOf(",");
            String selectClause = (lastCommaIndex != -1)
                    ? selectBuilder.substring(0, lastCommaIndex)
                    : selectBuilder.toString();

            String fromClause = " FROM TRINO_ORC_DF";

            String query = selectClause + fromClause;
            logger.error("Query: {}", query);

            Dataset<Row> updatedTrinoOrcDF = jobContext.sqlctx().sql(query);
            // updatedTrinoOrcDF.show(5, false);
            updatedTrinoOrcDF.createOrReplaceTempView("UPDATED_TRINO_ORC_DF");
            logger.error("Final Trino ORC DataFrame Created Successfully!");

            logger.error("Raw Counter List: {}", rawCounterList);
            logger.error("Final Query Map: {}", finalQueryMap);

            String joinQuery = "SELECT c.fiveMinuteKey, c.quarterKey, c.dateKey, c.hourKey, c.finalKey, c.interfacename AS pmemsid, c.categoryname, m.NAM, "
                    + rawCounterList + finalQueryMap
                    + " FROM UPDATED_TRINO_ORC_DF c JOIN TRINO_NE_DF m ON UPPER(c.interfacename) =UPPER(m.pmemsid) AND c.dateKey= m.date WHERE c.interfacename IS NOT NULL AND m.pmemsid IS NOT NULL";

            logger.error("Updated Join Query: {}", joinQuery);
            Dataset<Row> joinedResult = jobContext.sqlctx().sql(joinQuery);
            Dataset<Row> updatedResult = joinedResult.withColumn(
                    "metaData",
                    map_concat(
                            col("metaData"),
                            map(lit("L0"), lit("India"))));

            return updatedResult;

        } catch (Exception e) {
            logger.error("Exception While Creating Updated Trino ORC DataFrame. Input={}, Message={}, Error={}",
                    counterIds, e.getMessage(), e);
            return emptyDF;
        }
    }

    private static List<Map<String, String>> getCounterDetailsList(String counterDetailsMapJson) {
        List<Map<String, String>> counterDetailsList = new ArrayList<>();

        if (counterDetailsMapJson == null || counterDetailsMapJson.trim().isEmpty()) {
            return counterDetailsList;
        }

        try {
            Map<String, String> rawMap = new ObjectMapper().readValue(
                    counterDetailsMapJson, new TypeReference<Map<String, String>>() {
                    });

            for (Map.Entry<String, String> entry : rawMap.entrySet()) {
                String id = entry.getKey();
                String rawDetails = entry.getValue();

                Map<String, String> detailMap = new HashMap<>();
                detailMap.put("KPI_COUNTER_ID_PK", id);

                if (rawDetails != null && !rawDetails.trim().isEmpty()) {
                    String[] keyValuePairs = rawDetails.split("##");
                    for (String pair : keyValuePairs) {
                        String[] kv = pair.split("=", 2);
                        if (kv.length == 2) {
                            detailMap.put(kv[0].trim(), kv[1].trim());
                        }
                    }
                }

                counterDetailsList.add(detailMap);
            }

        } catch (JsonMappingException e) {
            logger.error("Error Mapping Counter Details Map: {}", e.getMessage());
        } catch (JsonProcessingException e) {
            logger.error("Error Processing Counter Details Map: {}", e.getMessage());
        }

        return counterDetailsList;
    }

    private static String generateMetaDataMapQuery(String[] trinoNeColumns) {
        StringBuilder finalMapQuery = new StringBuilder("map(");
        for (String col : trinoNeColumns) {
            if (!"pmemsid".equalsIgnoreCase(col)) {
                finalMapQuery.append("'")
                        .append(col)
                        .append("' ,m.`")
                        .append(col)
                        .append("`,");
            }
        }

        int lastCommaIndex = finalMapQuery.lastIndexOf(",");
        if (lastCommaIndex != -1) {
            finalMapQuery.deleteCharAt(lastCommaIndex);
        }

        finalMapQuery.append(") AS metaData");
        return finalMapQuery.toString();
    }

    private static Dataset<Row> getEmptyDF(JobContext jobContext, String counterIds) {

        logger.error("Getting Empty DataFrame For Counter IDs: {}", counterIds);

        StructType schema = getReturnType(counterIds);
        Dataset<Row> emptyDF = jobContext.createDataFrame(Collections.emptyList(), schema);
        return emptyDF;
    }

    public static StructType getReturnType(String countersCommaSeparated) {

        List<StructField> fields = new ArrayList<StructField>();

        try {

            if (countersCommaSeparated == null || countersCommaSeparated.trim().isEmpty()) {
                logger.error("Input String For Counters is NULL/Empty. Proceeding With Static Fields Only.");
            }

            fields.add(DataTypes.createStructField("fiveMinuteKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("quarterKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("dateKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("hourKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("finalKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("pmemsid", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("NAM", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("categoryName", DataTypes.StringType, true));

            if (countersCommaSeparated != null && !countersCommaSeparated.trim().isEmpty()) {
                String[] counters = countersCommaSeparated.split(",");
                for (String counter : counters) {
                    fields.add(DataTypes.createStructField(counter.trim(), DataTypes.StringType, true));
                }
            }

            fields.add(DataTypes.createStructField(
                    "metaData",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true),
                    true));

            logger.error("Successfully Constructed return StructType With {} Fields.", fields.size());

        } catch (Exception e) {
            logger.error("Exception While Building Return Type Schema. Input={}, Message={}, Error={}",
                    countersCommaSeparated, e.getMessage(), e);
        }

        return DataTypes.createStructType(fields);

    }

    private static String getSubCategoryCriteria(Map<String, String> counterInfoMap) {
        String subcategory = null;

        try {
            for (int i = 1; i <= 4; i++) {
                String headerKey = "subcategoryHeader" + i;
                String valueKey = "subcategoryValue" + i;

                String subcategoryHeader = counterInfoMap.get(headerKey);
                String subcategoryValue = counterInfoMap.get(valueKey);

                if (subcategoryHeader != null && subcategoryValue != null) {
                    subcategory = getSubCategory(subcategoryHeader, subcategoryValue, subcategory);
                }
            }
        } catch (Exception e) {
            logger.error("Error While Building Subcategory Criteria From counterInfoMap, Message: {}, Error: {}",
                    e.getMessage(), e);
        }

        return subcategory;
    }

    private static String getSubCategory(String subCategoryHeader, String subCategoryValue, String subcategory) {
        if (!subCategoryValue.contains("Individual")) {
            String subCat1Condition = "`" + subCategoryHeader + "`" + " IN ('" + subCategoryValue.replace(",", "','")
                    + "')";
            subcategory = checkAndAppend(subcategory, subCat1Condition);
        }
        return subcategory;
    }

    private static String checkAndAppend(String subcategory, String subCat1Condition) {
        if (subcategory != null && !subcategory.equalsIgnoreCase("null")) {
            subcategory = subcategory + " AND " + subCat1Condition;
        } else {
            subcategory = subCat1Condition;
        }
        return subcategory;
    }

    private static String getTimeKey(String frequency) {

        String timeKey = "";

        if (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY")) {
            timeKey = "quarterKey ";
        }
        if (frequency.equalsIgnoreCase("DAILY") || frequency.equalsIgnoreCase("PERDAY")
                || frequency.equalsIgnoreCase("WEEKLY") || frequency.equalsIgnoreCase("PERWEEK")
                || frequency.equalsIgnoreCase("MONTHLY") || frequency.equalsIgnoreCase("PERMONTH")
                || frequency.equalsIgnoreCase("YEARLY") || frequency.equalsIgnoreCase("PERYEAR")) {
            timeKey = "dateKey ";
        }
        if (frequency.equalsIgnoreCase("HOURLY") || frequency.equalsIgnoreCase("PERHOUR")) {
            timeKey = "hourKey ";
        }

        logger.error("GeneratedTime Key={}", timeKey);

        return timeKey;
    }

    private static Dataset<Row> readTrinoOrcFileFromMinio(String filePaths, String basePath, JobContext jobContext) {
        // logger.error("📊 Starting to read files from MinIO. File Paths: {}",
        // filePaths);

        // Validate input parameters
        if (StringUtils.isBlank(filePaths)) {
            throw new IllegalArgumentException("File Paths Parameter Cannot Be Null or Empty!");
        }
        if (StringUtils.isBlank(basePath)) {
            throw new IllegalArgumentException("Base Path Parameter Cannot Be Null or Empty!");
        }

        try {
            // Get MinIO configuration parameters
            String endpointUrl = jobContext.getParameter("SPARK_MINIO_ENDPOINT_URL");
            String accessKey = jobContext.getParameter("SPARK_MINIO_ACCESS_KEY");
            String secretKey = jobContext.getParameter("SPARK_MINIO_SECRET_KEY");
            String bucketName = jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM");

            // Validate MinIO parameters
            validateMinioParameters(endpointUrl, accessKey, secretKey, bucketName);

            logger.error("Spark MinIO Endpoint URL: {}", endpointUrl);
            logger.error("Spark MinIO Bucket Name: {}", bucketName);

            // Configure MinIO parameters
            configureMinioParameters(jobContext, endpointUrl, accessKey, secretKey);
            logger.error("MinIO/S3A Configuration Set Successfully!");

            // Validate ORC path format
            validateOrcPath(filePaths);
            logger.error("Path Format Validated Successfully!");

            // Prepare reading options
            Map<String, String> optionsMap = new HashMap<>();
            optionsMap.put("basePath", basePath);
            optionsMap.put("mergeSchema", "true");

            // Get filter query
            String filterQuery = getFilterQuery(jobContext);
            logger.error("Filter Query: {}", filterQuery);

            // Process file paths and collect valid paths
            List<String> validPaths = new ArrayList<>();
            String[] pathArray = filePaths.split(",");

            for (String rawPath : pathArray) {
                String trimmedPath = rawPath.trim();
                if (StringUtils.isBlank(trimmedPath)) {
                    continue;
                }

                // Validate path format without checking existence (avoid S3A wildcard issues)
                if (trimmedPath.startsWith("s3a://")) {
                    validPaths.add(trimmedPath);
                    // logger.error("Added Valid Path Pattern: {}", trimmedPath);
                } else {
                    logger.error("Invalid Path Format (must start with s3a://): {}", trimmedPath);
                }
            }

            if (validPaths.isEmpty()) {
                throw new RuntimeException("No Valid File Paths Found In The Provided Paths: " + filePaths);
            }

            logger.error("Found {} Valid File Patterns To Process", validPaths.size());

            // Check file existence and list existing files
            List<String> existingFilePaths = checkAndListExistingFiles(jobContext, validPaths);

            if (existingFilePaths.isEmpty()) {
                // throw new RuntimeException("No ORC Files Found In Any Of The Provided Paths:
                // " + filePaths);
                return null;
            }

            logger.error("Found {} ORC Files To Read", existingFilePaths.size());

            // Read all existing files in a single shot
            Dataset<Row> orcDataFrame = readExistingFilesInSingleShot(jobContext, existingFilePaths, optionsMap,
                    filterQuery);

            if (orcDataFrame != null) {
                orcDataFrame = orcDataFrame.cache();
                logger.error("DataFrame Cached Successfully. Row Count: {}", orcDataFrame.count());
            } else {
                throw new RuntimeException("Failed To Read Any Data From The Provided File Paths");
            }

            return orcDataFrame;

        } catch (IllegalArgumentException e) {
            logger.error("Configuration Error: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Unknown error";

            if (errorMsg.contains("File does not exist") || errorMsg.contains("No such file")) {
                throw new RuntimeException("Files not found at path: " + filePaths, e);
            } else if (errorMsg.contains("Access Denied") || errorMsg.contains("Permission denied")) {
                throw new RuntimeException("Access denied to files at: " + filePaths, e);
            } else if (errorMsg.contains("Connection") || errorMsg.contains("timeout")) {
                throw new RuntimeException("Connection failed to MinIO at: " + filePaths, e);
            } else if (errorMsg.contains("Invalid ORC")) {
                throw new RuntimeException("Invalid ORC file format in: " + filePaths, e);
            } else {
                logger.error("Critical error in readTrinoOrcFileFromMinio: {}", errorMsg, e);
                throw new RuntimeException("Failed to read files from MinIO: " + errorMsg, e);
            }
        }
    }

    /**
     * Check file existence and list existing files using Spark's file listing
     * capabilities
     */
    private static List<String> checkAndListExistingFiles(JobContext jobContext, List<String> validPaths) {
        List<String> existingFilePaths = new ArrayList<>();

        logger.error("Checking ORC File Existence For {} Path Patterns", validPaths.size());

        for (String pathPattern : validPaths) {
            try {
                // logger.error("Checking Path Pattern: {}", pathPattern);

                Dataset<Row> fileList = jobContext.sqlctx().read()
                        .format("binaryFile")
                        .option("pathGlobFilter", "*.orc")
                        .option("recursiveFileLookup", "true")
                        .load(pathPattern);

                List<String> orcFilesInPattern = fileList.select("path")
                        .collectAsList()
                        .stream()
                        .map(row -> row.getString(0))
                        .filter(filePath -> filePath.toLowerCase().endsWith(".orc"))
                        .collect(Collectors.toList());

                if (!orcFilesInPattern.isEmpty()) {
                    existingFilePaths.addAll(orcFilesInPattern);
                    logger.error("Found {} ORC Files In Pattern: {}", orcFilesInPattern.size(), pathPattern);
                    logger.error("ORC Files: {}", orcFilesInPattern);
                } else {
                    // logger.error("No ORC Files Found In Pattern: {}", pathPattern);
                }

            } catch (Exception e) {
                // logger.error("No ORC Files Found In Pattern: {}", pathPattern);
            }
        }

        logger.error("Total ORC Files Found: {}", existingFilePaths.size());

        if (existingFilePaths.isEmpty()) {
            logger.error("No ORC Files Found In Any Of The Provided Paths!");
        } else {
            logger.error("Successfully Found ORC Files In {} Paths",
                    existingFilePaths.stream().map(path -> {
                        int lastSlash = path.lastIndexOf('/');
                        return lastSlash > 0 ? path.substring(0, lastSlash) : path;
                    }).distinct().count());
        }

        return existingFilePaths;
    }

    /**
     * Read all existing files in a single shot
     */
    private static Dataset<Row> readExistingFilesInSingleShot(JobContext jobContext, List<String> existingFilePaths,
            Map<String, String> optionsMap, String filterQuery) {
        logger.error("Reading {} Files In Single Shot", existingFilePaths.size());

        try {
            String[] filePathsArray = existingFilePaths.toArray(new String[0]);

            Dataset<Row> orcDataFrame = jobContext.getFileReader()
                    .options(optionsMap)
                    .orc(filePathsArray);

            if (StringUtils.isNotBlank(filterQuery)) {
                orcDataFrame = orcDataFrame.where(filterQuery);
                logger.error("Applied Filter Query: {}", filterQuery);
            } else {
                logger.error("No Filter Query Applied (Filter Query is null or empty)");
            }

            logger.error("Successfully Read All Files In Single Shot");
            return orcDataFrame;

        } catch (Exception e) {
            logger.error("Error Reading Files In Single Shot: {}", e.getMessage());
            logger.error("Falling Back To Individual File Reading For {} Files", existingFilePaths.size());
            return readFilesIndividually(jobContext, existingFilePaths, optionsMap, filterQuery);
        }
    }

    /**
     * Fallback method to read files individually when bulk reading fails
     */
    private static Dataset<Row> readFilesIndividually(JobContext jobContext, List<String> existingFilePaths,
            Map<String, String> optionsMap, String filterQuery) {
        Dataset<Row> combinedDataFrame = null;
        int successCount = 0;
        int failureCount = 0;

        logger.error("Starting Individual File Reading For {} Files", existingFilePaths.size());

        for (String filePath : existingFilePaths) {
            try {
                logger.error("Attempting to read: {}", filePath);

                Dataset<Row> df = jobContext.getFileReader()
                        .options(optionsMap)
                        .orc(filePath);

                if (StringUtils.isNotBlank(filterQuery)) {
                    df = df.where(filterQuery);
                }

                long rowCount = df.count();
                if (rowCount > 0) {
                    if (combinedDataFrame == null) {
                        combinedDataFrame = df;
                    } else {
                        combinedDataFrame = combinedDataFrame.unionByName(df, true);
                    }
                    successCount++;
                    logger.error("Successfully Read File: {} ({} rows)", filePath, rowCount);
                } else {
                    logger.error("File Has No Data After Filtering: {}", filePath);
                }

            } catch (Exception e) {
                failureCount++;
                String errorMsg = e.getMessage() != null ? e.getMessage() : "Unknown error";
                logger.error("Failed To Read File: {}. Error: {}", filePath, errorMsg);

                if (errorMsg.contains("AbortedException") || errorMsg.contains("listStatus")) {
                    logger.error("S3A File Access Issue Detected For: {}", filePath);
                }
            }
        }

        logger.error("Individual File Reading Completed. Success: {}, Failures: {}", successCount, failureCount);

        if (combinedDataFrame == null) {
            throw new RuntimeException(
                    "Failed To Read Any Files Individually. All " + existingFilePaths.size() + " Files Failed.");
        }

        return combinedDataFrame;
    }

    private static String getFilterQuery(JobContext jobContext) {

        String frequency = jobContext.getParameter("FREQUENCY");
        String timeKeysCommaSeparated = jobContext.getParameter("TIME_KEYS_COMMA_SEPARATED");

        String timeKey = "";
        String upperFreq = frequency.toUpperCase();

        Set<String> fiveMinuteKeys = Set.of("FIVEMIN", "FIVE_MIN", "FIVEMINUTE", "FIVE_MINUTE", "5 MIN", "5MIN");
        Set<String> quarterKeys = Set.of("15 MIN", "QUARTERLY");
        Set<String> dateKeys = Set.of("DAILY", "PERDAY", "WEEKLY", "PERWEEK", "MONTHLY", "PERMONTH", "YEARLY",
                "PERYEAR");
        Set<String> hourKeys = Set.of("HOURLY", "PERHOUR");

        if (fiveMinuteKeys.contains(upperFreq)) {
            timeKey = "fiveMinuteKey";
        } else if (quarterKeys.contains(upperFreq)) {
            timeKey = "quarterKey";
        } else if (dateKeys.contains(upperFreq)) {
            timeKey = "dateKey";
        } else if (hourKeys.contains(upperFreq)) {
            timeKey = "hourKey";
        }

        String[] timeKeys = timeKeysCommaSeparated.split(",");
        String filterQuery = timeKey + " IN ('" + String.join("','", timeKeys) + "')";

        return filterQuery;
    }

    private static void validateOrcPath(String trinoOrcPath) {
        if (!trinoOrcPath.startsWith("s3a://")) {
            throw new IllegalArgumentException(
                    "Invalid ORC Path Format. Expected s3a:// Prefix, Got: " + trinoOrcPath);
        }
    }

    private static void configureMinioParameters(JobContext jobContext, String sparkMinioEndpointUrl,
            String sparkMinioAccessKey, String sparkMinioSecretKey) {
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.access.key", sparkMinioAccessKey);
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.secret.key", sparkMinioSecretKey);
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.endpoint", sparkMinioEndpointUrl);
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.path.style.access", "true");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.connection.maximum", "1000");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.connection.timeout", "600000");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.connection.ttl", "600000");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.connection.establish.timeout", "600000");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.connection.keepalive.time", "600000");

        // Additional S3A configurations for better wildcard handling
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.experimental.input.fadvise", "normal");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.threads.max", "20");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.threads.keepalivetime", "60");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.max.total.tasks", "32");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.block.size", "134217728");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.buffer.dir", "/tmp");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.retry.limit", "10");
        jobContext.sqlctx().sparkSession().conf().set("spark.hadoop.fs.s3a.retry.interval", "1000");
    }

    private static void validateMinioParameters(String sparkMinioEndpointUrl, String sparkMinioAccessKey,
            String sparkMinioSecretKey, String sparkMinioBucketName) {

        if (sparkMinioEndpointUrl == null || sparkMinioEndpointUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("SPARK_MINIO_ENDPOINT_URL Parameter is Required But Not Provided!");
        }
        if (sparkMinioAccessKey == null || sparkMinioAccessKey.trim().isEmpty()) {
            throw new IllegalArgumentException("SPARK_MINIO_ACCESS_KEY Parameter is Required But Not Provided!");
        }
        if (sparkMinioSecretKey == null || sparkMinioSecretKey.trim().isEmpty()) {
            throw new IllegalArgumentException("SPARK_MINIO_SECRET_KEY Parameter is Required But Not Provided!");
        }
        if (sparkMinioBucketName == null || sparkMinioBucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("SPARK_MINIO_BUCKET_NAME_PM Parameter is Required But Not Provided!");
        }

    }
}
