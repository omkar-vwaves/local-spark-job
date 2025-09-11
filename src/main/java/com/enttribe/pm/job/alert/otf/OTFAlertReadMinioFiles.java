package com.enttribe.pm.job.alert.otf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OTFAlertReadMinioFiles extends Processor {

    public OTFAlertReadMinioFiles() {
        super();
    }

    public OTFAlertReadMinioFiles(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
    }

    public OTFAlertReadMinioFiles(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (this.dataFrame == null || this.dataFrame.isEmpty()) {
            return this.dataFrame;
        }

        String trinoOrcPath = getTrinoOrcPath(jobContext);
        String trinoNePath = getTrinoNePath(jobContext);

        logger.info("Trino ORC Path: {}, Trino NE Path: {}", trinoOrcPath, trinoNePath);

        String counterInfoMap = jobContext.getParameter("COUNTER_INFO_MAP");
        String categoryInfoMap = jobContext.getParameter("CATEGORY_INFO_MAP");
        String categoryList = jobContext.getParameter("CATEGORY_LIST");

        @SuppressWarnings("unchecked")
        Map<String, List<Map<String, String>>> categoryInfoMapObject = new ObjectMapper().readValue(categoryInfoMap,
                Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> counterInfoMapObject = new ObjectMapper().readValue(counterInfoMap,
                Map.class);

        /*
         * READ TRINO ORC
         */

        String baseTrinoOrcPath = jobContext.getParameter("BASE_TRINO_ORC_PATH");
        logger.info("Base Trino ORC Path: {}", baseTrinoOrcPath);
        Dataset<Row> trinoOrcDF = null;

        String counterIds = counterInfoMapObject.entrySet().stream()
                .map(entry -> "C" + entry.getValue().get("SEQUENCE_NO") + "#"
                        + entry.getValue().get("PM_COUNTER_VARIABLE_ID_PK"))
                .distinct().collect(Collectors.joining(","));

        try {
            trinoOrcDF = readFileFromMinio(trinoOrcPath, baseTrinoOrcPath, jobContext);
            if (trinoOrcDF == null || trinoOrcDF.isEmpty()) {
                logger.info("Trino ORC DataFrame is Null or Empty! Returning Empty DataFrame!");
                Dataset<Row> result = getEmptyDF(jobContext, counterIds);
                result.show();
                logger.info("+++++++++++++++++++++[TRINO ORC WITH NE_META (NO ORC FOUND)]+++++++++++++++++++++");
                return result;
            }
            trinoOrcDF.createOrReplaceTempView("TRINO_ORC_DF");
        } catch (Exception e) {
            logger.error("Exception While Reading Trino ORC Files. Input={}, Message={}, Error={}",
                    trinoOrcPath, e.getMessage(), e);

            Dataset<Row> result = getEmptyDF(jobContext, counterIds);
            result.show();
            logger.info("+++++++++++++++++++++[TRINO ORC WITH NE_META (NO ORC FOUND)]+++++++++++++++++++++");
            return result;
        }

        /*
         * READ TRINO NE
         */
        String baseTrinoNePath = jobContext.getParameter("BASE_TRINO_NE_PATH");
        logger.info("Base Trino NE Path: {}", baseTrinoNePath);
        Dataset<Row> trinoNeDF = null;
        try {
            trinoNeDF = readFileFromMinio(trinoNePath, baseTrinoNePath, jobContext);
            if (trinoNeDF == null || trinoNeDF.isEmpty()) {
                logger.info("Trino NE DataFrame is Null or Empty! Returning Empty DataFrame!");
                Dataset<Row> result = getEmptyDF(jobContext, counterIds);
                result.show();
                logger.info("+++++++++++++++++++++[TRINO ORC WITH NE_META (NO NE_META FOUND)]+++++++++++++++++++++");
                return result;
            }
            trinoNeDF = trinoNeDF.withColumn("L0", org.apache.spark.sql.functions.lit("Custom"));
            trinoNeDF.createOrReplaceTempView("TRINO_NE_DF");

            // Filter Trino NE DF
            String START_INDEX = jobContext.getParameter("START_INDEX");
            logger.info("OTFAlertReadMinioFiles START INDEX: {}", START_INDEX);

            String nodeAndAggregationDetailsMapJson = jobContext
                    .getParameter("NODE_AND_AGGREGATION_DETAILS_MAP" + START_INDEX);
            Map<String, String> nodeAndAggregationDetailsMap = new ObjectMapper()
                    .readValue(nodeAndAggregationDetailsMapJson, new TypeReference<Map<String, String>>() {
                    });

            logger.info("Node And Aggregation Details Map: {}", nodeAndAggregationDetailsMap);

            String isGeoL1MultiSelect = nodeAndAggregationDetailsMap.get("isGeoL1MultiSelect");
            String isGeoL2MultiSelect = nodeAndAggregationDetailsMap.get("isGeoL2MultiSelect");
            String isGeoL3MultiSelect = nodeAndAggregationDetailsMap.get("isGeoL3MultiSelect");
            String isGeoL4MultiSelect = nodeAndAggregationDetailsMap.get("isGeoL4MultiSelect");

            List<String> filterConditions = new ArrayList<>();

            if (StringUtils.isNotBlank(isGeoL1MultiSelect) && isGeoL1MultiSelect.equalsIgnoreCase("true")) {
                String geoL1List = nodeAndAggregationDetailsMap.get("geoL1List");
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

            if (StringUtils.isNotBlank(isGeoL2MultiSelect) && isGeoL2MultiSelect.equalsIgnoreCase("true")) {
                String geoL2List = nodeAndAggregationDetailsMap.get("geoL2List");
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

            if (StringUtils.isNotBlank(isGeoL3MultiSelect) && isGeoL3MultiSelect.equalsIgnoreCase("true")) {
                String geoL3List = nodeAndAggregationDetailsMap.get("geoL3List");
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

            if (StringUtils.isNotBlank(isGeoL4MultiSelect) && isGeoL4MultiSelect.equalsIgnoreCase("true")) {
                String geoL4List = nodeAndAggregationDetailsMap.get("geoL4List");
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

            String geoL1 = nodeAndAggregationDetailsMap.get("geoL1");
            if (geoL1.equalsIgnoreCase("Custom")) {
                String cellsList = nodeAndAggregationDetailsMap.get("cellsList");
                cellsList = cellsList.replace("[", "").replace("]", "");
                if (StringUtils.isNotBlank(cellsList)) {
                    String[] cellsListArray = cellsList.split(",");
                    String cellsListQuoted = Arrays.stream(cellsListArray)
                            .map(String::trim)
                            .filter(StringUtils::isNotBlank)
                            .map(e -> "'" + e.toUpperCase() + "'")
                            .collect(Collectors.joining(","));
                    if (StringUtils.isNotBlank(cellsListQuoted)) {
                        filterConditions.add("UPPER(H1) IN (" + cellsListQuoted + ")");
                    }
                }
            }

            String isNodeNameListEmpty = nodeAndAggregationDetailsMap.get("isNodeMultiSelect");
            if (StringUtils.isNotBlank(isNodeNameListEmpty) && isNodeNameListEmpty.equalsIgnoreCase("true")) {
                String nodeInfoMapJson = nodeAndAggregationDetailsMap.get("NODE_INFO_MAP");

                @SuppressWarnings("unchecked")
                Map<String, String> nodeInfoMapObject = new ObjectMapper().readValue(nodeInfoMapJson, Map.class);

                List<String> upperNames = nodeInfoMapObject.values().stream()
                        .map(name -> "'" + name.toUpperCase() + "'")
                        .collect(Collectors.toList());

                if (!upperNames.isEmpty()) {
                    String inClause = String.join(",", upperNames);
                    filterConditions.add("UPPER(H1) IN (" + inClause + ")");
                }
            }

            String filterQuery = "";
            if (!filterConditions.isEmpty()) {
                filterQuery = String.join(" AND ", filterConditions);
                logger.info("Final Filter Query: {}", filterQuery);

                trinoNeDF = trinoNeDF.where(filterQuery);
                trinoNeDF.createOrReplaceTempView("TRINO_NE_DF");
                logger.info("Trino NE DataFrame Filtered Successfully!");
            } else {
                logger.info("No Filter Query Applied (No Valid Filter Conditions Found)");
            }

        } catch (Exception e) {
            logger.error("Exception While Reading Trino NE Files. Input={}, Message={}, Error={}",
                    trinoNePath, e.getMessage(), e);
            Dataset<Row> result = getEmptyDF(jobContext, counterIds);
            result.show();
            logger.info("+++++++++++++++++++++[TRINO ORC WITH NE_META (EXCEPTION OCCURED)]+++++++++++++++++++++");
            return result;
        }

        /*
         * FILTER TRINO NE
         */
        String neType = jobContext.getParameter("NE_TYPE");
        String filterQuery = "SELECT * FROM TRINO_NE_DF WHERE UPPER(NET) IN (" + neType + ")";

        logger.info("Filter Query: {}", filterQuery);

        trinoNeDF = jobContext.sqlctx().sql(filterQuery);
        trinoNeDF.createOrReplaceTempView("TRINO_NE_DF");
        logger.info("Trino NE DataFrame Filtered Successfully!");

        String[] trinoNeColumns = trinoNeDF.columns();
        String finalQueryMap = generateMetaDataMapQuery(trinoNeColumns);

        try {
            Dataset<Row> joinedResult = getFinalResult(jobContext, categoryInfoMapObject,
                    categoryList, counterInfoMapObject, finalQueryMap);

            joinedResult.createOrReplaceTempView("JOINED_RESULT");
            joinedResult.show();
            logger.info("+++++++++++++++++++++[TRINO ORC WITH NE_META (JOINED RESULT)]+++++++++++++++++++++");
            return joinedResult;
        } catch (Exception e) {
            logger.error("Exception While Joining Trino ORC and Trino NE DataFrames. Input={}, Message={}, Error={}",
                    counterInfoMapObject, e.getMessage(), e);
            Dataset<Row> result = getEmptyDF(jobContext, counterIds);
            result.show();
            logger.info("+++++++++++++++++++++[TRINO ORC WITH NE_META (EXCEPTION OCCURED)]+++++++++++++++++++++");
            return result;
        }

    }

    private static Dataset<Row> getFinalResult(JobContext jobContext,
            Map<String, List<Map<String, String>>> categoryInfoMapObject, String categoryList,
            Map<String, Map<String, String>> counterInfoMapObject, String finalQueryMap) {

        String counterIds = counterInfoMapObject.entrySet().stream()
                .map(e -> "C" + e.getValue().get("SEQUENCE_NO") + "#"
                        + e.getValue().get("PM_COUNTER_VARIABLE_ID_PK"))
                .distinct().collect(Collectors.joining(","));

        logger.info("Final Counter Info: {}", counterIds);

        Dataset<Row> emptyDF = getEmptyDF(jobContext, counterIds);
        emptyDF.createOrReplaceTempView("EMPTY_DF");
        logger.info("Empty DataFrame Created Successfully!");

        try {

            Set<String> sequenceNoList = new HashSet<>();
            sequenceNoList.add("quarterKey");
            sequenceNoList.add("fiveminutekey");
            sequenceNoList.add("dateKey");
            sequenceNoList.add("hourKey");
            sequenceNoList.add("pmemsid");
            sequenceNoList.add("interfacename");
            sequenceNoList.add("categoryname");

            for (String counterID : counterIds.split(",")) {
                String sequenceno = counterID.split("#")[0];
                sequenceNoList.add(sequenceno);
            }

            logger.info("Sequence No List={}", sequenceNoList);

            String selectBuilder = " SELECT fiveminutekey, quarterKey, dateKey, hourKey,"
                    + getTimeKey(jobContext.getParameter("FREQUENCY"))
                    + " AS finalKey, pmemsid, interfacename, categoryname, ";

            String rawCounterList = "";
            String rawCounters = "";
            String[] categoryListArray = categoryList.split(",");
            String[] sequenceColumns = null;

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
                                rawCounters = rawCounters + "c.`C" + sequenceNo + "#" + pmCounterVariableIdPk + "`,";

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

            List<String> sequenceColumnsList = Arrays.asList(sequenceColumns);
            logger.info("Sequence Columns List: {}", sequenceColumnsList);

            int lastCommaIndex = selectBuilder.lastIndexOf(",");
            String selectClause = (lastCommaIndex != -1)
                    ? selectBuilder.substring(0, lastCommaIndex)
                    : selectBuilder.toString();

            String fromClause = " FROM TRINO_ORC_DF";

            String query = selectClause + fromClause;
            logger.info("Built Query: {}", query);

            Dataset<Row> updatedTrinoOrcDF = jobContext.sqlctx().sql(query);
            updatedTrinoOrcDF.createOrReplaceTempView("UPDATED_TRINO_ORC_DF");
            logger.info("Final Trino ORC DataFrame Created Successfully!");

            logger.info("Raw Counter List: {}", rawCounterList);
            logger.info("Final Query Map: {}", finalQueryMap);

            String joinQuery = "SELECT c.fiveminutekey, c.quarterKey, c.dateKey, c.hourKey, c.finalKey, c.interfacename AS pmemsid, c.categoryname, m.NAM, "
                    + rawCounterList + finalQueryMap
                    + " FROM UPDATED_TRINO_ORC_DF c JOIN TRINO_NE_DF m ON UPPER(c.interfacename) =UPPER(m.pmemsid) AND c.dateKey= m.date";

            logger.info("Join Query: {}", joinQuery);
            Dataset<Row> joinedResult = jobContext.sqlctx().sql(joinQuery);
            return joinedResult;

        } catch (Exception e) {
            logger.error("Exception While Creating Updated Trino ORC DataFrame. Input={}, Message={}, Error={}",
                    counterIds, e.getMessage(), e);
            return emptyDF;
        }
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

        StructType schema = getReturnType(counterIds);
        Dataset<Row> emptyDF = jobContext.createDataFrame(Collections.emptyList(), schema);
        emptyDF.createOrReplaceTempView("JOINED_RESULT");
        logger.info("Empty DataFrame[JOINED_RESULT] Created Successfully!");
        return emptyDF;
    }

    public static StructType getReturnType(String countersCommaSeparated) {

        List<StructField> fields = new ArrayList<StructField>();

        try {

            if (countersCommaSeparated == null || countersCommaSeparated.trim().isEmpty()) {
                logger.error("Input String For Counters is NULL/Empty. Proceeding With Static Fields Only.");
            }

            fields.add(DataTypes.createStructField("fiveminutekey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("quarterKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("dateKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("hourKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("finalKey", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("moHierachy", DataTypes.StringType, true));
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

            logger.info("Successfully Constructed return StructType With {} Fields.", fields.size());

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

        logger.info("Getting Time Key For Frequency: {}", frequency);

        String timeKey = "";

        if (frequency.equalsIgnoreCase("5 MIN") || frequency.equalsIgnoreCase("FIVEMIN")) {
            timeKey = "fiveminutekey ";
        } else if (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY")) {
            timeKey = "quarterKey ";
        } else if (frequency.equalsIgnoreCase("DAILY") || frequency.equalsIgnoreCase("PERDAY")
                || frequency.equalsIgnoreCase("WEEKLY") || frequency.equalsIgnoreCase("PERWEEK")
                || frequency.equalsIgnoreCase("MONTHLY") || frequency.equalsIgnoreCase("PERMONTH")) {
            timeKey = "dateKey ";
        } else if (frequency.equalsIgnoreCase("HOURLY") || frequency.equalsIgnoreCase("PERHOUR")) {
            timeKey = "hourKey ";
        } else {
            timeKey = "quarterKey ";
        }

        logger.info("Time Key: {}", timeKey);

        return timeKey;
    }

    private static Dataset<Row> readFileFromMinio(String filePath, String basePath, JobContext jobContext) {

        logger.info("File Path: {}", filePath);

        try {

            String endpointUrl = jobContext.getParameter("SPARK_MINIO_ENDPOINT_URL");
            String accessKey = jobContext.getParameter("SPARK_MINIO_ACCESS_KEY");
            String secretKey = jobContext.getParameter("SPARK_MINIO_SECRET_KEY");
            String bucketName = jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM");

            validateMinioParameters(endpointUrl, accessKey, secretKey, bucketName);

            logger.info("Spark MinIO Endpoint URL: {}, Access Key: {}, Secret Key: {}, Bucket Name: {}", endpointUrl,
                    accessKey, secretKey, bucketName);

            configureMinioParameters(jobContext, endpointUrl, accessKey, secretKey);
            logger.info("MinIO/S3A Configuration Set Successfully!");

            validateOrcPath(filePath);
            logger.info("Path Validated Successfully!");

            logger.info("Reading Files From Path: {}", filePath);

            Dataset<Row> orcDataFrame = jobContext.sqlctx().read()
                    .format("orc")
                    .option("mergeSchema", "true")
                    .option("basePath", basePath)
                    .load(filePath);

            orcDataFrame = orcDataFrame.cache();
            logger.info("DataFrame Cached Successfully!");

            return orcDataFrame;

        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("File does not exist")) {
                throw new RuntimeException("Files Not Found At Path: " + filePath, e);
            } else if (msg.contains("Access Denied")) {
                throw new RuntimeException("Access Denied To Files At: " + filePath, e);
            } else if (msg.contains("Connection")) {
                throw new RuntimeException("Connection Failed To MinIO At: " + filePath, e);
            } else {
                logger.error("Critical Error In ReadFiles: {}", msg);
                // throw new RuntimeException("Failed To Read Files", e);
                return null;
            }
        }
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

    private static String getTrinoOrcPath(JobContext jobContext) {

        String baseTrinoOrcPath = jobContext.getParameter("BASE_TRINO_ORC_PATH");
        String baseTrinoPathDate = jobContext.getParameter("BASE_TRINO_ORC_PATH_DATE");
        String baseTrinoPathTime = jobContext.getParameter("BASE_TRINO_ORC_PATH_TIME");
        String domain = jobContext.getParameter("DOMAIN");
        String vendor = jobContext.getParameter("VENDOR");
        String emsType = jobContext.getParameter("EMS_TYPE");
        String technology = jobContext.getParameter("TECHNOLOGY");
        String frequency = jobContext.getParameter("FREQUENCY");
        String frequencyPartition = "";

        logger.info(
                "Getting Trino ORC Path With Inputs: BASE_TRINO_ORC_PATH={}, BASE_TRINO_ORC_PATH_DATE={}, BASE_TRINO_ORC_PATH_TIME={}, DOMAIN={}, VENDOR={}, EMS_TYPE={}, TECHNOLOGY={}, FREQUENCY={}",
                baseTrinoOrcPath, baseTrinoPathDate, baseTrinoPathTime, domain, vendor, emsType, technology, frequency);

        switch (frequency.toUpperCase()) {
            case "5 MIN":
                frequencyPartition = "FIVE_MIN";
                break;
            case "FIVEMIN":
                frequencyPartition = "FIVE_MIN";
                break;
            case "15 MIN":
                frequencyPartition = "QUARTERLY";
                break;
            case "QUARTERLY":
                frequencyPartition = "QUARTERLY";
                break;
            default:
                frequencyPartition = "QUARTERLY";
                break;
        }

        String trinoOrcPath = baseTrinoOrcPath + "frequency=" + frequencyPartition + "/domain=" + domain + "/vendor="
                + vendor + "/emstype=" + emsType
                + "/technology=" + technology + "/date=" + baseTrinoPathDate + "/time=" + baseTrinoPathTime + "/"
                + "ptime=*/" + "categoryname=*";

        logger.info("Trino ORC Path: {}", trinoOrcPath);

        return trinoOrcPath;
    }

    private static String getTrinoNePath(JobContext jobContext) {

        String baseTrinoNePath = jobContext.getParameter("BASE_TRINO_NE_PATH");
        String baseTrinoNePathDate = jobContext.getParameter("BASE_TRINO_NE_PATH_DATE");

        String domain = jobContext.getParameter("DOMAIN");
        String vendor = jobContext.getParameter("VENDOR");
        String emsType = jobContext.getParameter("EMS_TYPE");
        String technology = jobContext.getParameter("TECHNOLOGY");

        logger.info(
                "Getting Trino NE Path With Inputs: BASE_TRINO_NE_PATH={}, BASE_TRINO_NE_PATH_DATE={}, DOMAIN={}, VENDOR={}, EMS_TYPE={}, TECHNOLOGY={}",
                baseTrinoNePath, baseTrinoNePathDate, domain, vendor, emsType, technology);

        String trinoNePath = baseTrinoNePath + "d=" + domain + "/v=" + vendor + "/emstype=" + emsType
                + "/t=" + technology + "/date=" + baseTrinoNePathDate + "/";

        logger.info("Trino NE Path: {}", trinoNePath);

        return trinoNePath;
    }
}
