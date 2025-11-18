package com.enttribe.pm.job.report.otf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;

import static org.apache.spark.sql.functions.*;

public class OTFDFGenerator extends Processor {

    private static Logger logger = LoggerFactory.getLogger(OTFDFGenerator.class);
    private static final boolean IS_LOG_ENABLED = true;

    public OTFDFGenerator() {
        super();
    }

    public OTFDFGenerator(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (IS_LOG_ENABLED) {
            logger.info("OTFDFGenerator Execution Started!");
        }

        Dataset<Row> dataFrame = this.dataFrame;
        if (dataFrame == null || dataFrame.isEmpty()) {
            return createEmptyDF(jobContext);
        }

        if (IS_LOG_ENABLED) {
            this.dataFrame.show(25, false);
            logger.info("++++++++++[KPI EVALUATOR RESULT]++++++++++");
        }
        return generateReportDF(dataFrame, jobContext);
    }

    private Dataset<Row> generateReportDF(Dataset<Row> dataFrame, JobContext jobContext) {

        try {
            String metaColumns = jobContext.getParameter("META_COLUMNS");

            Map<String, String> metaColumnsMap = new ObjectMapper().readValue(metaColumns,
                    new TypeReference<Map<String, String>>() {
                    });

            String dynamicQuery = generateDynamicQuery(metaColumnsMap, jobContext);

            if (IS_LOG_ENABLED) {
                logger.info("Meta Columns: {}", metaColumnsMap);
                logger.info("Dynamic Query: {}", dynamicQuery);
            }

            dataFrame.createOrReplaceTempView("InputData");
            Dataset<Row> stableDataDF = jobContext.sqlctx().sql(dynamicQuery);
            if (IS_LOG_ENABLED) {
                logger.info("Stable Dataframe Created Successfully!");
            }

            List<String> metaColumnKeys = new ArrayList<>(metaColumnsMap.keySet());
            if (IS_LOG_ENABLED) {
                logger.info("Meta Column Keys: {}", metaColumnKeys);
            }

            List<String> defaultMetaColumnKeys = Arrays.asList("DT", "HR", "COUNTRY", "DL1", "DL2", "DL3", "DL4",
                    "NODENAME");
            if (IS_LOG_ENABLED) {
                logger.info("Default Meta Column Keys: {}", defaultMetaColumnKeys);
            }

            String selectedHeader = jobContext.getParameter("selectedHeader");
            selectedHeader = selectedHeader == null ? "default" : selectedHeader;

            List<String> enrichFromNEKeys = new ArrayList<>();

            if (selectedHeader.equalsIgnoreCase("default")) {
                enrichFromNEKeys = new ArrayList<>();

            } else {
                enrichFromNEKeys = metaColumnKeys.stream()
                        .filter(col -> !defaultMetaColumnKeys.contains(col))
                        .collect(Collectors.toList());
            }

            if (IS_LOG_ENABLED) {
                logger.info("Enrich From NE Keys: {}", enrichFromNEKeys);
            }

            Dataset<Row> joinedDF = stableDataDF;

            // Stop using DB enrichment; NE_META enrichment already present upstream
            // (TRINO_NE_DF -> metaData)
            // Use stableDataDF as-is here
                joinedDF = stableDataDF;

            if (IS_LOG_ENABLED) {
                // joinedDF.show(5, false);
                // logger.info("Joined Dataframe Shown Successfully!");
            }

            // NEW: Conditional dynamic grouping based on nemetaColumnsAggregation and selectedHeader='custom'
            // This uses NE_META ORC enrichment and nemeta_groupColumns
            String selectedHeaderEff = jobContext.getParameter("selectedHeader");
            selectedHeaderEff = selectedHeaderEff == null ? "default" : selectedHeaderEff;
            String nemetaColumnsAggregation = jobContext.getParameter("NEMETA_COLUMNS_AGGREGATION");
            nemetaColumnsAggregation = nemetaColumnsAggregation == null ? "false" : nemetaColumnsAggregation;
            String nemetaGroupColumns = jobContext.getParameter("NEMETA_GROUP_COLUMNS");
            nemetaGroupColumns = nemetaGroupColumns == null ? "" : nemetaGroupColumns.trim();

            // LEGACY: Conditional dynamic grouping based on customAggregation and selectedHeader (backward compatibility)
            String customAggregation = jobContext.getParameter("CUSTOM_AGGREGATION");
            customAggregation = customAggregation == null ? "false" : customAggregation;
            String groupCenterKeys = jobContext.getParameter("GROUP_CENTER_KEYS");
            groupCenterKeys = groupCenterKeys == null ? "" : groupCenterKeys.trim();

            Dataset<Row> groupedOrJoined = joinedDF;
            
            // NEW LOGIC: Use nemetaColumnsAggregation when selectedHeader='custom'
            boolean useNemetaAggregation = selectedHeaderEff.equalsIgnoreCase("custom") 
                    && nemetaColumnsAggregation.equalsIgnoreCase("true") 
                    && !nemetaGroupColumns.isEmpty();
            
            // LEGACY LOGIC: Use customAggregation when selectedHeader != 'default' (backward compatibility)
            boolean useLegacyAggregation = !selectedHeaderEff.equalsIgnoreCase("default") 
                    && customAggregation.equalsIgnoreCase("true") 
                    && !groupCenterKeys.isEmpty();
            
            if (useNemetaAggregation) {
                // Apply nemeta value filters before grouping, e.g., M10='ONAIR'
                String nemetaFilters = jobContext.getParameter("NEMETA_GROUP_FILTERS");
                if (nemetaFilters != null && !nemetaFilters.trim().isEmpty()) {
                    List<String> filters = Arrays.stream(nemetaFilters.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
                    for (String f : filters) {
                        int eqIdx = f.indexOf('=');
                        if (eqIdx > 0) {
                            String key = f.substring(0, eqIdx).trim();
                            String val = f.substring(eqIdx + 1).trim();
                            if (val.startsWith("'") && val.endsWith("'")) {
                                val = val.substring(1, val.length() - 1);
                            }
                            if (!key.isEmpty() && Arrays.asList(joinedDF.columns()).contains(key)) {
                                joinedDF = joinedDF.filter(col(key).equalTo(lit(val)));
                            }
                        }
                    }
                }
                // NEW: Use nemeta_groupColumns for grouping
                List<String> groupKeys = Arrays.stream(nemetaGroupColumns.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());

                List<String> availCols = Arrays.asList(joinedDF.columns());
                if (IS_LOG_ENABLED) {
                    logger.info("Available columns before grouping: {}", availCols);
                }
                List<String> actualGroupCols = groupKeys.stream()
                        .filter(availCols::contains)
                        .collect(Collectors.toList());

                // Always also group by visible meta columns present in the output to avoid
                // collapsing time/geo
                List<String> baseGroupCols = new ArrayList<>();
                for (String c : new String[] { "DT", "HR", "Country", "DL1", "DL2", "DL3", "DL4" }) {
                    if (availCols.contains(c)) {
                        baseGroupCols.add(c);
                    }
                }
                // Include ALL M# columns as groupBy columns automatically
                List<String> allMCols = new ArrayList<>();
                for (String c : availCols) {
                    if (c != null && c.length() > 1) {
                        char c0 = c.charAt(0);
                        if ((c0 == 'M' || c0 == 'm')) {
                            boolean digits = true;
                            for (int i = 1; i < c.length(); i++) {
                                if (!Character.isDigit(c.charAt(i))) {
                                    digits = false;
                                    break;
                                }
                            }
                            if (digits) {
                                allMCols.add(c);
                            }
                        }
                    }
                }
                // Build final group columns with order preserved and WITHOUT duplicates
                java.util.LinkedHashSet<String> finalGroupColsSet = new java.util.LinkedHashSet<>();
                finalGroupColsSet.addAll(baseGroupCols);
                finalGroupColsSet.addAll(actualGroupCols);
                finalGroupColsSet.addAll(allMCols);
                List<String> finalGroupCols = new ArrayList<>(finalGroupColsSet);
                if (IS_LOG_ENABLED) {
                    logger.info("M# columns found for grouping: {}", allMCols);
                    logger.info("Final groupBy columns: {}", finalGroupCols);
                }

                if (!actualGroupCols.isEmpty()) {
                    List<String> kpiIds = new ArrayList<>();
                    List<String> counterIds = new ArrayList<>();
                    String kpiCsv = jobContext.getParameter("KPI_CODE_COMMA_SEPARATED");
                    String counterCsv = jobContext.getParameter("COUNTER_ID_COMMA_SEPARATED");
                    if (kpiCsv != null) {
                        for (String k : kpiCsv.split(",")) {
                            if (!k.trim().isEmpty())
                                kpiIds.add(k.trim());
                        }
                    }
                    if (counterCsv != null) {
                        for (String c : counterCsv.split(",")) {
                            if (!c.trim().isEmpty())
                                counterIds.add(c.trim());
                        }
                    }

                    List<String> metricCols = new ArrayList<>();
                    metricCols.addAll(kpiIds);
                    metricCols.addAll(counterIds);

                    // If there are no metrics to aggregate, skip grouping entirely
                    if (metricCols.isEmpty()) {
                        groupedOrJoined = joinedDF;
                    } else {

                        List<String> metaCandidates = new ArrayList<>(availCols);
                        metaCandidates.removeAll(metricCols);

                        // Build aggregations: first() for meta (non-group keys); avg(Double) for
                        // metrics
                        List<org.apache.spark.sql.Column> aggExprs = new ArrayList<>();
                        for (String mc : metaCandidates) {
                            // Never aggregate groupBy columns and never aggregate any M# columns
                            boolean isGroupCol = finalGroupCols.contains(mc);
                            boolean isMColumn = (mc.length() > 1 && (mc.charAt(0) == 'M' || mc.charAt(0) == 'm'));
                            if (!isGroupCol && !isMColumn) {
                                aggExprs.add(first(col(mc), true).alias(mc));
                            }
                        }
                        for (String m : metricCols) {
                            if (availCols.contains(m)) {
                                aggExprs.add(avg(col(m).cast(DataTypes.DoubleType)).alias(m));
                            }
                        }

                        org.apache.spark.sql.RelationalGroupedDataset rgd = joinedDF.groupBy(
                                finalGroupCols.stream().map(org.apache.spark.sql.functions::col)
                                        .toArray(org.apache.spark.sql.Column[]::new));
                        Dataset<Row> aggregatedDF;
                        if (aggExprs.isEmpty()) {
                            // Should not happen when metricCols non-empty; safe fallback to count
                            aggregatedDF = rgd.count().drop("count");
                        } else {
                            aggregatedDF = rgd.agg(aggExprs.get(0),
                                    aggExprs.subList(1, aggExprs.size()).toArray(new org.apache.spark.sql.Column[0]));

                            // Explicitly include groupBy columns in the aggregated output selection
                            // The aggregated result already contains: groupBy columns + aggregated columns
                            // We just need to ensure proper column order: groupBy cols first, then aggregated cols
                            java.util.LinkedHashSet<String> selectNames = new java.util.LinkedHashSet<>();
                            selectNames.addAll(finalGroupCols);
                            // Add non-group meta columns that were aggregated
                            for (String mc : metaCandidates) {
                                boolean isGroupCol = finalGroupCols.contains(mc);
                                boolean isMColumn = (mc.length() > 1 && (mc.charAt(0) == 'M' || mc.charAt(0) == 'm'));
                                if (!isGroupCol && !isMColumn) {
                                    selectNames.add(mc);
                                }
                            }
                            selectNames.addAll(metricCols);

                            final Dataset<Row> finalAggregatedDF = aggregatedDF;
                            java.util.List<org.apache.spark.sql.Column> selectCols = selectNames.stream()
                                    .filter(name -> java.util.Arrays.asList(finalAggregatedDF.columns()).contains(name))
                                    .map(org.apache.spark.sql.functions::col)
                                    .collect(java.util.stream.Collectors.toList());
                            if (!selectCols.isEmpty()) {
                                aggregatedDF = aggregatedDF.select(selectCols.toArray(new org.apache.spark.sql.Column[0]));
                            }
                        }
                        groupedOrJoined = aggregatedDF;
                    }
                }
            } else if (useLegacyAggregation) {
                // LEGACY: Use group_center for grouping (backward compatibility)
                List<String> groupKeys = Arrays.stream(groupCenterKeys.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());

                List<String> availCols = Arrays.asList(joinedDF.columns());
                if (IS_LOG_ENABLED) {
                    logger.info("Available columns before grouping (LEGACY): {}", availCols);
                }
                List<String> actualGroupCols = groupKeys.stream()
                        .filter(availCols::contains)
                        .collect(Collectors.toList());

                // Always also group by visible meta columns present in the output to avoid
                // collapsing time/geo
                List<String> baseGroupCols = new ArrayList<>();
                for (String c : new String[] { "DT", "HR", "Country", "DL1", "DL2", "DL3", "DL4" }) {
                    if (availCols.contains(c)) {
                        baseGroupCols.add(c);
                    }
                }
                // Include ALL M# columns as groupBy columns automatically
                List<String> allMCols = new ArrayList<>();
                for (String c : availCols) {
                    if (c != null && c.length() > 1) {
                        char c0 = c.charAt(0);
                        if ((c0 == 'M' || c0 == 'm')) {
                            boolean digits = true;
                            for (int i = 1; i < c.length(); i++) {
                                if (!Character.isDigit(c.charAt(i))) {
                                    digits = false;
                                    break;
                                }
                            }
                            if (digits) {
                                allMCols.add(c);
                            }
                        }
                    }
                }
                // Build final group columns with order preserved and WITHOUT duplicates
                java.util.LinkedHashSet<String> finalGroupColsSet = new java.util.LinkedHashSet<>();
                finalGroupColsSet.addAll(baseGroupCols);
                finalGroupColsSet.addAll(actualGroupCols);
                finalGroupColsSet.addAll(allMCols);
                List<String> finalGroupCols = new ArrayList<>(finalGroupColsSet);
                if (IS_LOG_ENABLED) {
                    logger.info("M# columns found for grouping (LEGACY): {}", allMCols);
                    logger.info("Final groupBy columns (LEGACY): {}", finalGroupCols);
                }

                if (!actualGroupCols.isEmpty()) {
                    List<String> kpiIds = new ArrayList<>();
                    List<String> counterIds = new ArrayList<>();
                    String kpiCsv = jobContext.getParameter("KPI_CODE_COMMA_SEPARATED");
                    String counterCsv = jobContext.getParameter("COUNTER_ID_COMMA_SEPARATED");
                    if (kpiCsv != null) {
                        for (String k : kpiCsv.split(",")) {
                            if (!k.trim().isEmpty())
                                kpiIds.add(k.trim());
                        }
                    }
                    if (counterCsv != null) {
                        for (String c : counterCsv.split(",")) {
                            if (!c.trim().isEmpty())
                                counterIds.add(c.trim());
                        }
                    }

                    List<String> metricCols = new ArrayList<>();
                    metricCols.addAll(kpiIds);
                    metricCols.addAll(counterIds);

                    // If there are no metrics to aggregate, skip grouping entirely
                    if (metricCols.isEmpty()) {
                        groupedOrJoined = joinedDF;
                    } else {

                        List<String> metaCandidates = new ArrayList<>(availCols);
                        metaCandidates.removeAll(metricCols);

                        // Build aggregations: first() for meta (non-group keys); avg(Double) for
                        // metrics
                        List<org.apache.spark.sql.Column> aggExprs = new ArrayList<>();
                        for (String mc : metaCandidates) {
                            // Never aggregate groupBy columns and never aggregate any M# columns
                            boolean isGroupCol = finalGroupCols.contains(mc);
                            boolean isMColumn = (mc.length() > 1 && (mc.charAt(0) == 'M' || mc.charAt(0) == 'm'));
                            if (!isGroupCol && !isMColumn) {
                                aggExprs.add(first(col(mc), true).alias(mc));
                            }
                        }
                        for (String m : metricCols) {
                            if (availCols.contains(m)) {
                                aggExprs.add(avg(col(m).cast(DataTypes.DoubleType)).alias(m));
                            }
                        }

                        org.apache.spark.sql.RelationalGroupedDataset rgd = joinedDF.groupBy(
                                finalGroupCols.stream().map(org.apache.spark.sql.functions::col)
                                        .toArray(org.apache.spark.sql.Column[]::new));
                        Dataset<Row> aggregatedDF;
                        if (aggExprs.isEmpty()) {
                            // Should not happen when metricCols non-empty; safe fallback to count
                            aggregatedDF = rgd.count().drop("count");
                        } else {
                            aggregatedDF = rgd.agg(aggExprs.get(0),
                                    aggExprs.subList(1, aggExprs.size()).toArray(new org.apache.spark.sql.Column[0]));

                            // Explicitly include groupBy columns in the aggregated output selection
                            // The aggregated result already contains: groupBy columns + aggregated columns
                            // We just need to ensure proper column order: groupBy cols first, then aggregated cols
                            java.util.LinkedHashSet<String> selectNames = new java.util.LinkedHashSet<>();
                            selectNames.addAll(finalGroupCols);
                            // Add non-group meta columns that were aggregated
                            for (String mc : metaCandidates) {
                                boolean isGroupCol = finalGroupCols.contains(mc);
                                boolean isMColumn = (mc.length() > 1 && (mc.charAt(0) == 'M' || mc.charAt(0) == 'm'));
                                if (!isGroupCol && !isMColumn) {
                                    selectNames.add(mc);
                                }
                            }
                            selectNames.addAll(metricCols);

                            final Dataset<Row> finalAggregatedDF = aggregatedDF;
                            java.util.List<org.apache.spark.sql.Column> selectCols = selectNames.stream()
                                    .filter(name -> java.util.Arrays.asList(finalAggregatedDF.columns()).contains(name))
                                    .map(org.apache.spark.sql.functions::col)
                                    .collect(java.util.stream.Collectors.toList());
                            if (!selectCols.isEmpty()) {
                                aggregatedDF = aggregatedDF.select(selectCols.toArray(new org.apache.spark.sql.Column[0]));
                            }
                        }
                        groupedOrJoined = aggregatedDF;
                    }
                }
            }

            Dataset<Row> finalDF = generateFinalDataDF(groupedOrJoined, jobContext);

            String frequency = jobContext.getParameter("FREQUENCY");

            if (frequency.equalsIgnoreCase("DAILY") || frequency.equalsIgnoreCase("PERDAY")) {
                finalDF = finalDF.drop("TIME");
            }

            if (frequency.equalsIgnoreCase("WEEKLY") || frequency.equalsIgnoreCase("PERWEEK")) {
                finalDF = finalDF
                        .withColumnRenamed("DATE", "WEEK OF YEAR")
                        .withColumnRenamed("TIME", "WEEK RANGE");
            }

            if (frequency.equalsIgnoreCase("MONTHLY") || frequency.equalsIgnoreCase("PERMONTH")) {
                finalDF = finalDF.withColumnRenamed("DATE", "MONTH OF YEAR");
                finalDF = finalDF.drop("TIME");
            }

            return finalDF;
        } catch (Exception e) {
            logger.error("Error In Generating Report DataFrame, Message: {}, Error: {}", e.getMessage(), e);
            return createEmptyDF(jobContext);
        }
    }

    private static Dataset<Row> createEmptyDF(JobContext jobContext) {

        String metaColumns = jobContext.getParameter("META_COLUMNS");
        String kpiCodeWithKpiNameMapJson = jobContext.getParameter("KPI_CODE_WITH_KPI_NAME_MAP");
        String counterIdWithCounterNameMapJson = jobContext.getParameter("COUNTER_ID_WITH_COUNTER_NAME_MAP");

        Map<String, String> metaColumnsMap = new LinkedHashMap<>();
        Map<String, String> kpiCodeWithKpiNameMap = new LinkedHashMap<>();
        Map<String, String> counterIdWithCounterNameMap = new LinkedHashMap<>();

        try {
            ObjectMapper mapper = new ObjectMapper();

            metaColumnsMap = mapper.readValue(metaColumns, new TypeReference<Map<String, String>>() {
            });
            kpiCodeWithKpiNameMap = mapper.readValue(kpiCodeWithKpiNameMapJson,
                    new TypeReference<Map<String, String>>() {
                    });
            counterIdWithCounterNameMap = mapper.readValue(counterIdWithCounterNameMapJson,
                    new TypeReference<Map<String, String>>() {
                    });

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (IS_LOG_ENABLED) {
            logger.info("Meta Columns Map: {}", metaColumnsMap);
            logger.info("KPI Code With KPI Name Map: {}", kpiCodeWithKpiNameMap);
            logger.info("Counter Id With Counter Name Map: {}", counterIdWithCounterNameMap);
        }

        List<String> allColumns = new ArrayList<>();
        allColumns.addAll(metaColumnsMap.values()); // e.g. DATE, TIME, ...
        allColumns.addAll(kpiCodeWithKpiNameMap.values()); // e.g. 1066-OUTBOUND TRAFFIC (MB), ...
        allColumns.addAll(counterIdWithCounterNameMap.values()); // e.g. 29396-INTERFACE.OUTBOUND_TRAFFIC...

        List<StructField> fields = allColumns.stream()
                .map(colName -> DataTypes.createStructField(colName, DataTypes.StringType, true))
                .collect(Collectors.toList());

        StructType schema = DataTypes.createStructType(fields);
        return jobContext.sqlctx().createDataFrame(new ArrayList<Row>(), schema);
    }

    private static Dataset<Row> getDataFrameFromQuery(String query, JobContext jobContext) {

        Map<String, String> jobContextMap = jobContext.getParameters();

        String FALLBACK_SPARK_PM_JDBC_DRIVER = "org.mariadb.jdbc.Driver";
        String FALLBACK_SPARK_PM_JDBC_URL = "jdbc:mysql://mysql-nst-cluster.nstdb.svc.cluster.local:6446/PERFORMANCE?autoReconnect=true";
        String FALLBACK_SPARK_PM_JDBC_USERNAME = "PERFORMANCE";
        String FALLBACK_SPARK_PM_JDBC_PASSWORD = "perform!123";

        Dataset<Row> dataFrame = null;
        try {
            String SPARK_PM_JDBC_DRIVER = jobContextMap.get("SPARK_PM_JDBC_DRIVER");
            String SPARK_PM_JDBC_URL = jobContextMap.get("SPARK_PM_JDBC_URL");
            String SPARK_PM_JDBC_USERNAME = jobContextMap.get("SPARK_PM_JDBC_USERNAME");
            String SPARK_PM_JDBC_PASSWORD = jobContextMap.get("SPARK_PM_JDBC_PASSWORD");

            SPARK_PM_JDBC_DRIVER = SPARK_PM_JDBC_DRIVER != null && !SPARK_PM_JDBC_DRIVER.isEmpty()
                    ? SPARK_PM_JDBC_DRIVER
                    : FALLBACK_SPARK_PM_JDBC_DRIVER;
            SPARK_PM_JDBC_URL = SPARK_PM_JDBC_URL != null && !SPARK_PM_JDBC_URL.isEmpty() ? SPARK_PM_JDBC_URL
                    : FALLBACK_SPARK_PM_JDBC_URL;
            SPARK_PM_JDBC_USERNAME = SPARK_PM_JDBC_USERNAME != null && !SPARK_PM_JDBC_USERNAME.isEmpty()
                    ? SPARK_PM_JDBC_USERNAME
                    : FALLBACK_SPARK_PM_JDBC_USERNAME;
            SPARK_PM_JDBC_PASSWORD = SPARK_PM_JDBC_PASSWORD != null && !SPARK_PM_JDBC_PASSWORD.isEmpty()
                    ? SPARK_PM_JDBC_PASSWORD
                    : FALLBACK_SPARK_PM_JDBC_PASSWORD;

            dataFrame = jobContext.sqlctx().read()
                    .format("jdbc")
                    .option("driver", SPARK_PM_JDBC_DRIVER)
                    .option("url", SPARK_PM_JDBC_URL)
                    .option("user", SPARK_PM_JDBC_USERNAME)
                    .option("password", SPARK_PM_JDBC_PASSWORD)
                    .option("query", query)
                    .load();

        } catch (Exception e) {
            logger.error("Error in Getting DataFrame From Query, Message: {}, Error: {}", e.getMessage(), e);
        }
        return dataFrame;
    }

    private Dataset<Row> generateFinalDataDF(Dataset<Row> inputDF, JobContext jobContext) {
        try {
            ObjectMapper mapper = new ObjectMapper();

            Map<String, String> metaColumnsMap = mapper.readValue(
                    jobContext.getParameter("META_COLUMNS"), new TypeReference<>() {
                    });
            Map<String, String> kpiMap = mapper.readValue(
                    jobContext.getParameter("KPI_CODE_WITH_KPI_NAME_MAP"), new TypeReference<>() {
                    });
            Map<String, String> counterMap = mapper.readValue(
                    jobContext.getParameter("COUNTER_ID_WITH_COUNTER_NAME_MAP"), new TypeReference<>() {
                    });

            inputDF.createOrReplaceTempView("InputData");
            
            if (IS_LOG_ENABLED) {
                logger.info("InputData columns before final SELECT: {}", Arrays.asList(inputDF.columns()));
                logger.info("metaColumnsMap: {}", metaColumnsMap);
            }

            StringBuilder sql = new StringBuilder("SELECT ");
            Consumer<Map<String, String>> appendCols = (map) -> {
                map.forEach((key, alias) -> {
                    if (key.matches("\\d+")) {
                        sql.append("`").append(key).append("` AS `").append(alias).append("`, ");
                    } else if (key != null && key.length() > 1 && (key.charAt(0) == 'M' || key.charAt(0) == 'm')) {
                        // M# columns need backticks too
                        sql.append("`").append(key).append("` AS `").append(alias).append("`, ");
                    } else {
                        sql.append(key).append(" AS `").append(alias).append("`, ");
                    }
                });
            };

            appendCols.accept(metaColumnsMap);
            appendCols.accept(kpiMap);
            appendCols.accept(counterMap);
            if (sql.length() >= 2) {
                sql.setLength(sql.length() - 2);
            }

            String finalSql = sql.toString() + " FROM InputData";
            if (IS_LOG_ENABLED) {
                logger.info("Final SQL: {}", finalSql);
            }

            return inputDF.sparkSession().sql(finalSql);

        } catch (Exception e) {
            logger.error("Error Generating Final DataFrame Using SQL", e);
            return createEmptyDF(jobContext);
        }
    }

    private String generateDynamicQuery(Map<String, String> metaColumnsMap, JobContext jobContext) {

        List<String> metaColumnKeys = new ArrayList<>(
                metaColumnsMap.keySet().stream().map(String::toUpperCase).collect(Collectors.toList()));
        if (IS_LOG_ENABLED) {
            logger.info("Meta Column Keys: {}", metaColumnKeys);
        }

        List<String> defaultMetaColumnKeys = Arrays.asList("DT", "HR", "COUNTRY", "DL1", "DL2", "DL3", "DL4",
                "NODENAME");
        if (IS_LOG_ENABLED) {
            logger.info("Default Meta Column Keys: {}", defaultMetaColumnKeys);
        }

        List<String> enrichFromNEKeys = metaColumnKeys.stream()
                .filter(col -> !defaultMetaColumnKeys.contains(col))
                .collect(Collectors.toList());

        String reportLevel = jobContext.getParameter("AGGREGATION_LEVEL");
        if (IS_LOG_ENABLED) {
            logger.info("Fields to Enrich from NE: {}", enrichFromNEKeys);
            logger.info("Report Level: {}", reportLevel);
        }

        String dynamicQuery = "SELECT ";

        switch (reportLevel) {
            case "L0": {

                if (jobContext.getParameter("CUSTOM_INDIA_LEVEL").equalsIgnoreCase("TRUE")) {

                    String nodename = jobContext.getParameter("CUSTOM_OR_POLYGON_NAME");
                    nodename = nodename == null ? "Custom" : nodename;
                    dynamicQuery += "DATE AS DT, TIME AS HR, '-' AS COUNTRY, '-' AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, '$nodename' AS NODENAME";
                    dynamicQuery = dynamicQuery.replace("$nodename", nodename);
                    break;

                } else {
                    dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, '-' AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, 'INDIA' AS NODENAME";
                    break;
                }
            }
            case "L1":
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, '-' AS DL2, '-' AS DL3, '-' AS DL4, ENTITY_NAME AS nodename";
                break;
            case "L2":
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, UPPER(L2) AS DL2, '-' AS DL3, '-' AS DL4, ENTITY_NAME AS nodename";
                break;
            case "L3":
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, UPPER(L2) AS DL2, UPPER(L3) AS DL3, '-' AS DL4, ENTITY_NAME AS nodename";
                break;
            case "L4":
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, UPPER(L2) AS DL2, UPPER(L3) AS DL3, UPPER(L4) AS DL4, ENTITY_NAME AS nodename";
                break;
            case "H1":
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, UPPER(L2) AS DL2, UPPER(L3) AS DL3, UPPER(L4) AS DL4, PARENT_ENTITY_ID AS ENTITY_ID, PARENT_ENTITY_NAME AS ENTITY_NAME, ENTITY_NAME AS nodename";
                break;
            case "NAM":
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, UPPER(L2) AS DL2, UPPER(L3) AS DL3, UPPER(L4) AS DL4, ENTITY_ID AS ENTITY_ID, ENTITY_NAME AS ENTITY_NAME, PARENT_ENTITY_ID AS PARENT_ENTITY_ID, PARENT_ENTITY_NAME AS PARENT_ENTITY_NAME, ENTITY_NAME AS nodename";
                break;
            default:
                dynamicQuery += "DATE AS DT, TIME AS HR, 'INDIA' AS COUNTRY, UPPER(L1) AS DL1, UPPER(L2) AS DL2, UPPER(L3) AS DL3, UPPER(L4) AS DL4, UPPER(ENTITY_NAME) AS nodename";
                break;
        }

        String kpiCodeCommaSeparated = jobContext.getParameter("KPI_CODE_COMMA_SEPARATED");
        String counterIdCommaSeparated = jobContext.getParameter("COUNTER_ID_COMMA_SEPARATED");
        String[] kpiCodeArray = kpiCodeCommaSeparated.split(",");
        String[] counterIdArray = counterIdCommaSeparated.split(",");
        for (String kpiCode : kpiCodeArray) {
            if (!kpiCode.trim().equalsIgnoreCase("")) {
                dynamicQuery += ", `" + kpiCode.trim() + "`";
            }
        }
        for (String counterId : counterIdArray) {
            if (!counterId.trim().equalsIgnoreCase("")) {
                dynamicQuery += ", `" + counterId.trim() + "`";
            }
        }

        // Also project dynamic NE META M# columns so they are available for grouping
        // and final selection
        for (String key : metaColumnsMap.keySet()) {
            if (key != null && key.length() > 1) {
                char c0 = key.charAt(0);
                boolean looksLikeM = (c0 == 'M' || c0 == 'm');
                boolean restDigits = true;
                for (int i = 1; i < key.length(); i++) {
                    if (!Character.isDigit(key.charAt(i))) {
                        restDigits = false;
                        break;
                    }
                }
                if (looksLikeM && restDigits) {
                    dynamicQuery += ", `" + key + "`";
                }
            }
        }

        dynamicQuery += " FROM InputData";

        return dynamicQuery;
    }
}
