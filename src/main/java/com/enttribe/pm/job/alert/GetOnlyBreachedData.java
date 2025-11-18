package com.enttribe.pm.job.alert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetOnlyBreachedData extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(GetOnlyBreachedData.class);

    public GetOnlyBreachedData() {
        super();
        logger.info("GetOnlyBreachedData No Argument Constructor Called!");
    }

    public GetOnlyBreachedData(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        logger.info("GetOnlyBreachedData Constructor Called with Input DataFrame With ID: {} and Processor Name: {}",
                id,
                processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("GetOnlyBreachedData Execution Started!");

        Dataset<Row> inputDF = this.dataFrame;
        if (inputDF == null || inputDF.isEmpty()) {
            logger.info("Input DataFrame is Empty! Returning Empty DataFrame!");
            return inputDF;
        }

        List<Row> rows = inputDF.collectAsList();
        if (rows.isEmpty()) {
            logger.info("Rows are Empty! Returning Empty DataFrame!");
            return inputDF;
        } else {
            logger.info("Input DataFrame Row Size : {}", rows.size());
        }

        String kpiCodes = jobContext.getParameter("KPI_CODES");
        String counterIds = jobContext.getParameter("COUNTER_IDS");

        String[] kpiCodesArray = kpiCodes.split(",");
        String[] counterIdsArray = counterIds.split(",");

        String currentIndex = jobContext.getParameter("START_INDEX");
        logger.info("GetOnlyBreachedData START_INDEX: {}", currentIndex);

        String configurationMapJson = jobContext.getParameter("CONFIGURATION_MAP" + currentIndex);

        Map<String, String> configurationMap = new ObjectMapper().readValue(configurationMapJson,
                new TypeReference<Map<String, String>>() {
                });

        logger.info("Configuration Map {} : {}", currentIndex, configurationMap);

        Dataset<Row> thresholdBreachDataset = processEachConfiguration(currentIndex, rows, configurationMap,
                kpiCodesArray, counterIdsArray, jobContext);

        thresholdBreachDataset.show(5);
        logger.info("GetOnlyBreachedData Execution Completed!");

        return thresholdBreachDataset;

    }

    private static Dataset<Row> processEachConfiguration(String currentIndex, List<Row> rows,
            Map<String, String> configurationMap,
            String[] kpiCodesArray, String[] counterIdsArray,
            JobContext jobContext) {

        List<Row> thresholdBreachRows = new ArrayList<>();

        for (Row row : rows) {
            try {
                Map<String, String> metaDataMap = getMetaDataMap(row);
                logger.info("Meta Data Map {} : {}", currentIndex, metaDataMap);
                Map<String, String> kpiCounterMap = getKpiCounterMap(row, kpiCodesArray, counterIdsArray);
                logger.info("KPI Counter Map {} : {}", currentIndex, kpiCounterMap);

                String result = processEachRowForEachConfiguration(kpiCounterMap, configurationMap, jobContext);

                Row breachRow = RowFactory.create(metaDataMap, kpiCounterMap);

                if ("1".equalsIgnoreCase(result)) {
                    thresholdBreachRows.add(breachRow);
                }

            } catch (Exception e) {
                logger.error("Error Processing Configuration [{}] for Row: {}. Skipping this Row.", currentIndex,
                        row, e);
            }
        }

        try {
            return buildThresholdBreachDataset(jobContext, thresholdBreachRows);
        } catch (Exception e) {
            logger.error("Error Building Threshold Breach Dataset for Configuration [{}]", currentIndex, e);
            return null;
        }
    }

    private static String processEachRowForEachConfiguration(
            Map<String, String> kpiCounterMap,
            Map<String, String> configurationMap,
            JobContext jobContext) {

        String expression = configurationMap.get("EXPRESSION");
        logger.info("Expression: {}", expression);

        if (expression == null || expression.isEmpty()) {
            logger.info("Expression is NULL or Empty. Skipping Expression Evaluation.");
            return "0";
        }

        expression = expression.replace("COUNTER#", "KPI#");
        logger.info("Expression after replacing COUNTER# with KPI#: {}", expression);
        expression = replaceKPICounterValueInExpression(expression, kpiCounterMap);

        logger.info("Expression after replacing KPICounterValueInExpression: {}", expression);
        return evaluateExpression(expression);

    }

    public static Dataset<Row> buildThresholdBreachDataset(JobContext jobContext, List<Row> rows) {
        StructType schema = new StructType()
                .add("META_DATA_MAP", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
                .add("KPI_COUNTER_MAP", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        return jobContext.createDataFrame(rows, schema);
    }

    private static String evaluateExpression(String expression) {

        String result = "0";

        try {
            Expression evaluatedExpression = new Expression(expression);
            result = evaluatedExpression.eval();
        } catch (Exception e) {
            result = "0";
        }

        logger.info("Expression={} And Its Result={}", expression, result);

        return result;
    }

    private static String replaceKPICounterValueInExpression(String expression, Map<String, String> kpiCounterMap) {
        StringBuilder optimizedExpression = new StringBuilder(expression);

        if (expression.contains("KPI#")) {
            String[] exp = expression.split("KPI#");
            int i = 0;
            for (String kpi : exp) {
                if (i++ != 0) {
                    int endIndex = kpi.indexOf(')');
                    String kpiId = (endIndex != -1) ? kpi.substring(0, endIndex) : kpi;

                    if (kpiCounterMap.containsKey(kpiId)) {
                        String kpiValue = kpiCounterMap.get(kpiId);
                        String replacement = (kpiValue.equalsIgnoreCase("-")) ? "NULL" : kpiValue;

                        int startIndex = optimizedExpression.indexOf("((" + "KPI#" + kpiId + "))");
                        if (startIndex != -1) {
                            optimizedExpression.replace(startIndex,
                                    startIndex + ("((" + "KPI#" + kpiId + "))").length(), replacement);
                        }
                    }
                }
            }
        }

        return optimizedExpression.toString();
    }

    private static Map<String, String> getKpiCounterMap(Row row, String[] kpiCodesArray, String[] counterIdsArray) {
        Map<String, String> kpiCounterMap = new HashMap<>();

        if (kpiCodesArray != null) {
            for (String kpiCode : kpiCodesArray) {
                try {
                    Object value = (row != null && kpiCode != null) ? row.getAs(kpiCode) : null;
                    kpiCounterMap.put(kpiCode, value != null ? value.toString() : "");
                } catch (Exception e) {
                    // kpiCounterMap.put(kpiCode, "");
                }
            }
        }

        if (counterIdsArray != null) {
            for (String counterId : counterIdsArray) {
                try {
                    Object value = (row != null && counterId != null) ? row.getAs(counterId) : null;
                    kpiCounterMap.put(counterId, value != null ? value.toString() : "");
                } catch (Exception e) {
                    // kpiCounterMap.put(counterId, "");
                }
            }
        }

        return kpiCounterMap;
    }

    private static Map<String, String> getMetaDataMap(Row row) {
        Map<String, String> metaDataMap = new HashMap<>();
        metaDataMap.put("DATE", row.getAs("DATE") != null ? row.getAs("DATE").toString() : "");
        metaDataMap.put("TIME", row.getAs("TIME") != null ? row.getAs("TIME").toString() : "");
        metaDataMap.put("L1", row.getAs("L1") != null ? row.getAs("L1").toString() : "");
        metaDataMap.put("L2", row.getAs("L2") != null ? row.getAs("L2").toString() : "");
        metaDataMap.put("L3", row.getAs("L3") != null ? row.getAs("L3").toString() : "");
        metaDataMap.put("L4", row.getAs("L4") != null ? row.getAs("L4").toString() : "");
        metaDataMap.put("PARENT_ENTITY_ID",
                row.getAs("PARENT_ENTITY_ID") != null ? row.getAs("PARENT_ENTITY_ID").toString() : "");
        metaDataMap.put("PARENT_ENTITY_NAME",
                row.getAs("PARENT_ENTITY_NAME") != null ? row.getAs("PARENT_ENTITY_NAME").toString() : "");
        metaDataMap.put("ENTITY_ID", row.getAs("ENTITY_ID") != null ? row.getAs("ENTITY_ID").toString() : "");
        metaDataMap.put("ENTITY_NAME", row.getAs("ENTITY_NAME") != null ? row.getAs("ENTITY_NAME").toString() : "");
        metaDataMap.put("PARENT_ENTITY_TYPE",
                row.getAs("PARENT_ENTITY_TYPE") != null ? row.getAs("PARENT_ENTITY_TYPE").toString() : "");
        metaDataMap.put("ENTITY_STATUS",
                row.getAs("ENTITY_STATUS") != null ? row.getAs("ENTITY_STATUS").toString() : "");
        return metaDataMap;
    }
}
