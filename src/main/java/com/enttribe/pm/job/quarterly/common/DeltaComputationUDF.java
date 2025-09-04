package com.enttribe.pm.job.quarterly.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enttribe.commons.lang.StringUtils;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

import scala.jdk.CollectionConverters;

public class DeltaComputationUDF implements
        UDF4<String, String, scala.collection.immutable.Map<String, String>, scala.collection.immutable.Map<String, String>, List<Row>>,
        AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(DeltaComputationUDF.class);

    private static Map<String, List<String>> categoryVSDetla = null;

    public JobContext jobContext;
    private static Object SYNCHRONIZER = new Object();

    @Override
    public List<Row> call(String PT, String mapKey, scala.collection.immutable.Map<String, String> counterValueMap,
            scala.collection.immutable.Map<String, String> deltaCounterValueMap) throws Exception {
        getCategoryVsDeltaCounterMap();
        Map<String, Map<String, String>> enbIdCellMap = new HashMap<>();
        List<Row> fileContent = new ArrayList<>();
        String nodeCategory = StringUtils.substringBefore(mapKey, "##");
        // logger.info("nodeCategory is :{}", nodeCategory);
        List<String> counterList = categoryVSDetla.get(nodeCategory);
        // logger.info("counterList is :{}", counterList);
        // Map<String, String> counterValueJMap = new HashMap<>(
        // scala.collection.JavaConversions.mapAsJavaMap(counterValueMap));
        Map<String, String> counterValueJMap = new HashMap<>(
                CollectionConverters.MapHasAsJava(counterValueMap).asJava());
        // logger.info("counterValueJMap is :{}", counterValueJMap);
        if (deltaCounterValueMap != null) {

            // Map<String, String> deltaCounterValueJMap =
            // scala.collection.JavaConversions.mapAsJavaMap(deltaCounterValueMap);
            // logger.info("deltaCounterValueMap is :{}", deltaCounterValueMap);
            Map<String, String> deltaCounterValueJMap = new HashMap<>(
                    CollectionConverters.MapHasAsJava(deltaCounterValueMap)
                            .asJava());
            // logger.info("deltaCounterValueJMap is :{}", deltaCounterValueJMap);
            if (counterList != null)
                counterList.forEach(e -> {
                    String prevValue = deltaCounterValueJMap.get(e);
                    if (prevValue != null) {
                        String currValue = counterValueJMap.get(e);
                        if (currValue != null) {
                            try {
                                Double diff = Double.valueOf(currValue) - Double.valueOf(prevValue);
                                // System.out.println(mapKey + "==>counterValueJMap.get(" + e + ")==>" + currValue + "-"
                                //         + prevValue + "=" + diff);
                                if (diff >= 0) {
                                    counterValueJMap.put(e, String.valueOf(diff));
                                } else {
                                    counterValueJMap.remove(e);
                                }
                            } catch (NumberFormatException e1) {
                                logger.error("Exception While Setting Data In fileContent of Call {},{}",
                                        e1.getMessage(),
                                        mapKey);
                            }

                        } else
                            counterValueJMap.remove(e);
                    } else
                        counterValueJMap.remove(e);
                });
        } else {
            if (counterList != null) {
                counterList.forEach(e -> counterValueJMap.remove(e));
            }
        }
        enbIdCellMap.put(mapKey, counterValueJMap);
        fileContent.add(RowFactory.create(PT, enbIdCellMap));

        // try {
        // Map<String, Map<String, String>> enbIdCellMap = new HashMap<>();
        //
        // if (deltaCounterValueMap != null) {
        //
        // deltaCounterValueJMap.forEach((k, v) -> {
        // try {
        // String currentVal = counterValueJMap.get(k);
        // if (currentVal != null && v != null) {
        // Double diff = Double.valueOf(currentVal) - Double.valueOf(v);
        // System.out.println(mapKey + "==>counterValueJMap.get(" + k + ")==>" +
        // currentVal + "-" + v+ "=" + diff);
        // if (diff >= 0) {
        // counterValueJMap.put(k, String.valueOf(diff));
        // } else {
        // counterValueJMap.remove(k);
        // }
        // }
        // } catch (NumberFormatException e) {
        // logger.error("Exception While Setting Data In fileContent of Call {},{}",
        // e.getMessage(), mapKey);
        // }
        // });
        // }
        // else {
        // getCategoryVsDeltaCounterMap();
        //
        //
        // if(counterList!=null)
        // counterList.forEach(e -> counterValueJMap.remove(e));
        //
        // }
        // enbIdCellMap.put(mapKey, counterValueJMap);
        // fileContent.add(RowFactory.create(PT, enbIdCellMap));
        // } catch (Exception e) {
        // logger.error("Exception While Setting Data In fileContent of Call {},{}",
        // Utils.getStackTrace(e), mapKey);
        // }
        return fileContent;
    }

    private void getCategoryVsDeltaCounterMap() {
        if (categoryVSDetla == null) {
            synchronized (SYNCHRONIZER) {
                if (categoryVSDetla == null) {
                    // logger.info("Inside categoryVSDetla ");
                    String parameter = jobContext.getParameter("CATEGORY_VS_DELTACOUNTER");
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        categoryVSDetla = mapper.readValue(parameter, new TypeReference<Map<String, List<String>>>() {
                        });
                    } catch (Exception e) {
                        logger.error("Exception occured in method @getCounterVariableIndexMap in class @Ericsson- {}",
                                ExceptionUtils.getStackTrace(e));
                    }
                    // logger.info("counterVariableIndexMap Size - {}", categoryVSDetla.size());
                }
            }
        }
    }

    @Override
    public String getName() {
        return "DeltaComputationUDF";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("processingTime", DataTypes.StringType, true));
        fields.add(
                DataTypes.createStructField("parsedCounterMap",
                        DataTypes.createMapType(DataTypes.StringType,
                                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true),
                        true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

}