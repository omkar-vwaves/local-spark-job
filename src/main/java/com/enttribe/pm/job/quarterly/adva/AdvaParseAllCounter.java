package com.enttribe.pm.job.quarterly.adva;
// package com.enttribe.sparkrunner.custom.pm.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.substring;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.apache.commons.lang3.StringUtils.substringBetween;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.Symbol;
import static com.enttribe.commons.lang.StringUtils.isEmpty;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;
import com.enttribe.sparkrunner.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.jdk.CollectionConverters;

public class AdvaParseAllCounter implements
        UDF2<String, scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>>, List<Row>>,
        AbstractUDF {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(AdvaParseAllCounter.class);
    private JobContext jobcontext;
    private static final String ALL_CATEGORY_COUNTER_MAPJSON = "ALL_CATEGORY_COUNTER_MAPJSON";
    private static final String PMEMSID_VS_NENAME_JSON = "PMEMSID_VS_NENAME_JSON";
    private static final String CATEGORY_COUNTER_MAPJSON = "CATEGORY_COUNTER_MAPJSON";
    private static Map<String, List<Map<String, String>>> allCounterCategoryMap = null;
    private static Map<String, String> neNameVspmemsIdMap = null;
    private static Object SYNCHRONIZER = new Object();

    static long startTime;
    static {
        startTime = System.currentTimeMillis();
    }

    public AdvaParseAllCounter(JobContext jobContext) {
        this.jobcontext = jobContext;
    }

    public AdvaParseAllCounter() {
    }

    @Override
    public List<Row> call(String processingTime,
            scala.collection.immutable.Map<String, scala.collection.immutable.Map<String, String>> parsedCounterMap)
            throws Exception {

        logger.debug("Starting call method in ParseAllCounter with processingTime: {}", processingTime); // 0000

        // logger.debug("Inside class ParseWDMLTEAllCounter processingTime : {} ,
        // parsedCounterMap {} ",processingTime, parsedCounterMap.size());
        List<Row> fileContent = new ArrayList<>();
        getAllCategoryCounterMap();

        logger.debug("Loaded all category counter map successfully.");

        // logger.debug("getting the parsed counter map"+parsedCounterMap.toString());
        getneNameVspmemsId();
        logger.debug("Loaded neName vs PmemsId map successfully.");

        try {
            // java.util.Map<String, scala.collection.immutable.Map<String, String>>
            // rowDataFinalMap = scala.collection.JavaConversions
            // .mapAsJavaMap(parsedCounterMap);

            java.util.Map<String, scala.collection.immutable.Map<String, String>> rowDataFinalMap = CollectionConverters
                    .MapHasAsJava(parsedCounterMap).asJava();

            logger.debug("Successfully converted parsedCounterMap to Java Map. Invoking parseAll method.");

            parseAll(fileContent, rowDataFinalMap, processingTime);

            // INTERFACE_METRICS{##10.100.100.100_GigabitEthernet1/0/7##20241128114001##={ifAdminStatus=1,
            // ifType=6, ifInDiscards=0, ifMtu=1500, ifPhysAddress=00:b0:e1:f6:f8:87,
            // ifOutUcastPkts=0, outbound_errors=0, ifSpeed=10000000, ifInErrors=0,
            // inbound_traffic=0, ifDescr=GigabitEthernet1/0/7, ifLastChange=10397,
            // ifOutDiscards=0, inbound_errors=0, ifInUnknownProtos=0, ifInUcastPkts=0,
            // ifOutOctets=0, outbound_traffic=0, ifInOctets=0, ifOutErrors=0,
            // ifOperStatus=2}}
            logger.debug("Successfully processed parsed counters. Total rows added to fileContent: {}",
                    fileContent.size());

        } catch (Exception e) {
            logger.error("Exception occurred in call method of ParseAllCounter: {}", Utils.getStackTrace(e));

        }
        // logger.debug("inside ParseWDMLTEAllCounter filecontent is {}",fileContent);
        // ================
        long endTime = System.currentTimeMillis();
        logger.debug("Time taken by ParseWDMLTEAllCounter: {} milliseconds", endTime - startTime);
        return fileContent;
    }

    private void parseAll(List<Row> fileContent,
            Map<String, scala.collection.immutable.Map<String, String>> rowDataFinalMap, String processingTime)
            throws IOException {
        try {

            logger.debug("Starting parseAll method with processingTime: {}", processingTime);

            for (String key : rowDataFinalMap.keySet()) {

                logger.debug("Processing key: {}", key);

                // java.util.Map<String, String> counterValueMap =
                // scala.collection.JavaConversions
                // .mapAsJavaMap(rowDataFinalMap.get(key));

                java.util.Map<String, String> counterValueMap = CollectionConverters
                        .MapHasAsJava(rowDataFinalMap.get(key)).asJava();

                logger.debug("Converted counterValueMap size: {}", counterValueMap.size());

                // wdm##11_1_8##20240312000000##11103 ->
                // {DELAY_MEASUREMENT_MAX -> 3087}
                // category+"##"+measObjLdn+"##"+dateTime

                String[] categoryKeyArray = key.split("##");// INTERFACE_METRICS{##10.100.100.100_GigabitEthernet1/0/7##20241128114001##
                String category = categoryKeyArray[0];// wdm
                String measObjLdn = categoryKeyArray[1];// 11_1_8
                String dateTime = categoryKeyArray[2];// 20240312000000

                logger.debug("Parsed Key Components - Category: {}, MeasObjLdn: {}, DateTime: {}",
                        category, measObjLdn, dateTime);

                // Category: ifInOut, MeasObjLdn: SN-SINPL1-BO702

                // PLMN-PLMN/MRBTS-1000100/LNBTS-1000100/LNCEL-110,MCCMNC=PLMN-PLMN/MCC-234/MNC-10
                String measObjLdnForPmemsid = substringAfter(measObjLdn, Symbol.SLASH_FORWARD_STRING);// MRBTS-1000100/LNBTS-1000100/LNCEL-110,MCCMNC=PLMN-PLMN/MCC-234/MNC-10

                logger.debug("Derived measObjLdnForPmemsid: {}", measObjLdnForPmemsid);
                // logger.debug("getting the measObjLdnForPmemsid value map1 and its value " +
                // measObjLdnForPmemsid);
                // logger.debug("getting the measObjLdnForPmemsid value map1 and its value "
                // + substringBefore(measObjLdnForPmemsid, Symbol.SLASH_FORWARD_STRING));
                // String pmemsId = substringBefore(measObjLdnForPmemsid,
                // Symbol.SLASH_FORWARD_STRING)// MRBTS-1000100
                // .split(Symbol.HYPHEN_STRING)[1];// 1000100
                String pmemsId = measObjLdn;
                if (measObjLdnForPmemsid.contains("LNCEL")) {// 1000100_110
                    pmemsId = pmemsId + Symbol.UNDERSCORE_STRING + substringAfterLast(
                            substringBefore(measObjLdnForPmemsid, Symbol.COMMA_STRING), Symbol.HYPHEN_STRING);

                    logger.debug("Adjusted pmemsId for LNCEL: {}", pmemsId);

                }

                logger.debug("Invoking parseAllCounter for Category: {}, PmemsId: {}", category, pmemsId);
                parseAllCounter(fileContent, dateTime, category, pmemsId, processingTime, counterValueMap, measObjLdn);

            }
            logger.debug("Successfully completed parseAll method with fileContent size: {}", fileContent.size());

        } catch (Exception e) {
            logger.error("Exception inside parseAll method @ParseAllCounter: {}", Utils.getStackTrace(e));

        }
    }

    private void parseAllCounter(List<Row> fileContent, String dateTime, String category, String pmemsId,
            String processingTime, Map<String, String> counterValueMap1, String measObjLdn) {
        try {

            logger.debug("Starting parseAllCounter method for category: {}, pmemsId: {}, dateTime: {}",
                    category, pmemsId, dateTime);

            String date = substring(dateTime, 2, 8);
            String time = substring(dateTime, 8, 12);
            String dateKey = substring(dateTime, 0, 8);
            String hourkey = substring(dateTime, 0, 10);
            String quarterKey = substring(dateTime, 0, 12);
            String finalKey = pmemsId + "##" + date + time;

            logger.debug("Generated Keys - Date: {}, Time: {}, DateKey: {}, HourKey: {}, QuarterKey: {}, FinalKey: {}",
                    date, time, dateKey, hourkey, quarterKey, finalKey);

            // ifInOut
            if (allCounterCategoryMap.containsKey(category)) {

                logger.debug("Category '{}' found in allCounterCategoryMap.", category);

                Map<String, Map<String, String>> counterValueMap = getValueMapAllCounters(category, counterValueMap1,
                        measObjLdn);

                logger.debug("CounterValueMap Size for Category '{}': {}", category, counterValueMap.size());

                for (Map.Entry<String, Map<String, String>> entry : counterValueMap.entrySet()) {

                    logger.debug("Processing Entry - Key: {}, Value Size: {}",
                            entry.getKey(), entry.getValue().size());

                    fileContent.add(RowFactory.create(finalKey, date, time, dateKey, hourkey, quarterKey,
                            processingTime, pmemsId, entry.getKey(), entry.getValue(), measObjLdn));
                }

                logger.debug("Successfully parsed and added all counters for category: {}.", category);

            } else {
                logger.debug("Category '{}' not found in allCounterCategoryMap. Skipping processing.", category);
            }

        } catch (Exception e) {
            logger.error("Exception occurred while parsing counters for category: {}, pmemsId: {}, Exception: {}",
                    category, pmemsId, ExceptionUtils.getStackTrace(e));

        }
    }

    private Map<String, Map<String, String>> getValueMapAllCounters(String category,
            Map<String, String> counterValueMap1, String measObjLdn) {
        Map<String, Map<String, String>> categoryVsCounterValueMap = new HashMap<>();
        List<Map<String, String>> list = allCounterCategoryMap.get("CELLAVAILABLETIME");
        if (list != null && category.equalsIgnoreCase("LTE_Cell_Load")) {
            for (Map<String, String> item : list) {
                if (item.get("CounterName").equalsIgnoreCase("Granularity")) {
                    String categoryAliasName = item.get("categoryAliasName");
                    Map<String, String> tempMap = categoryVsCounterValueMap.getOrDefault(categoryAliasName,
                            new HashMap<>());
                    tempMap.put(item.get("sequenceno"), "3600");
                    categoryVsCounterValueMap.put(categoryAliasName, tempMap);
                }
            }
        }
        List<Map<String, String>> derivedColumnList = allCounterCategoryMap.get(category);
        if (derivedColumnList != null) {

            // {ifAdminStatus=1, ifType=6, ifInDiscards=0, ifMtu=1500,
            // ifPhysAddress=00:b0:e1:f6:f8:87, ifOutUcastPkts=0, outbound_errors=0,
            // ifSpeed=10000000, ifInErrors=0, inbound_traffic=0,
            // ifDescr=GigabitEthernet1/0/7, ifLastChange=10397, ifOutDiscards=0,
            // inbound_errors=0, ifInUnknownProtos=0, ifInUcastPkts=0, ifOutOctets=0,
            // outbound_traffic=0, ifInOctets=0, ifOutErrors=0, ifOperStatus=2}
            for (Map<String, String> wrapper : derivedColumnList) {
                String counterheaderName = wrapper.get("CounterName");
                String sequenceNo = wrapper.get("sequenceno");
                String categoryAliasName = wrapper.get("Category");

                logger.debug("getting the categoryname as start" + categoryAliasName);

                logger.debug("getting the wrapper" + wrapper.toString());

                Map<String, String> tempMap = categoryVsCounterValueMap.getOrDefault(categoryAliasName,
                        new HashMap<>());

                logger.debug("getting the counter value map1 as " + counterValueMap1.toString());

                // String value = counterValueMap1.get(counterheaderName.toUpperCase());
                String value = counterValueMap1.get(counterheaderName);

                logger.debug("getting the counter value map1 and its value " + value);

                logger.debug("getting the counter value map1 and its value for counterheadername " + counterheaderName);

                if (value != null) {
                    String column = createCustomizedColumns(value, wrapper);
                    tempMap.put(sequenceNo, column);
                } else if (category.equalsIgnoreCase("LTE_Neighb_Cell_HO")
                        && measObjLdn.contains("HANDOVER_ADJACENT_CELL=PLMN-PLMN/ECI-")
                        && counterheaderName.contains("neighbour")) {
                    String eci = substringBefore(
                            substringAfterLast(measObjLdn, "HANDOVER_ADJACENT_CELL=PLMN-PLMN/ECI-"), ",");
                    int eciNumber = Integer.parseInt(eci);
                    int cellcal = Math.floorMod(eciNumber, 256);
                    int source = (eciNumber - cellcal) / 256;
                    String targetPmemsid = source + "_" + cellcal;
                    if (counterheaderName.equalsIgnoreCase("neighbourid"))
                        tempMap.put(sequenceNo, targetPmemsid);
                    if (counterheaderName.equalsIgnoreCase("neighbourname")
                            && neNameVspmemsIdMap.get(targetPmemsid) != null) {
                        String targetName = neNameVspmemsIdMap.get(targetPmemsid);
                        tempMap.put(sequenceNo, targetName);
                    }
                } else if (measObjLdn.contains(counterheaderName)) {
                    if (measObjLdn.contains(",")) {
                        String[] subCategories = measObjLdn.split(",");
                        for (String subCategory : subCategories) {
                            if (subCategory.contains(counterheaderName)) {
                                String subCateVaue = substringAfter(subCategory, counterheaderName + "=")
                                        .replace("PLMN-PLMN/", "");
                                if (!isEmpty(subCateVaue)) {
                                    tempMap.put(sequenceNo, subCateVaue);
                                }
                            }
                        }
                    } else {
                        String substringBetween = substringBetween(measObjLdn, counterheaderName + "-", "/");
                        String subCateVaue = substringBetween != null
                                ? substringBetween(measObjLdn, counterheaderName + "-", "/")
                                : substringAfterLast(measObjLdn, counterheaderName + "-");
                        if (!isEmpty(subCateVaue)) {
                            tempMap.put(sequenceNo, subCateVaue);
                        }
                    }
                }

                logger.debug("getting the categoryname and counter map" + tempMap.toString());
                categoryVsCounterValueMap.put(categoryAliasName, tempMap);
            }
        }
        return categoryVsCounterValueMap;
    }

    private String createCustomizedColumns(String value, Map<String, String> wrapper) {
        if (equalsIgnoreCase(wrapper.get("NodeAgg"), "COUNT")) {
            value = "1";
        }
        return value;
    }

    private void getneNameVspmemsId() {
        if (neNameVspmemsIdMap == null) {
            synchronized (SYNCHRONIZER) {
                if (neNameVspmemsIdMap == null) {

                    logger.debug("Initializing neNameVspmemsIdMap in method getneNameVspmemsId");

                    ObjectMapper mapper = new ObjectMapper();
                    String json = jobcontext.getParameter(PMEMSID_VS_NENAME_JSON);

                    if (json == null || json.isEmpty()) {
                        logger.debug("No JSON data found in parameter: {}", PMEMSID_VS_NENAME_JSON);
                        return;
                    }

                    logger.debug("JSON data retrieved for PMEMSID_VS_NENAME_JSON: {}", json);

                    try {

                        neNameVspmemsIdMap = mapper.readValue(json, new TypeReference<Map<String, String>>() {
                        });

                        if (neNameVspmemsIdMap != null) {
                            logger.debug("Successfully parsed neNameVspmemsIdMap. Size: {}", neNameVspmemsIdMap.size());
                        } else {
                            logger.debug("Parsed neNameVspmemsIdMap is null after reading JSON.");
                        }

                    } catch (Exception e) {
                        logger.error("Exception occurred while parsing JSON in getneNameVspmemsId", e);

                    }
                }
            }
        } else {
            logger.debug("neNameVspmemsIdMap is Already Initialized.");

        }
    }

    private void getAllCategoryCounterMap() {
        if (allCounterCategoryMap == null) {
            synchronized (SYNCHRONIZER) {
                if (allCounterCategoryMap == null) {

                    logger.debug("Initializing allCounterCategoryMap in method getAllCategoryCounterMap");

                    ObjectMapper mapper = new ObjectMapper();
                    String json = jobcontext.getParameter(ALL_CATEGORY_COUNTER_MAPJSON);

                    if (json == null || json.isEmpty()) {
                        logger.debug("No JSON data found in parameter: {}", ALL_CATEGORY_COUNTER_MAPJSON);
                        return;
                    }

                    logger.debug("JSON data retrieved for ALL_CATEGORY_COUNTER_MAPJSON: {}", json);

                    try {
                        allCounterCategoryMap = mapper.readValue(json,
                                new TypeReference<Map<String, List<Map<String, String>>>>() {
                                });

                        if (allCounterCategoryMap != null) {

                            logger.debug("allCounterCategoryMap : {}", allCounterCategoryMap);

                            logger.debug("Successfully parsed allCounterCategoryMap. Size: {}",
                                    allCounterCategoryMap.size());
                        } else {

                            logger.debug("Parsed allCounterCategoryMap is NULL After Reading JSON.");
                        }

                    } catch (Exception e) {

                        logger.error("Exception occurred while parsing JSON in getAllCategoryCounterMap", e);

                    }

                }
            }
        } else {
            logger.debug("allCounterCategoryMap is Already Initialized.");
        }
    }

    @Override
    public String getName() {
        return "AdvaParseAllCounter";
    }

    @Override
    public DataType getReturnType() {

        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("Date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("Time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("DateKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("HourKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("QuarterKey", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("PT", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("pmemsid", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("categoryName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("rawcounters",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
        fields.add(DataTypes.createStructField("cmdistname", DataTypes.StringType, true));

        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

}