package com.enttribe.fm;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamReader;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.sparkrunner.constants.SparkRunnerConstants;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.streaming.StructureStreaming;
import com.enttribe.sparkrunner.util.Utils;

public class StructuredStreamingKafkaCustom extends StructureStreaming {

    private static final Logger logger = LogManager.getLogger(StructuredStreamingKafkaCustom.class);

    private static final long serialVersionUID = 1L;
    public String tempTable;
    public String brokers;
    public String group = "test";
    public String topics;
    public String options;
    public String batchInterval = "10";
    public String maxOffsetsPerTrigger;
    public String startingOffsets;
    public String sslEnable = "false";
    public String jksLocation = null;
    public String jksPassword = null;
    public String trustStoreType = "";
    public String keyStorType = "";
    public String jtsLocation = "";
    public String jtsPassword = "";
    public String identificationAlgo = "";

    public StructuredStreamingKafkaCustom() {
        super();
    }

    public StructuredStreamingKafkaCustom(int id, String processorName, String tempTable) {
        super(id, processorName);
        this.tempTable = tempTable;
        logger.info("StructuredStreamingKafkaCustom Constructor Called: Temp Table={}", tempTable);
    }

    public StructuredStreamingKafkaCustom(int processorId, String processorName, String brokers, String top,
            String tempTable) {
        super(processorId, processorName);
        this.brokers = brokers;
        this.topics = top;
        this.tempTable = tempTable;
        logger.info("StructuredStreamingKafkaCustom Constructor Called: Temp Table={}, Brokers={}, Topics={}",
                tempTable, brokers, top);

    }

    public StructuredStreamingKafkaCustom(int processorId, String processorName, String brokers, String top,
            String batchInterval, String tempTable) {
        super(processorId, processorName);
        this.brokers = brokers;
        this.topics = top;
        this.batchInterval = batchInterval;
        this.tempTable = tempTable;
        logger.info(
                "StructuredStreamingKafkaCustom Constructor Called: Temp Table={}, Brokers={}, Topics={}, Batch Interval={}",
                tempTable, brokers, top, batchInterval);
    }

    public StructuredStreamingKafkaCustom(int processorId, String processorName, String brokers, String top,
            String batchInterval, String tempTable, String maxOffsetsPerTrigger) {
        super(processorId, processorName);
        this.brokers = brokers;
        this.topics = top;
        this.batchInterval = batchInterval;
        this.tempTable = tempTable;
        this.maxOffsetsPerTrigger = maxOffsetsPerTrigger;
        logger.info(
                "StructuredStreamingKafkaCustom Constructor Called: Temp Table={}, Brokers={}, Topics={}, Batch Interval={}, Max Offsets Per Trigger={}",
                tempTable, brokers, top, batchInterval, maxOffsetsPerTrigger);
    }

    public StructuredStreamingKafkaCustom(int processorId, String processorName, String brokers, String top,
            String batchInterval, String tempTable, String maxOffsetsPerTrigger, String startingOffsets) {
        super(processorId, processorName);
        this.brokers = brokers;
        this.topics = top;
        this.batchInterval = batchInterval;
        this.tempTable = tempTable;
        this.maxOffsetsPerTrigger = maxOffsetsPerTrigger;
        this.startingOffsets = startingOffsets;
        logger.info(
                "StructuredStreamingKafkaCustom Constructor Called: Temp Table={}, Brokers={}, Topics={}, Batch Interval={}, Max Offsets Per Trigger={}, Starting Offsets={}",
                tempTable, brokers, top, batchInterval, maxOffsetsPerTrigger, startingOffsets);
    }

    public StructuredStreamingKafkaCustom(int processorId, String processorName, String brokers, String top,
            String batchInterval, String tempTable, String maxOffsetsPerTrigger, String startingOffsets,
            String sslEnable, String jksLocation, String jksPassword) {
        super(processorId, processorName);
        this.brokers = brokers;
        this.topics = top;
        this.batchInterval = batchInterval;
        this.tempTable = tempTable;
        this.maxOffsetsPerTrigger = maxOffsetsPerTrigger;
        this.startingOffsets = startingOffsets;
        this.sslEnable = sslEnable;
        this.jksLocation = jksLocation;
        this.jksPassword = jksPassword;
        logger.info(
                "StructuredStreamingKafkaCustom Constructor Called: Temp Table={}, Brokers={}, Topics={}, Batch Interval={}, Max Offsets Per Trigger={}, Starting Offsets={}, SSL Enable={}, JKS Location={}, JKS Password={}",
                tempTable, brokers, top, batchInterval, maxOffsetsPerTrigger, startingOffsets, sslEnable, jksLocation,
                jksPassword);
    }

    public StructuredStreamingKafkaCustom(int processorId, String processorName, String brokers, String top,
            String batchInterval, String tempTable, String maxOffsetsPerTrigger, String startingOffsets,
            String sslEnable, String jksLocation, String jksPassword, String jtsLocation, String jtsPassword,
            String trustStoreType, String keyStorType, String identificationAlgo) {
        super(processorId, processorName);
        this.brokers = brokers;
        this.topics = top;
        this.batchInterval = batchInterval;
        this.tempTable = tempTable;
        this.maxOffsetsPerTrigger = maxOffsetsPerTrigger;
        this.startingOffsets = startingOffsets;
        this.sslEnable = sslEnable;
        this.jksLocation = jksLocation;
        this.jksPassword = jksPassword;
        this.jtsLocation = jtsLocation;
        this.jtsPassword = jtsPassword;
        this.trustStoreType = trustStoreType;
        this.keyStorType = keyStorType;
        this.identificationAlgo = identificationAlgo;
        logger.info(
                "StructuredStreamingKafkaCustom Constructor Called: Temp Table={}, Brokers={}, Topics={}, Batch Interval={}, Max Offsets Per Trigger={}, Starting Offsets={}, SSL Enable={}, JKS Location={}, JKS Password={}, JTS Location={}, JTS Password={}, Trust Store Type={}, Key Store Type={}, Identification Algo={}",
                tempTable, brokers, top, batchInterval, maxOffsetsPerTrigger, startingOffsets, sslEnable, jksLocation,
                jksPassword, jtsLocation, jtsPassword, trustStoreType, keyStorType, identificationAlgo);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("StructuredStreamingKafkaCustom Processor Started: {}", this.processorName);

        Dataset<Row> lines = null;
        if (!Utils.hasValidValue(this.topics) && !Utils.hasValidValue(this.options)) {
            String exception = "Exception in StructuredStreamingKafkaCustom Processor: Topic or Options is Not Set";
            throw new NullArgumentException(exception);
        }
        if (Utils.hasValidValue(this.batchInterval)) {
            jobContext.setParameters("batchInterval", this.batchInterval);
        }

        String failOnDataLoss = ConfigUtils.getString("FAILONDATALOSS");

        DataStreamReader dataStreamReader = jobContext.getReadStream().format("kafka")
                .options(getOptionsMap(jobContext));

        if (Utils.hasValidValue(this.maxOffsetsPerTrigger)) {
            dataStreamReader = dataStreamReader.option("maxOffsetsPerTrigger", this.maxOffsetsPerTrigger);
            logger.info("Max Offsets Per Trigger: {}", this.maxOffsetsPerTrigger);

        }
        if (Utils.hasValidValue(this.startingOffsets)) {
            dataStreamReader = dataStreamReader.option("startingOffsets", this.startingOffsets);
            logger.info("Starting Offsets: {}", this.startingOffsets);
        }
        if (Utils.hasValidValue(failOnDataLoss)) {
            dataStreamReader = dataStreamReader.option("failOnDataLoss", failOnDataLoss);
            logger.info("Fail On Data Loss: {}", failOnDataLoss);
        }

        dataStreamReader.load().printSchema();
        lines = dataStreamReader.load();

        lines.createOrReplaceTempView("CAST_TABLE");

        lines = jobContext.sqlctx().sql(
                "select CAST(value AS STRING) AS value, key, topic, partition, offset, timestamp, timestampType FROM CAST_TABLE ");

        lines.printSchema();

        Utils.createOrReplaceTempView(tempTable, lines, processorName);
        return lines;
    }

    private Map<String, String> getOptionsMap(JobContext jobContext) {

        Map<String, String> options = new HashMap<>();
        if ("TRUE".equalsIgnoreCase(this.sslEnable)) {
            options.put("kafka.security.protocol", "SSL");
            options.put("kafka.ssl.truststore.location", this.jtsLocation);
            options.put("kafka.ssl.truststore.password", this.jtsPassword);
            options.put("kafka.ssl.keystore.location", this.jksLocation);
            options.put("kafka.ssl.keystore.password", this.jksPassword);
            options.put("kafka.ssl.keystore.type", this.keyStorType);
            options.put("kafka.ssl.truststore.type", this.trustStoreType);
            options.put("kafka.ssl.endpoint.identification.algorithm", this.identificationAlgo);
        }
        if (Utils.hasValidValue(this.brokers)) {
            options.put("kafka.bootstrap.servers", this.brokers);
            logger.info("Bootstrap Servers: {}", this.brokers);
        }
        if (Utils.hasValidValue(this.topics)) {
            if (this.topics.contains(SparkRunnerConstants.ASTERISK)) {
                options.put(SparkRunnerConstants.SUBSCRIBE_PATTERN, this.topics);
                jobContext.setParameters(SparkRunnerConstants.TOPICS,
                        this.topics.substring(0, this.topics.length() - 2));

            } else {
                options.put(SparkRunnerConstants.SUBSCRIBE, this.topics);
                jobContext.setParameters(SparkRunnerConstants.TOPICS, this.topics);
            }

        }
        try {
            if (Utils.hasValidValue(this.options)) {
                Map<String, String> mp = new Gson().fromJson(this.options, new TypeToken<Map<String, String>>() {
                }.getType());
                options.putAll(mp);
            }
        } catch (JsonSyntaxException e) {
            throw new JsonSyntaxException(ExceptionUtils.getStackTrace(e));
        }

        logger.info("Options Map: {}", options);
        return options;
    }
}
