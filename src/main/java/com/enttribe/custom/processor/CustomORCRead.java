package com.enttribe.custom.processor;

import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SparkSession;

public class CustomORCRead extends Processor {
    private static final long serialVersionUID = 1L;

    @Dynamic
    public String inputPath;

    @Dynamic
    public String basePath;

    @Dynamic
    public String fallbackPath;

    @Dynamic
    public String inputSchema;

    public String tempTable;

    public CustomORCRead() {
    }

    public CustomORCRead(int processorId, String processorName, String inputPath, String basePath, String fallbackPath,
            String tempTable) {
        super(processorId, processorName);
        this.inputPath = inputPath;
        this.basePath = basePath;
        this.fallbackPath = fallbackPath;
        this.tempTable = tempTable;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        SparkSession spark = jobContext.sqlctx().sparkSession();
        String effectivePath = resolveExistingPath(spark, inputPath, fallbackPath);

        Map<String, String> options = new HashMap<>();
        if (basePath != null && !basePath.isEmpty()) {
            options.put("basePath", basePath);
        }

        Map<String, String> jobProps = jobContext.getParameters();
        if (jobProps != null) {
            options.put("fs.s3a.endpoint", jobProps.getOrDefault("SPARK_MINIO_ENDPOINT_URL", ""));
            options.put("fs.s3a.access.key", jobProps.getOrDefault("SPARK_MINIO_ACCESS_KEY", ""));
            options.put("fs.s3a.secret.key", jobProps.getOrDefault("SPARK_MINIO_SECRET_KEY", ""));
            options.put("fs.s3a.path.style.access", "true");
            options.put("fs.s3a.connection.ssl.enabled", "false");
        }

        Dataset<Row> df;

        if (inputSchema != null && !inputSchema.trim().isEmpty()) {

            logger.info("Current State={}", this.dataFrame.isStreaming());
            logger.info("Effective Path={}", effectivePath);
            logger.info("Options={}", options);

            if (this.dataFrame.isStreaming()) {
                df = spark.readStream()
                        .options(options)
                        .schema(parseSchema(inputSchema))
                        .orc(effectivePath);
            } else {
                df = spark.read()
                        .options(options)
                        .schema(parseSchema(inputSchema))
                        .orc(effectivePath);
            }

        } else {

            logger.info("Current State={}", this.dataFrame.isStreaming());
            logger.info("Effective Path={}", effectivePath);
            logger.info("Options={}", options);

            if (this.dataFrame.isStreaming()) {
                df = spark.readStream()
                        .options(options)
                        .orc(effectivePath);
            } else {
                df = spark.read()
                        .options(options)
                        .orc(effectivePath);
            }
        }

        if (tempTable != null && !tempTable.isEmpty()) {
            df.createOrReplaceTempView(tempTable);
        }

        this.dataFrame = df;
        logger.info("ORC File Read Successfully For Path={}", effectivePath);
        return df;
    }

    private String resolveExistingPath(SparkSession spark, String primaryPath, String fallbackPath) throws Exception {
        try {
            if (primaryPath != null) {
                spark.read().orc(primaryPath).limit(1).count();
                logger.error("Primary Path Found: {}", primaryPath);
                return primaryPath;
            }
        } catch (Exception e) {
            logger.error("Primary Path Not Found: {}", primaryPath);
        }

        try {
            if (fallbackPath != null) {
                spark.read().orc(fallbackPath).limit(1).count();
                logger.error("Primary Path Not Found. Falling Back To: {}", fallbackPath);
                return fallbackPath;
            }
        } catch (Exception e) {
            logger.error("Fallback Path Not Found: {}", fallbackPath);
        }

        throw new RuntimeException("Neither Primary Nor Fallback Path Exists or is Accessible.");
    }

    private StructType parseSchema(String schemaString) {
        String[] fields = schemaString.split(",");
        List<StructField> structFields = new ArrayList<>();

        for (String field : fields) {
            String[] parts = field.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid Schema Format. Expected 'name:datatype'. Found: " + field);
            }

            String name = parts[0].trim();
            String type = parts[1].trim().toLowerCase();

            DataType dataType = getSparkDataType(type);
            structFields.add(DataTypes.createStructField(name, dataType, true));
        }

        return DataTypes.createStructType(structFields);
    }

    private DataType getSparkDataType(String type) {
        switch (type) {
            case "string":
                return DataTypes.StringType;
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "long":
                return DataTypes.LongType;
            case "double":
                return DataTypes.DoubleType;
            case "float":
                return DataTypes.FloatType;
            case "boolean":
                return DataTypes.BooleanType;
            case "timestamp":
                return DataTypes.TimestampType;
            case "date":
                return DataTypes.DateType;
            default:
                throw new IllegalArgumentException("Unsupported Data Type: " + type);
        }
    }

    @Override
    public List<String> validProcessorConfigurations() {
        List<String> issues = new ArrayList<>();
        if (inputPath == null || inputPath.trim().isEmpty()) {
            issues.add("Input Path is Required But Missing.");
        }
        return issues;
    }
}
