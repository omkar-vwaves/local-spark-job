package com.enttribe.custom.processor;

import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.workflowengine.CustomSchema;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ORCRead extends Processor {
    private static final long serialVersionUID = 1L;

    @Dynamic
    public String directoryPath;

    @Dynamic
    public String inputSchema;

    @Dynamic
    public String basePath;

    @Dynamic
    public String tempTable;

    public ORCRead() {
    }

    public ORCRead(int processorId, String processorName, String inputPath, String tempTable, String basePath) {
        super(processorId, processorName);
        this.directoryPath = inputPath;
        this.tempTable = tempTable;
        this.basePath = basePath;
    }

    public ORCRead(int processorId, String processorName, String directoryPath, String inputSchema, String basePath,
            String tempTable) {
        super(processorId, processorName);
        this.directoryPath = directoryPath;
        this.inputSchema = inputSchema;
        this.basePath = basePath;
        this.tempTable = tempTable;
    }

    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("====== ORCRead Execution Started! ======");
        long startTime = System.currentTimeMillis();

        jobContext.sqlctx().setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false");

        String[] inputPaths = this.directoryPath.split(",");
        logger.info("Input Paths: {}", Arrays.toString(inputPaths));

        Map<String, String> optionsMap = this.getOptionsMap();
        if (isNonEmpty(this.inputSchema)) {
            logger.info("Input Schema: {}", this.inputSchema);
            this.dataFrame = jobContext.sqlctx().read().options(optionsMap).schema(this.getSchema(this.inputSchema))
                    .orc(inputPaths);
        } else {
            this.dataFrame = jobContext.sqlctx().read().options(optionsMap).orc(inputPaths);
        }

        this.dataFrame = normalizePartitionColumns(this.dataFrame);

        if (!isNonEmpty(this.tempTable)) {
            this.tempTable = "OrcRead";
        }
        this.dataFrame.createOrReplaceTempView(this.tempTable);
        long count = this.dataFrame.count();
        logger.info("Total Rows: {}", count);
        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / (1000 * 60);
        long seconds = (durationMillis / 1000) % 60;
        logger.info("====== ORCRead Execution Completed in {} minutes and {} seconds! ======", minutes, seconds);        return this.dataFrame;
    }

    private Dataset<Row> normalizePartitionColumns(Dataset<Row> df) {
        try {
            String[] cols = df.columns();
            for (String c : cols) {
                if ("time".equalsIgnoreCase(c) || "ptime".equalsIgnoreCase(c)) {
                    df = df.withColumn(c, functions.lpad(df.col(c).cast("string"), 4, "0"));
                }
            }
        } catch (Exception ignore) {
        }
        return df;
    }

    private Map<String, String> getOptionsMap() {
        Map<String, String> options = new HashMap<>();
        if (isNonEmpty(this.basePath)) {
            options.put("basePath", this.basePath);
        }

        return options;
    }

    public StructType getSchema(String inputSchema) {
        String[] schema = inputSchema.split(",");
        List<StructField> fields = new ArrayList<>();
        for (String col : schema) {
            String[] colNamesAndDataTypes = col.split(":");
            fields.add(DataTypes.createStructField(colNamesAndDataTypes[0],
                    CustomSchema.getSparkSQLType(CustomSchema.getType(colNamesAndDataTypes[1])), true));
        }

        StructType structType = DataTypes.createStructType(fields);
        return structType;
    }

    public static String getExistingFiles(String filePaths) {
        if (filePaths == null || filePaths.trim().isEmpty())
            return null;
        StringBuilder existing = new StringBuilder();
        String[] paths = filePaths.split(",");
        for (String p : paths) {
            String path = p == null ? null : p.trim();
            if (path == null || path.isEmpty())
                continue;
            try {
                FileSystem fs = FileSystem.get(new URI(path), new Configuration());
                if (fs.exists(new Path(path))) {
                    if (existing.indexOf(path) < 0) {
                        existing.append(path).append(",");
                    }
                } else {
                    logger.error("Path [{}] Doesn't Exist in SeaweedFS", path);
                    throw new RuntimeException("Path [" + path + "] Doesn't Exist in SeaweedFS");
                }
            } catch (Exception e) {
                logger.error("Failed to Check Path Existence: {}", path, e);
                throw new RuntimeException("Failed to Check Path Existence: " + path, e);
            }
        }
        if (existing.length() > 0) {
            existing.setLength(existing.length() - 1);
            logger.info("Existing File Paths: {}", existing);
            return existing.toString();
        }
        throw new RuntimeException("No Existing File Paths Found");
    }

    public List<String> validProcessorConfigurations() {
        List<String> issues = new ArrayList<>();
        if (!isNonEmpty(this.directoryPath)) {
            issues.add("Exception Inside ORCRead Processor named " + this.processorName + " : Path is "
                    + this.directoryPath + " that must not be " + this.directoryPath);
        }
        return issues;
    }

    private boolean isNonEmpty(String s) {
        return s != null && !s.trim().isEmpty();
    }
}
