package com.enttribe.custom.processor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processor.audit.ProcessorAudit;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.AWSS3Util;
import com.enttribe.sparkrunner.util.MINIOUtil;
import com.enttribe.sparkrunner.util.Utils;
import com.enttribe.sparkrunner.utils.exceptions.BusinessException;

public class ListMINIOFilesCustomV1 extends Processor implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(ListMINIOFilesCustomV1.class);
    @Dynamic
    public String path;
    public String columnName;
    public String tempTable;
    public String isRecursive;
    public String bucket;

    private JobContext context = null;

    public ListMINIOFilesCustomV1() {
    }

    public ListMINIOFilesCustomV1(int processorId, String processorName, String hdfsFolderPath, String columnName,
            String tempTable) {
        super(processorId, processorName);
        this.path = hdfsFolderPath;
        this.columnName = columnName;
        this.tempTable = tempTable;
    }

    public ListMINIOFilesCustomV1(int processorId, String processorName, String hdfsFolderPath, String columnName,
            String bucket, String tempTable, String isRecursive) {
        super(processorId, processorName);
        this.path = hdfsFolderPath;
        this.columnName = columnName;
        this.bucket = bucket;
        this.tempTable = tempTable;
        this.isRecursive = isRecursive;
    }

    public ListMINIOFilesCustomV1(Dataset<Row> dataFrame, int processorId, String processorName, String path,
            String columnName, String bucket, String tempTable, String isRecursive) {
        super(processorId, processorName); // Only pass ID and Name to parent (basic info)
        this.dataFrame = dataFrame; // Initialize custom fields yourself
        this.path = path;
        this.columnName = columnName;
        this.bucket = bucket;
        this.tempTable = tempTable;
        this.isRecursive = isRecursive;
    }

    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        context = jobContext;

        List<String> files = new ArrayList<>();
        String[] bucketArr = this.bucket.split(Symbol.COMMA_STRING);
        logger.info("🚀 Provided Bucket Name: {}", Arrays.toString(bucketArr));

        Map<String, Set<String>> generateBucketPathMap = this.generateBucketPathMap();
        logger.info("🚀 Generated Bucket-Path Map: {}", generateBucketPathMap);

        boolean isRecursive = Boolean.parseBoolean(this.isRecursive);
        logger.info("🚀 Provided Recursive Flag: {}", isRecursive);

        // String fileSystem = ConfigUtils.getString("MINIO_SYSTEM");
        String fileSystem = "minio";
        logger.info("🚀 File System From Config: {}", fileSystem);

        if ((!Utils.hasValidValue(fileSystem) || !fileSystem.equalsIgnoreCase("aws"))
                && (!Utils.hasValidValue(fileSystem) || !fileSystem.equalsIgnoreCase("minio"))) {

            String errorMsg = "Folder Doesn't Exist For The ➡️  Bucket: {} With ➡️  Path: {}";
            logger.error("🚀 {}", errorMsg, this.bucket, this.path);
            throw new BusinessException(errorMsg);
        } else {
            logger.info("🚀 Proceeding to Fetch Files From S3 For The ➡️  Bucket: {} With ➡️  Path: {}", this.bucket,
                    this.path);
            this.getAllFilesFromAWSS3(files, bucketArr, generateBucketPathMap, isRecursive);
        }

        logger.info("🚀 Total Files Collected: {}", files.size());

        List<Row> fileList = this.getListOfRowFromListOfString(files);
        logger.info("🚀 Converted File Paths To Row List. Total Rows: {}", fileList.size());

        StructType schema = this.getSchemaForFileRdd(this.columnName);

        this.dataFrame = jobContext.createDataFrame(fileList, files.size(), schema);
        logger.info("🚀 Created DataFrame With {} Records", this.dataFrame.count());

        Utils.createOrReplaceTempView(this.tempTable, this.dataFrame, this.processorName);
        logger.info("🚀 Created or Replaced Temp Table View: {}", this.tempTable);

        return this.dataFrame;
    }

    private void getAllFilesFromAWSS3(List<String> files, String[] bucketArr, Map<String, Set<String>> prefixMap,
            boolean isRecursive) throws Exception {

        String endpoint = context.getParameter("SPARK_MINIO_ENDPOINT_URL");
        String accessKey = context.getParameter("SPARK_MINIO_ACCESS_KEY");
        String secretKey = context.getParameter("SPARK_MINIO_SECRET_KEY");

        logger.info("🚀 Starting to List Files From AWS S3 (MINIO-compatible) For The ➡️  Bucket: {} With ➡️  Path: {}",
                this.bucket, this.path);

        for (String rawBucket : bucketArr) {
            String bucket = rawBucket.trim();
            logger.info("🚀 Processing Bucket: {}", bucket);

            if (AWSS3Util.isBucketExist(AWSS3Util.getS3Client(endpoint, accessKey, secretKey), bucket)) {
                logger.info("Bucket '{}' Exists ✅", bucket);

                if (prefixMap == null) {
                    logger.warn("⚠️ No Prefix Map Provided, Listing All Files");
                    List<String> listOfFiles = AWSS3Util.listMINIO(bucket, isRecursive, endpoint, accessKey, secretKey);
                    logger.info("🚀 Retrieved {} Files From Bucket '{}'", listOfFiles.size(), bucket);

                    if (!listOfFiles.isEmpty()) {
                        files.addAll(listOfFiles);
                    }
                } else {

                    logger.info("🚀 Prefix Map Provided, Listing Files With Prefixes");

                    logger.info("🚀 Prefix Map: {}", prefixMap);

                    Set<String> prefixSet = prefixMap.get(bucket);

                    if (prefixSet == null || prefixSet.isEmpty()) {
                        logger.warn("⚠️ No Prefixes Found For Bucket '{}'", bucket);
                        continue;
                    }

                    logger.info("Prefixes Found For Bucket '{}': {} ✅", bucket, prefixSet);

                    for (String prefixpath : prefixSet) {
                        logger.info("Listing Files In Bucket '{}' With Prefix '{}'", bucket,
                                prefixpath);
                        List<String> listOfFiles = AWSS3Util.listMINIO(bucket, prefixpath, isRecursive, endpoint,
                                accessKey, secretKey);
                        logger.info("Retrieved {} Files For Prefix '{}'", listOfFiles.size(),
                                prefixpath);
                        files.addAll(listOfFiles);
                    }
                }
            } else {
                String errorMsg = "Folder Doesn't Exist For The ➡️  Bucket: {}";
                logger.error("🚀 {}", errorMsg, bucket);
                throw new BusinessException(errorMsg);
            }
        }
    }

    private Map<String, Set<String>> generateBucketPathMap() throws BusinessException {
        if (this.path != null && this.path.length() > 0) {
            String[] folderPathArr = this.path.split(Symbol.COMMA_STRING);
            Map<String, Set<String>> result = new HashMap();
            String[] var3 = folderPathArr;
            int var4 = folderPathArr.length;

            for (int var5 = 0; var5 < var4; ++var5) {
                String bucketpath = var3[var5];
                String[] bucketpatharr = bucketpath.split(":");
                String bucketname = bucketpatharr[0].trim();
                if (bucketname.length() <= 0 || bucketpatharr.length <= 1) {
                    throw new BusinessException("Invalid Path Prefix  or Bucket " + this.processorName
                            + " :: Bucket Name : " + bucketname + " Path Prefix : " + String.valueOf(bucketpatharr));
                }

                if (result.containsKey(bucketname)) {
                    if (bucketpatharr[1] != null && bucketpatharr[1].trim().length() > 1) {
                        ((Set) result.get(bucketname)).add(bucketpatharr[1]);
                    }
                } else {
                    HashSet<String> path = new HashSet();
                    if (bucketpatharr[1] != null && bucketpatharr[1].trim().length() > 1) {
                        path.add(bucketpatharr[1]);
                        result.put(bucketname, path);
                    }
                }
            }

            return result;
        } else {
            return null;
        }
    }

    private List<Row> getListOfRowFromListOfString(List<String> files) {
        List<Row> fileList = new ArrayList();
        Iterator var3 = files.iterator();

        while (var3.hasNext()) {
            String string = (String) var3.next();
            Row row = RowFactory.create(new Object[] { string });
            fileList.add(row);
        }

        return fileList;
    }

    private StructType getSchemaForFileRdd(String columnName) {
        StructField[] structFields = new StructField[] {
                DataTypes.createStructField(columnName, DataTypes.StringType, false) };
        return DataTypes.createStructType(structFields);
    }

    public List<String> validProcessorConfigurations() {
        List<String> list = new ArrayList();
        if (!Utils.hasValidValue(this.path)) {
            list.add("Exception in ListHDFSFiles processor named " + this.processorName + " : hdfsFolderPath is "
                    + this.path + " that must not be " + this.path);
        }

        return list;
    }
}
