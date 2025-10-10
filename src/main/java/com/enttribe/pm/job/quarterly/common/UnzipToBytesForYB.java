package com.enttribe.pm.job.quarterly.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enttribe.commons.Symbol;
import com.enttribe.commons.io.IOUtils;
import com.enttribe.commons.lang.StringUtils;
import com.enttribe.commons.storage.s3.S3Client;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class UnzipToBytesForYB implements UDF1<String, List<Row>>, AbstractUDF {

    private static Logger logger = LoggerFactory.getLogger(UnzipToBytesForYB.class);

    private static final long serialVersionUID = 1L;
    final int BUFFER = 1024;
    public JobContext jobContext;
    private static final String IS_FILE_FILTER_REQUIRED = "isFileFilterRequired";
  
    private static Map<String, String> enbIdMap = null;
    private static final Object SYNCHRONIZER = new Object();

    @Override
    public String getName() {
        return "UnzipToBytes";
    }

    @Override
    public DataType getReturnType() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("partitionNumber", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("fileName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("xmlNameContent", DataTypes.BinaryType, true));
        fields.add(DataTypes.createStructField("zipFilePath", DataTypes.StringType, true));
        return DataTypes.createArrayType(DataTypes.createStructType(fields));
    }

    private static InputStream readFileFromMinIO(S3Client s3Client, String bucketName, String filePath) {

        logger.debug("Reading file from MinIO - Bucket: {}, Path: {}", bucketName, filePath);

        try {
            if (s3Client.checkBucketExist(bucketName)) {
                logger.debug("Bucket Found: {}", bucketName);
                return s3Client.getObject(bucketName, filePath);
            } else {
                logger.warn("Bucket Not Found: {}", bucketName);
            }
        } catch (Exception e) {
            logger.error("Exception While Reading File From MinIO - Bucket: {}, Path: {}, Error: {}",
                    bucketName, filePath, e.getMessage(), e);
        }
        return null;
    }

    @Override
    public List<Row> call(String zipFilePath) throws Exception {

        logger.debug("Start Processing Zip File: {}", zipFilePath);

        List<Row> fileContent = new ArrayList<>();
        int index = 0;

        Map<String, String> contextMap = jobContext.getParameters();

        // CUSTOM-NIFI-PROPERTIES
        String SPARK_MINIO_ENDPOINT_URL = contextMap.get("SPARK_MINIO_ENDPOINT_URL");
        String SPARK_MINIO_ACCESS_KEY = contextMap.get("SPARK_MINIO_ACCESS_KEY");
        String SPARK_MINIO_SECRET_KEY = contextMap.get("SPARK_MINIO_SECRET_KEY");
        String SPARK_MINIO_BUCKET_NAME_PM = contextMap.get("SPARK_MINIO_BUCKET_NAME_PM");

        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");

        logger.debug("Minio Endpoint: {}, Access key: [PROTECTED], Secret key: [PROTECTED]", SPARK_MINIO_ENDPOINT_URL);

        AWSCredentials credentials = new BasicAWSCredentials(SPARK_MINIO_ACCESS_KEY, SPARK_MINIO_SECRET_KEY);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3client = (AmazonS3) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new EndpointConfiguration(SPARK_MINIO_ENDPOINT_URL, "us-east-1")))
                .withPathStyleAccessEnabled(true))
                .withClientConfiguration(clientConfiguration))
                .withCredentials(new AWSStaticCredentialsProvider(credentials)))
                .build();

        S3Client client = new S3Client(s3client);

        String zipFileKey = StringUtils.substringAfter(zipFilePath, SPARK_MINIO_BUCKET_NAME_PM + "/");
        logger.debug("Extracting Zip File: {}", zipFileKey);

        InputStream fsin = null;

        try{
            fsin = readFileFromMinIO(client, SPARK_MINIO_BUCKET_NAME_PM, zipFileKey);

            if (fsin == null) {
                logger.warn("Input stream is null for file: {}. This might mean the file does not exist or there was an access issue. Returning an empty list.", zipFilePath);
                return new ArrayList<>(); // Return an empty list if InputStream is null
            }

            try (ZipInputStream zip = new ZipInputStream(new java.io.BufferedInputStream(fsin, 65536))) {
            ZipEntry entry = zip.getNextEntry();
            String isFileFilterRequired = jobContext.getParameter(IS_FILE_FILTER_REQUIRED);
            logger.debug("File Filter Required: {}", isFileFilterRequired);

            while (entry != null) {
                String name = entry.getName();

                // Skip Entries that contain "MACOSX"
                if (name.contains("MACOSX")) {
                    entry = zip.getNextEntry();
                    continue; // Skip this iteration and move to the next entry
                }

                if (isFileFilterRequired != null && !isFileFilterRequired.isEmpty()) {
                    initializeEnbIdMap(jobContext);
                    String enbid = StringUtils.substringAfterLast(
                            StringUtils.substringBeforeLast(name, Symbol.DOT_STRING),
                            Symbol.EQUAL_STRING);
                    if (enbIdMap.containsKey(enbid)) {
                        index = getByteArrayFromFileData(zipFilePath, fileContent, index, zip, name);
                    }
                } else {
                    index = getByteArrayFromFileData(zipFilePath, fileContent, index, zip, name);
                }
                entry = zip.getNextEntry();

            }
        }
        } catch (IOException e) {
            logger.error("IOException While Extracting Zip file: {}, Error: {}", zipFilePath, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Error While Extracting Zip file: {}, Error: {}", zipFilePath, e.getMessage(), e);
            throw e;
        }
        finally {
            if (fsin != null) {
                try {
                    fsin.close();
                } catch (IOException e) {
                    logger.error("Error closing InputStream for file {}: {}", zipFilePath, e.getMessage());
                }
            }
        }

        logger.debug("End Processing Zip File: {}, Total Rows Generated: {}",
                zipFilePath,
                fileContent != null ? fileContent.size() : 0);

        return fileContent;
    }

    private int getByteArrayFromFileData(String zipFilePath, List<Row> fileContent, int i, ZipInputStream zip,
            String name) throws IOException {

        logger.debug("Start Extracting File Data From Zip: {}, File Name: {}", zipFilePath, name);

        InputStream inputStream = null;
        if (name.endsWith(".gz")) {
            inputStream = new GZIPInputStream(zip);
        } else {
            inputStream = zip;
        }
        byte[] byteArray = IOUtils.toByteArray(inputStream);
        fileContent.add(RowFactory.create(i++, name, byteArray, zipFilePath));

        logger.debug("End Extracting File Data From Zip: {}, fileName: {}", zipFilePath, name);
        return i;
    }

    private void initializeEnbIdMap(JobContext jobcontext) {
        if (enbIdMap != null) {
            return;
        }

        synchronized (SYNCHRONIZER) {
            if (enbIdMap == null) {

                String json = jobcontext.getParameter("ENBID_MAPJSON");
                if (json == null || json.isEmpty()) {
                    logger.error("ENBID_MAPJSON is NULL OR Empty!");
                    return;
                }
                logger.debug("Loading ENBID_MAPJSON");

                try {
                    enbIdMap = new ObjectMapper().readValue(json,
                            new TypeReference<Map<String, String>>() {
                            });

                    logger.debug("ENBID_MAPJSON Loaded Successfully, Size: {}", enbIdMap.size());

                } catch (JsonProcessingException e) {
                    logger.error("Failed to Parse ENBID_MAPJSON: {}", e.getMessage(), e);
                } catch (Exception e) {
                    logger.error("Unexpected Error While Loading ENBID_MAPJSON: {}", e.getMessage(), e);
                }
            }
        }
    }
}