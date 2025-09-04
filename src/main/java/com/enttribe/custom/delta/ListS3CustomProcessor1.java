// package com.enttribe.custom.processor;
package com.enttribe.custom.delta;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.*;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;

public class ListS3CustomProcessor1 implements Runnable {

    public static final boolean IS_LOCAL = false;
    private Set<String> errorList = new HashSet<>();
    private Map<String, String> successFlowFileAttributes = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ListS3CustomProcessor1.class);

    // Define relationships (should be static and reused)
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to process")
            .build();

    @Override
    public void run() {
        // No-op
    }

    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
                               Connection connection, String cacheValue) {

        logger.info("🔍 Starting MinIO List -> Single FlowFile Logic...");

        try {
            // === Read from FlowFile attributes ===
            String bucket = getOrDefault(flowFile, "bucketName", "performance");
            String prefix = getOrDefault(flowFile, "RawFilePath", "/JOB/RawFiles/JUNIPER/20250616/1800/");
            String endpoint = getOrDefault(flowFile, "s3.endpoint", "http://seaweedfs-s3.swf.svc.cluster.local:8333");
            String accessKey = getOrDefault(flowFile, "s3.access.key", "bootadmin");
            String secretKey = getOrDefault(flowFile, "s3.secret.key", "bootadmin");

            if (bucket == null || endpoint == null || accessKey == null || secretKey == null) {
                throw new IllegalArgumentException("Missing one or more required attributes: "
                        + "[bucketName, s3.endpoint, s3.access.key, s3.secret.key]");
            }

            if (prefix == null) {
                prefix = "";
            }

            // === Set up S3 Client (MinIO compatible) ===
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(new ClientConfiguration().withSignerOverride("AWSS3V4SignerType"))
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(accessKey, secretKey)))
                    .build();

            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucket)
                    .withPrefix(prefix);

            ListObjectsV2Result result = s3.listObjectsV2(request);
            List<String> fileNames = new ArrayList<>();
            int jsonFileCount = 0;

            for (S3ObjectSummary obj : result.getObjectSummaries()) {
                String key = obj.getKey();
                if (key.endsWith(".zip")) {
                    // Download zip file from S3
                    S3Object s3Object = s3.getObject(bucket, key);
                    try (ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent())) {
                        ZipEntry entry;
                        while ((entry = zis.getNextEntry()) != null) {
                            if (!entry.isDirectory() && entry.getName().endsWith(".json")) {
                                jsonFileCount++;
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error reading zip file from S3: {}", key, e);
                    }
                }
            }

            logger.info("✅ Found {} .json files inside zips in bucket [{}] with prefix [{}]",
                    jsonFileCount, bucket, prefix);

            String countStr = String.valueOf(jsonFileCount);
            byte[] content = countStr.getBytes(StandardCharsets.UTF_8);

            // === Write to FlowFile
            flowFile = session.write(flowFile, out -> out.write(content));
            flowFile = session.putAttribute(flowFile, "s3.json.file.count", countStr);
            flowFile = session.putAttribute(flowFile, "s3.list.success", "true");

            session.transfer(flowFile, REL_SUCCESS);
            logger.info("🚀 FlowFile transferred to success with .json file count!");

        } catch (Exception e) {
            logger.error("❌ Exception in listing files: {}", e.getMessage(), e);
            createFailureJson(session, flowFile, e.getMessage());
        } finally {
            closeConnection(connection);
        }

        return flowFile;
    }

    private String getOrDefault(FlowFile flowFile, String attr, String defaultValue) {
        String val = flowFile.getAttribute(attr);
        return val != null ? val : defaultValue;
    }

    private void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("❌ Exception Occurred in closeConnection Method, Message: " + e.getMessage());
        }
    }

    private void createFailureJson(ProcessSession session, FlowFile failureFlowFile, String errorMessage) {
        JSONObject errorLogs = new JSONObject();
        errorLogs.put("errorDescription", errorMessage);
        errorLogs.put("processorName", "ALARM FLOODING BLACKLIST");
        errorLogs.put("processorType", "JavaSnippet");

        failureFlowFile = session.putAttribute(failureFlowFile, "ErrorLogs", errorLogs.toString());
        session.transfer(failureFlowFile, REL_FAILURE);

        logger.info("Transferred FlowFile to Failure Relationship Due to Error: " + errorMessage);
    }
}
