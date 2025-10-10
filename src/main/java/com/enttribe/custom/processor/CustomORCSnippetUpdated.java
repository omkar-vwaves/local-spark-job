package com.enttribe.custom.processor;

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
import com.enttribe.commons.lang.StringUtils;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;

public class CustomORCSnippetUpdated implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CustomORCSnippetUpdated.class);

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile Successfully Processed!")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile Failed to Process!")
            .build();

    @Override
    public void run() {
    }

    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) {

        logger.info("🔍 Starting MinIO List: Single FlowFile Logic...");
        try {
            String bucket = flowFile.getAttribute("BUCKET");
            bucket = bucket != null ? bucket : "performance";
            String domain = flowFile.getAttribute("domain");
            String vendor = flowFile.getAttribute("vendor");
            String technology = flowFile.getAttribute("technology");
            String jobType = flowFile.getAttribute("jobType");
            String tmpOrcBasePath = flowFile.getAttribute("TMP_ORC_BASE_PATH");
            String tmpOrcPathCurrentDate = flowFile.getAttribute("TMP_ORC_PATH_CURRENT_DATE");
            String tmpOrcPathCurrentTime = flowFile.getAttribute("TMP_ORC_PATH_CURRENT_TIME");

            logger.info(
                    "🔍 Bucket: {}, Domain: {}, Vendor: {}, Technology: {}, JobType: {}, TMP_ORC_BASE_PATH: {}, TMP_ORC_PATH_CURRENT_DATE: {}, TMP_ORC_PATH_CURRENT_TIME: {}",
                    bucket, domain, vendor, technology, jobType, tmpOrcBasePath, tmpOrcPathCurrentDate,
                    tmpOrcPathCurrentTime);

            String prefix = tmpOrcBasePath;
            if (prefix.startsWith("s3a://")) {
                prefix = StringUtils.substringAfter(prefix, "s3a://");
                prefix = StringUtils.substringAfter(prefix, bucket + "/");
            } else if (prefix.startsWith("s3://")) {
                prefix = StringUtils.substringAfter(prefix, "s3://");
                prefix = StringUtils.substringAfter(prefix, bucket + "/");
            }

            logger.info("🔍 Constructed Prefix: {}", prefix);

            if (domain != null && !domain.isEmpty()) {
                prefix = prefix + domain + "/";
            }
            if (vendor != null && !vendor.isEmpty()) {
                prefix = prefix + vendor + "/";
            }
            if (technology != null && !technology.isEmpty()) {
                prefix = prefix + technology + "/";
            }
            if (jobType != null && !jobType.isEmpty()) {
                prefix = prefix + jobType + "/";
            }

            tmpOrcPathCurrentDate = "date=" + tmpOrcPathCurrentDate;
            tmpOrcPathCurrentTime = "time=" + tmpOrcPathCurrentTime;
            prefix = prefix + tmpOrcPathCurrentDate + "/" + tmpOrcPathCurrentTime + "/";

            logger.info("🔍 Final Prefix: {}", prefix);

            String endpoint = flowFile.getAttribute("NIFI_MINIO_ENDPOINT_URL");
            endpoint = endpoint != null ? endpoint : "http://seaweedfs-s3.swf.svc.cluster.local:8333";
            String accessKey = flowFile.getAttribute("NIFI_MINIO_ACCESS_KEY");
            accessKey = accessKey != null ? accessKey : "bootadmin";
            String secretKey = flowFile.getAttribute("NIFI_MINIO_SECRET_KEY");
            secretKey = secretKey != null ? secretKey : "bootadmin";

            if (bucket == null || endpoint == null || accessKey == null || secretKey == null) {
                throw new IllegalArgumentException("Missing One or More Required Attributes: "
                        + "[bucketName, s3.endpoint, s3.access.key, s3.secret.key]");
            }

            logger.info("🔍 Bucket: {}, Endpoint: {}, Access Key: {}, Secret Key: {}", bucket, endpoint, accessKey,
                    secretKey);
            logger.info("🔍 TMP_ORC_BASE_PATH: {}", tmpOrcBasePath);
            logger.info("🔍 TMP_ORC_PATH_CURRENT_DATE: {}", tmpOrcPathCurrentDate);
            logger.info("🔍 TMP_ORC_PATH_CURRENT_TIME: {}", tmpOrcPathCurrentTime);
            logger.info("🔍 Final Prefix: {}", prefix);

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
            int snappyOrcFileCount = 0;

            for (S3ObjectSummary obj : result.getObjectSummaries()) {
                String key = obj.getKey();
                if (key.endsWith(".snappy.orc")) {
                    snappyOrcFileCount++;
                }
            }

            logger.info("✅ Found {} .snappy.orc Files In Bucket [{}] with Prefix [{}]",
                    snappyOrcFileCount, bucket, prefix);

            String countStr = String.valueOf(snappyOrcFileCount);
            byte[] content = countStr.getBytes(StandardCharsets.UTF_8);

            // === Write to FlowFile
            flowFile = session.write(flowFile, out -> out.write(content));
            flowFile = session.putAttribute(flowFile, "s3.snappy.orc.file.count", countStr);
            flowFile = session.putAttribute(flowFile, "s3.list.success", "true");
            flowFile = session.putAttribute(flowFile, "file_name", bucket + "/" + prefix);

            session.transfer(flowFile, REL_SUCCESS);
            logger.info("🚀 FlowFile Transferred to Success with .snappy.orc File Count!");

        } catch (Exception e) {
            logger.error("❌ Exception in listing files: {}", e.getMessage(), e);
            createFailureJson(session, flowFile, e.getMessage());
        } finally {
            closeConnection(connection);
        }

        return flowFile;
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
        errorLogs.put("processorName", "CustomORCSnippetUpdated");
        errorLogs.put("processorType", "JavaSnippet");
        failureFlowFile = session.putAttribute(failureFlowFile, "ErrorLogs", errorLogs.toString());
        failureFlowFile = session.putAttribute(failureFlowFile, "s3.list.success", "false");
        session.transfer(failureFlowFile, REL_FAILURE);
        logger.info("Transferred FlowFile to Failure Relationship Due to Error: " + errorMessage);
    }
}