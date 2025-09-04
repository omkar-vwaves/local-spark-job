package com.enttribe.pm.job.quarterly.common;


import java.io.IOException;
import java.io.InputStream;
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.enttribe.commons.io.IOUtils;
import com.enttribe.commons.lang.StringUtils;
import com.enttribe.commons.storage.s3.S3Client;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

public class UnzipToBytesForYB implements UDF1<String, List<Row>>, AbstractUDF {

    private static final Logger logger = LoggerFactory.getLogger(UnzipToBytesForYB.class);
    private static final long serialVersionUID = 1L;

    public JobContext jobContext;

    public UnzipToBytesForYB(JobContext jobContext){
        this.jobContext= jobContext;

    }

    public UnzipToBytesForYB(){

    }

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

    @Override
    public List<Row> call(String zipFilePath) throws Exception {

        logger.info("[Start] Unzipping File: {}", zipFilePath);
        
        List<Row> outputRows = new ArrayList<>();
        Map<String, String> ctx = jobContext.getParameters();

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(ctx.get("SPARK_MINIO_ENDPOINT_URL"), "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(new ClientConfiguration().withSignerOverride("AWSS3V4SignerType"))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(ctx.get("SPARK_MINIO_ACCESS_KEY"), ctx.get("SPARK_MINIO_SECRET_KEY"))))
                .build();

        S3Client s3Client = new S3Client(s3);
        String bucket = ctx.get("SPARK_MINIO_BUCKET_NAME_PM");
        String objectKey = StringUtils.substringAfter(zipFilePath, bucket + "/");

        try (InputStream input = s3Client.getObject(bucket, objectKey);
                ZipInputStream zip = new ZipInputStream(input)) {

            int index = 0;
            for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                String name = entry.getName();
                if (name.contains("MACOSX") || name.contains(".DS_Store"))
                    continue;

                byte[] data = name.endsWith(".gz") ? IOUtils.toByteArray(new GZIPInputStream(zip))
                        : IOUtils.toByteArray(zip);
                outputRows.add(RowFactory.create(index++, name, data, zipFilePath));

            }
        } catch (IOException e) {
            logger.error("[ERROR] Failed to unzip file: {}", zipFilePath, e);
            throw e;
        }

        logger.info("[End] Unzipped File: {}, Rows: {}", zipFilePath, outputRows.size());
        return outputRows;
    }
}