package com.enttribe.pm.job.report.common;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.enttribe.sparkrunner.context.JobContext;

import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import java.io.*;
import org.apache.poi.xssf.usermodel.XSSFRow;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;

import java.awt.Color;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;

public class WriteTrendReportWithRag extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(WriteTrendReportWithRag.class);
    private static String SPARK_MINIO_ENDPOINT_URL = "SPARK_MINIO_ENDPOINT_URL";
    private static String SPARK_MINIO_ACCESS_KEY = "SPARK_MINIO_ACCESS_KEY";
    private static String SPARK_MINIO_SECRET_KEY = "SPARK_MINIO_SECRET_KEY";
    private static String SPARK_MINIO_BUCKET_NAME_PM = "SPARK_MINIO_BUCKET_NAME_PM";

    public WriteTrendReportWithRag() {
        super();
    }

    public WriteTrendReportWithRag(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
    }

    public WriteTrendReportWithRag(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.info("[WriteTrendReportWithRag] Execution Started!");

        long startTime = System.currentTimeMillis();

        Map<String, String> jobContextMap = jobContext.getParameters();

        SPARK_MINIO_ENDPOINT_URL = jobContextMap.get("SPARK_MINIO_ENDPOINT_URL");
        SPARK_MINIO_ACCESS_KEY = jobContextMap.get("SPARK_MINIO_ACCESS_KEY");
        SPARK_MINIO_SECRET_KEY = jobContextMap.get("SPARK_MINIO_SECRET_KEY");
        SPARK_MINIO_BUCKET_NAME_PM = jobContextMap.get("SPARK_MINIO_BUCKET_NAME_PM");

        logger.info("MinIO Credentials: Endpoint={}, AccessKey={}, SecretKey={}, BucketName={}",
                SPARK_MINIO_ENDPOINT_URL, SPARK_MINIO_ACCESS_KEY, SPARK_MINIO_SECRET_KEY, SPARK_MINIO_BUCKET_NAME_PM);

        String extraParameters = jobContextMap.get("EXTRA_PARAMETERS");
        String reportWidgetDetails = jobContextMap.get("REPORT_WIDGET_DETAILS");
        String ragConfiguration = jobContextMap.get("RAG_CONFIGURATION");
        if (reportWidgetDetails == null) {
            throw new Exception(
                    "REPORT_WIDGET_DETAILS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (extraParameters == null) {
            throw new Exception(
                    "EXTRA_PARAMETERS is NULL. Please Ensure Extra Parameters is Properly Initialized Before Processing.");
        }

        if (ragConfiguration == null) {
            throw new Exception(
                    "RAG_CONFIGURATION is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extraParameters,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> reportWidgetDetailsMap = new ObjectMapper().readValue(reportWidgetDetails,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, Map<String, String>> ragConfigurationMap = new ObjectMapper().readValue(ragConfiguration,
                new TypeReference<Map<String, Map<String, String>>>() {
                });

        logger.info("RAG Configuration: {}", ragConfigurationMap);

        String reportWidgetIdPk = reportWidgetDetailsMap.get("reportWidgetIdPk");
        String generatedReportId = reportWidgetDetailsMap.get("generatedReportId");
        jobContext.setParameters("REPORT_WIDGET_ID_PK", reportWidgetIdPk);
        jobContext.setParameters("GENERATED_REPORT_ID", generatedReportId);
        logger.info("REPORT_WIDGET_ID_PK '{}' And GENERATED_REPORT_ID '{}' Set to Job Context Successfully!",
                reportWidgetIdPk, generatedReportId);

        String reportFileName = getReportFileName(jobContextMap);
        logger.info("Report File Name: {}", reportFileName);

        Dataset<Row> expectedDF = this.dataFrame;

        String reportFormatType = extraParametersMap.get("reportFormatType");

        if (reportFormatType == null) {
            throw new Exception(
                    "REPORT_FORMAT_TYPE is NULL. Please Ensure Report Format Type is Properly Set.");
        }

        logger.info("Report Format Type: {}", reportFormatType);
        reportFormatType = reportFormatType == null ? "csv" : reportFormatType;

        if (reportFormatType.equalsIgnoreCase("csv")) {
            processCSVReport(expectedDF, extraParametersMap, jobContext, reportFileName);
        } else if (reportFormatType.equalsIgnoreCase("excel")) {
            processExcelReport(expectedDF, extraParametersMap, jobContext, reportFileName, ragConfigurationMap);
        } else {
            processCSVReport(expectedDF, extraParametersMap, jobContext, reportFileName);
        }

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.info("[WriteTrendReportWithRag] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes,
                seconds);

        return this.dataFrame;
    }

    private static void processCSVReport(Dataset<Row> expectedDF, Map<String, String> extraParametersMap,
            JobContext jobContext, String reportFileName) {

        String bucketName = SPARK_MINIO_BUCKET_NAME_PM;
        String minioEndpointUrl = SPARK_MINIO_ENDPOINT_URL;
        String minioAccessKey = SPARK_MINIO_ACCESS_KEY;
        String minioSecretKey = SPARK_MINIO_SECRET_KEY;
        String minioRegion = "us-east-1";

        String pushedDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String tmpMinioDirPath = "protected/PM/FlowReport/tmp/" + UUID.randomUUID();
        String finalMinioPath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

        String reportWidgetFilePath = reportFileName;
        String generatedReportFilePath = "/" + finalMinioPath;

        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);
        logger.info("REPORT_WIDGET_FILE_PATH '{}' And GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully!",
                reportWidgetFilePath);

        AmazonS3 s3client = null;

        try {
            logger.info("Step 1: Writing CSV to MinIO Temporary Path: {}", tmpMinioDirPath);

            long rowCount = expectedDF.count();
            logger.info("Dataset Contains {} Rows, Optimizing Write Strategy", rowCount);

            Dataset<Row> optimizedDF;
            if (rowCount > 500000) {
                int optimalPartitions = Math.max(20, (int) Math.ceil(rowCount / 25000));
                logger.info(
                        "Extremely Large Dataset Detected ({} Rows). Using {} Partitions For Maximum Performance",
                        rowCount, optimalPartitions);
                optimizedDF = expectedDF.repartition(optimalPartitions);
            } else if (rowCount > 100000) {
                int optimalPartitions = Math.max(10, (int) Math.ceil(rowCount / 50000));
                logger.info("Large Dataset Detected. Using {} Partitions For Optimal performance",
                        optimalPartitions);
                optimizedDF = expectedDF.repartition(optimalPartitions);
            } else {
                logger.info("Small Dataset Detected. Using Single Partition For Output");
                optimizedDF = expectedDF.coalesce(1);
            }

            long writeStartTime = System.currentTimeMillis();
            optimizedDF.write()
                    .option("header", "true")
                    .option("delimiter", ",")
                    .option("quote", "\"")
                    .option("escape", "\\")
                    .option("nullValue", "")
                    .option("compression", "none")
                    .mode(SaveMode.Overwrite)
                    .csv("s3a://" + bucketName + "/" + tmpMinioDirPath);

            long writeTime = System.currentTimeMillis() - writeStartTime;
            logger.info("CSV File Written Successfully to MinIO Temporary Path With Optimized Partitioning");
            logger.info("Write Operation Completed In {} ms For {} Rows ({} Rows/Sec)",
                    writeTime, rowCount, String.format("%.0f", (double) rowCount / (writeTime / 1000.0)));

            logger.info("Step 2: Creating MinIO S3 Client");

            AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey, minioSecretKey);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setSignerOverride("AWSS3V4SignerType");
            clientConfiguration.setConnectionTimeout(30000);
            clientConfiguration.setSocketTimeout(30000);

            s3client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new EndpointConfiguration(minioEndpointUrl, minioRegion))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();

            logger.info("MinIO S3 Client Created Successfully");

            logger.info("Step 3: Listing Files Under MinIO Temporary Path");
            ListObjectsV2Result result = s3client.listObjectsV2(bucketName, tmpMinioDirPath + "/");
            List<S3ObjectSummary> csvFiles = result.getObjectSummaries()
                    .stream()
                    .filter(obj -> obj.getKey().contains("part-") && obj.getKey().endsWith(".csv"))
                    .collect(Collectors.toList());

            if (csvFiles.isEmpty()) {
                throw new FileNotFoundException("No part CSV files found in: " + tmpMinioDirPath);
            }

            logger.info("Found {} Spark Part Files in MinIO", csvFiles.size());

            long totalFileSize = csvFiles.stream().mapToLong(S3ObjectSummary::getSize).sum();
            jobContext.setParameters("FILE_SIZE", String.valueOf(totalFileSize));
            logger.info("Total File Size: {} Bytes", totalFileSize);

            if (csvFiles.size() == 1) {
                logger.info("Step 4: Copying Single File to Final MinIO Path: {}", finalMinioPath);
                s3client.copyObject(bucketName, csvFiles.get(0).getKey(), bucketName, finalMinioPath);
                logger.info("Single CSV File Copied Successfully to Final Path");
            } else {
                logger.info("Step 4: Concatenating {} Files to Final MinIO Path: {}", csvFiles.size(), finalMinioPath);
                concatenateCSVFiles(s3client, bucketName, csvFiles, finalMinioPath);
                logger.info("Multiple CSV Files Concatenated Successfully to Final Path");
            }

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            logger.info("Step 5: Deleting Temporary Files from MinIO");
            for (S3ObjectSummary obj : result.getObjectSummaries()) {
                s3client.deleteObject(bucketName, obj.getKey());
            }
            logger.info("Temporary MinIO Objects Deleted Successfully");

            logger.info("CSV Report Uploaded Successfully!");
            logger.info("Final File Location: {}/{}", bucketName, finalMinioPath);

        } catch (Exception e) {
            logger.error("Error in Processing CSV Report, Message: {}, Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to Process CSV Report", e);
        } finally {
            if (s3client != null) {
                s3client.shutdown();
            }
        }
    }

    private static void concatenateCSVFiles(AmazonS3 s3client, String bucketName,
            List<S3ObjectSummary> csvFiles, String finalMinioPath) {
        try {
            logger.info("Starting CSV concatenation for {} files", csvFiles.size());
            File tempFile = File.createTempFile("concatenated_csv_", ".csv");
            tempFile.deleteOnExit();

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                boolean isFirstFile = true;

                for (S3ObjectSummary fileSummary : csvFiles) {
                    logger.info("Processing File: {}", fileSummary.getKey());

                    try (InputStream inputStream = s3client.getObject(bucketName, fileSummary.getKey())
                            .getObjectContent();
                            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

                        String line;
                        boolean isFirstLine = true;

                        while ((line = reader.readLine()) != null) {
                            if (isFirstFile && isFirstLine) {
                                writer.write(line);
                                writer.newLine();
                                isFirstLine = false;
                            } else if (!isFirstFile && isFirstLine) {
                                isFirstLine = false;
                                continue;
                            } else {
                                writer.write(line);
                                writer.newLine();
                            }
                        }
                    }

                    isFirstFile = false;
                }
            }

            logger.info("Uploading Concatenated File To: {}", finalMinioPath);
            s3client.putObject(bucketName, finalMinioPath, tempFile);

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Upload Concatenated File: " + finalMinioPath);
            }

            logger.info("CSV Concatenation Completed Successfully!");

        } catch (Exception e) {
            logger.error("Error During CSV Concatenation: {}", e.getMessage(), e);
            throw new RuntimeException("CSV Concatenation Failed", e);
        }
    }

    private static void processExcelReport(Dataset<Row> expectedDF, Map<String, String> extraParametersMap,
            JobContext jobContext, String reportFileName, Map<String, Map<String, String>> ragConfigurationMap) {

        String bucketName = SPARK_MINIO_BUCKET_NAME_PM;
        String minioEndpointUrl = SPARK_MINIO_ENDPOINT_URL;
        String minioAccessKey = SPARK_MINIO_ACCESS_KEY;
        String minioSecretKey = SPARK_MINIO_SECRET_KEY;
        String minioRegion = "us-east-1";

        String pushedDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String tmpDirPath = "protected/PM/FlowReport/tmp/FlowReport_" + UUID.randomUUID();
        String tmpMinioPath = "protected/PM/FlowReport/tmp/" + reportFileName;
        String finalMinioPath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

        String reportWidgetFilePath = reportFileName;
        String generatedReportFilePath = "/" + finalMinioPath;

        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);

        logger.info("REPORT_WIDGET_FILE_PATH={}, GENERATED_REPORT_FILE_PATH={} Set to Job Context Successfully!",
                generatedReportFilePath, reportWidgetFilePath);

        InputStream inputStream = null;
        AmazonS3 s3client = null;

        try {
            logger.info("Step 1: Creating Temporary Directory: {}", tmpDirPath);
            File tmpDir = new File(tmpDirPath);
            if (!tmpDir.exists()) {
                if (!tmpDir.mkdirs()) {
                    throw new RuntimeException("Failed to Create Temporary Directory: " + tmpDirPath);
                }
                logger.info("Temporary Directory Created Successfully");
            }

            logger.info("Step 2: Converting DataFrame to Excel");
            String excelFilePath = tmpDirPath + "/" + reportFileName;
            File excelFile = new File(excelFilePath);

            String frequency = jobContext.getParameters().get("frequency").toUpperCase();

            try (XSSFWorkbook workbook = new XSSFWorkbook()) {
                XSSFSheet sheet = workbook.createSheet(frequency);

                CellStyle headerStyle = workbook.createCellStyle();
                headerStyle.setFillForegroundColor(IndexedColors.ROYAL_BLUE.getIndex());
                headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                headerStyle.setAlignment(HorizontalAlignment.CENTER);
                headerStyle.setVerticalAlignment(VerticalAlignment.CENTER);

                Font headerFont = workbook.createFont();
                headerFont.setBold(true);
                headerFont.setColor(IndexedColors.WHITE.getIndex());
                headerStyle.setFont(headerFont);

                CellStyle dataStyle = workbook.createCellStyle();
                dataStyle.setAlignment(HorizontalAlignment.CENTER);
                dataStyle.setVerticalAlignment(VerticalAlignment.CENTER);
                dataStyle.setBorderTop(BorderStyle.THIN);
                dataStyle.setBorderBottom(BorderStyle.THIN);
                dataStyle.setBorderLeft(BorderStyle.THIN);
                dataStyle.setBorderRight(BorderStyle.THIN);
                dataStyle.setTopBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
                dataStyle.setBottomBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
                dataStyle.setLeftBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
                dataStyle.setRightBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());

                XSSFRow headerRow = sheet.createRow(0);
                String[] columns = expectedDF.columns();
                for (int i = 0; i < columns.length; i++) {
                    XSSFCell cell = headerRow.createCell(i);
                    cell.setCellValue(columns[i]);
                    cell.setCellStyle(headerStyle);
                }

                Map<String, Integer> kpiCodeToIndexMap = new HashMap<>();
                for (int i = 0; i < columns.length; i++) {
                    String colName = columns[i];
                    if (colName.equals("METRIC")) {
                        kpiCodeToIndexMap.put("METRIC_COLUMN", i);
                    }
                }

                Map<String, CellStyle> styleCache = new HashMap<>();
                Map<String, Font> fontCache = new HashMap<>();

                CellStyle baseDataStyle = workbook.createCellStyle();
                baseDataStyle.cloneStyleFrom(dataStyle);
                styleCache.put("BASE_DATA", baseDataStyle);

                Font whiteFont = workbook.createFont();
                whiteFont.setColor(IndexedColors.WHITE.getIndex());
                fontCache.put("WHITE_FONT", whiteFont);

                logger.info("[Style Cache Initialized to Prevent Excel Style Limit Exceeded]");

                List<org.apache.spark.sql.Row> rows = expectedDF.collectAsList();
                logger.info("Excel DF Size: {}", rows.size());

                for (int i = 0; i < rows.size(); i++) {
                    XSSFRow dataRow = sheet.createRow(i + 1);
                    org.apache.spark.sql.Row sparkRow = rows.get(i);

                    String metricValue = sparkRow.getAs("METRIC");
                    String kpiCode = null;
                    if (metricValue != null && metricValue.contains("-")) {
                        kpiCode = metricValue.split("-")[0];
                    }

                    for (int j = 0; j < columns.length; j++) {
                        XSSFCell cell = dataRow.createCell(j);
                        Object value = sparkRow.get(j);
                        String cellStr = (value != null) ? value.toString() : "";
                        logger.info("Cell[{},{}] - Column: {}, Value: {}", i + 1, j, columns[j], cellStr);

                        if (value == null) {
                            cell.setCellValue("");
                        } else if (value instanceof Number) {
                            cell.setCellValue(((Number) value).doubleValue());
                        } else if (value instanceof Boolean) {
                            cell.setCellValue((Boolean) value);
                        } else {
                            cell.setCellValue(cellStr);
                        }

                        CellStyle cellStyle = styleCache.get("BASE_DATA");
                        logger.info("Using Cached Base Style For Cell[{},{}]", i + 1, j);

                        if (kpiCode != null && isNumericColumn(columns[j], j, rows)) {
                            Map<String, String> kpiRagConfig = ragConfigurationMap.get(kpiCode);
                            if (kpiRagConfig != null) {
                                logger.info("Found RAG Configuration For KPI {} At column {}", kpiCode, j);
                                for (Map.Entry<String, String> condition : kpiRagConfig.entrySet()) {
                                    String expression = condition.getKey();
                                    String hexColor = condition.getValue();
                                    logger.info("Evaluating RAG condition: {} With Color {}", expression, hexColor);
                                    try {
                                        if (!cellStr.isEmpty() && !cellStr.equals("-")) {
                                            double val = Double.parseDouble(cellStr);

                                            boolean conditionMet = evaluateCondition(expression, val, kpiCode);
                                            logger.info("RAG evaluation result: {}", conditionMet);

                                            if (conditionMet) {

                                                String styleKey = "RAG_" + hexColor;
                                                CellStyle ragStyle = styleCache.get(styleKey);

                                                if (ragStyle == null) {
                                                    ragStyle = workbook.createCellStyle();
                                                    ragStyle.cloneStyleFrom(baseDataStyle);

                                                    XSSFColor fillColor = new XSSFColor(Color.decode(hexColor), null);
                                                    ((XSSFCellStyle) ragStyle).setFillForegroundColor(fillColor);
                                                    ragStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                                                    ragStyle.setIndention((short) 2);
                                                    ragStyle.setFont(fontCache.get("WHITE_FONT"));

                                                    styleCache.put(styleKey, ragStyle);
                                                    logger.info("Created And Cached New RAG Style For Color: {}",
                                                            hexColor);
                                                }

                                                cellStyle = ragStyle;
                                                logger.info("Applied Cached RAG color {} To Cell[{},{}]", hexColor,
                                                        i + 1, j);
                                            }
                                        } else {
                                            logger.info("Skipping RAG Evaluation For Empty Cell or Dash Value");
                                        }
                                    } catch (Exception ex) {
                                        logger.info("RAG Evaluation Failed For KPI {}: {}", kpiCode,
                                                ex.getMessage());
                                        logger.info(
                                                "Failed RAG Evaluation Details - Cell[{},{}], Value: {}, Expression: {}",
                                                i + 1, j, cellStr, expression, ex);
                                    }
                                }
                            }
                        }

                        cell.setCellStyle(cellStyle);
                        logger.info("Final Cell Style Applied To Cell[{},{}]", i + 1, j);
                    }
                }

                for (int i = 0; i < columns.length; i++) {
                    sheet.autoSizeColumn(i);
                    int currentWidth = sheet.getColumnWidth(i);
                    sheet.setColumnWidth(i, currentWidth + 1000);
                }

                sheet.createFreezePane(0, 1);
                sheet.setAutoFilter(new CellRangeAddress(0, rows.size(), 0, columns.length - 1));

                try (FileOutputStream fileOut = new FileOutputStream(excelFile)) {
                    workbook.write(fileOut);
                }
            }

            logger.info("Excel File Created Successfully At: {}", excelFilePath);

            logger.info("Step 3: Creating MinIO S3 Client");
            AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey, minioSecretKey);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setSignerOverride("AWSS3V4SignerType");
            clientConfiguration.setConnectionTimeout(30000);
            clientConfiguration.setSocketTimeout(30000);

            s3client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new EndpointConfiguration(minioEndpointUrl, minioRegion))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();

            logger.info("MinIO S3 Client Created Successfully");

            logger.info("Step 4: Uploading to Temporary MinIO Path: {}", tmpMinioPath);
            inputStream = new FileInputStream(excelFile);
            ObjectMetadata metadata = new ObjectMetadata();
            long excelFileSize = excelFile.length();
            metadata.setContentLength(excelFileSize);
            s3client.putObject(bucketName, tmpMinioPath, inputStream, metadata);
            logger.info("Excel File Uploaded Successfully to Temporary MinIO Path");

            String fileSize = String.valueOf(excelFileSize);
            jobContext.setParameters("FILE_SIZE", fileSize);
            logger.info("FILE_SIZE '{}' Set to Job Context Successfully!", fileSize);

            logger.info("Step 5: Copying to Final MinIO Path: {}", finalMinioPath);
            s3client.copyObject(bucketName, tmpMinioPath, bucketName, finalMinioPath);
            logger.info("Excel File Copied Successfully to Final Path");

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            logger.info("Step 6: Deleting Temporary Files from MinIO");
            s3client.deleteObject(bucketName, tmpMinioPath);
            logger.info("Temporary MinIO Objects Deleted Successfully");

            logger.info("Step 7: Cleaning Up Local Temporary Files");
            FileUtils.deleteDirectory(new File(tmpDirPath));
            logger.info("Local Temporary Files Deleted Successfully");

            logger.info("Excel Report Uploaded Successfully!");
            logger.info("Final file location: {}/{}", bucketName, finalMinioPath);

        } catch (Exception e) {
            logger.error("Error in Processing Excel Report, Message: {}, Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to Process Excel Report", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    logger.error("Error in Closing Input Stream, Message: {}, Error: {}", e.getMessage(), e);
                }
            }
            if (s3client != null) {
                s3client.shutdown();
            }
        }
    }

    private static String getReportFileName(Map<String, String> jobContextMap) {

        String reportName = jobContextMap.get("reportName");
        String level = jobContextMap.get("aggregationLevel");
        String domain = jobContextMap.get("domain");
        String vendor = jobContextMap.get("vendor");
        String frequency = jobContextMap.get("frequency");
        String reportType = "Trend";
        String reportFormat = jobContextMap.get("REPORT_FORMAT_TYPE");
        String epochTime = String.valueOf(System.currentTimeMillis());

        String reportFileName = reportName + "_" + domain + "_" + vendor + "_" + frequency
                + "_" + level + "_" + reportType + "_" + reportFormat + "_" + epochTime;

        String[] reportFileNameArray = reportFileName.split(" ");
        reportFileName = String.join("_", reportFileNameArray);

        reportFileName = reportFileName.toUpperCase();

        if (reportFormat.equals("excel")) {
            reportFileName += ".xlsx";
        } else if (reportFormat.equals("csv")) {
            reportFileName += ".csv";
        }
        return reportFileName;
    }

    private static boolean evaluateCondition(String expression, double value, String kpiCode) {
        String cleanExpr = expression.replace(" ", "").replace("KPI#" + kpiCode, String.valueOf(value));
        if (cleanExpr.startsWith("(") && cleanExpr.endsWith(")")) {
            cleanExpr = cleanExpr.substring(1, cleanExpr.length() - 1);
        }

        if (cleanExpr.contains("&&")) {
            String[] parts = cleanExpr.split("&&");
            boolean leftResult = evaluateSimpleCondition(parts[0]);
            boolean rightResult = evaluateSimpleCondition(parts[1]);
            return leftResult && rightResult;
        }

        if (cleanExpr.contains("||")) {
            String[] parts = cleanExpr.split("\\|\\|");
            boolean leftResult = evaluateSimpleCondition(parts[0]);
            boolean rightResult = evaluateSimpleCondition(parts[1]);
            return leftResult || rightResult;
        }
        return evaluateSimpleCondition(cleanExpr);
    }

    /**
     * Checks if a column should have RAG formatting applied.
     * Dynamically determines if a column contains numeric data by analyzing sample
     * values.
     * 
     * @param columnName  The name of the column to check
     * @param columnIndex The index of the column in the DataFrame
     * @param sampleRows  Sample rows to analyze column data types
     * @return true if the column should have RAG formatting, false otherwise
     */
    private static boolean isNumericColumn(String columnName, int columnIndex,
            List<org.apache.spark.sql.Row> sampleRows) {
        if (columnName == null || sampleRows == null || sampleRows.isEmpty()) {
            return false;
        }

        if ("METRIC".equalsIgnoreCase(columnName)) {
            return false;
        }

        int numericCount = 0;
        int totalCount = 0;
        int sampleSize = Math.min(sampleRows.size(), 100);

        for (int i = 0; i < sampleSize; i++) {
            try {
                org.apache.spark.sql.Row row = sampleRows.get(i);
                Object value = row.get(columnIndex);

                if (value != null) {
                    String strValue = value.toString().trim();

                    if (!strValue.isEmpty() && !strValue.equals("-") && !strValue.equalsIgnoreCase("null")) {
                        totalCount++;
                        try {
                            Double.parseDouble(strValue);
                            numericCount++;
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            } catch (Exception e) {
                continue;
            }
        }

        double numericPercentage = totalCount > 0 ? (double) numericCount / totalCount : 0.0;
        boolean isNumeric = numericPercentage >= 0.8;

        logger.info("Column '{}' Analysis: {}/{} Numeric Values ({:.1f}%) - RAG Enabled: {}",
                columnName, numericCount, totalCount, numericPercentage * 100, isNumeric);

        return isNumeric;
    }

    private static boolean evaluateSimpleCondition(String condition) {
        try {
            if (condition.contains(">=")) {
                String[] parts = condition.split(">=");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val >= threshold;
            } else if (condition.contains("<=")) {
                String[] parts = condition.split("<=");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val <= threshold;
            } else if (condition.contains(">")) {
                String[] parts = condition.split(">");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val > threshold;
            } else if (condition.contains("<")) {
                String[] parts = condition.split("<");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val < threshold;
            } else if (condition.contains("==")) {
                String[] parts = condition.split("==");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val == threshold;
            } else if (condition.contains("!=")) {
                String[] parts = condition.split("!=");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val != threshold;
            } else if (condition.contains("=")) {
                String[] parts = condition.split("=");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val == threshold;
            }
        } catch (Exception e) {
            logger.error("Error Evaluating RAG Condition: {} - {}", condition, e.getMessage());
        }
        return false;
    }
}
