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

import org.apache.spark.api.java.*;
import org.apache.spark.sql.functions;

import java.util.*;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.*;

import static org.apache.spark.sql.functions.*;
import org.apache.poi.xssf.usermodel.XSSFRow;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;

public class WriteExceptionReportWithRag extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(WriteExceptionReportWithRag.class);
    private static String SPARK_MINIO_ENDPOINT_URL = "SPARK_MINIO_ENDPOINT_URL";
    private static String SPARK_MINIO_ACCESS_KEY = "SPARK_MINIO_ACCESS_KEY";
    private static String SPARK_MINIO_SECRET_KEY = "SPARK_MINIO_SECRET_KEY";
    private static String SPARK_MINIO_BUCKET_NAME_PM = "SPARK_MINIO_BUCKET_NAME_PM";

    public WriteExceptionReportWithRag() {
        super();
        logger.info("WriteExceptionReportWithRag No Argument Constructor Called!");
    }

    public WriteExceptionReportWithRag(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.info("WriteExceptionReportWithRag Constructor Called with Input DataFrame With ID: {} and Processor Name: {}", id,
                processorName);
    }

    public WriteExceptionReportWithRag(Integer id, String processorName) {
        super(id, processorName);
        logger.info("WriteExceptionReportWithRag Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("[WriteExceptionReportWithRag] Execution Started!");

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
                // {1049={(KPI#1049 >= 500)=#8b572a}, 1059={(KPI#1059 = 787)=#d0021b},
                // 1051={(KPI#1051 >= 499 && KPI#1051 <= 501)=#4a90e2}}
        

        String reportWidgetIdPk = reportWidgetDetailsMap.get("reportWidgetIdPk");
        jobContext.setParameters("REPORT_WIDGET_ID_PK", reportWidgetIdPk);
        logger.info("REPORT_WIDGET_ID_PK '{}' Set to Job Context Successfully! ✅", reportWidgetIdPk);

        String generatedReportId = reportWidgetDetailsMap.get("generatedReportId");
        jobContext.setParameters("GENERATED_REPORT_ID", generatedReportId);
        logger.info("GENERATED_REPORT_ID '{}' Set to Job Context Successfully! ✅", generatedReportId);

        String reportFileName = getReportFileName(jobContextMap);
        logger.info("Report File Name: {}", reportFileName);

        Dataset<Row> expectedDF = this.dataFrame;

        String reportFormatType = extraParametersMap.get("reportFormatType");

        if (reportFormatType == null) {
            throw new Exception(
                    "REPORT_FORMAT_TYPE is NULL. Please Ensure Report Format Type is Properly Set.");
        }

        logger.info("Report Format Type: {}", reportFormatType);

        if (reportFormatType.equalsIgnoreCase("csv")) {
            processCSVReport(expectedDF, extraParametersMap, jobContext, reportFileName);
        } else if (reportFormatType.equalsIgnoreCase("excel")) {
            processExcelReport(expectedDF, extraParametersMap, jobContext, reportFileName, ragConfigurationMap);
        } else {
            throw new Exception("REPORT_FORMAT_TYPE is Invalid. Please Ensure Input Report Format is Properly Set.");
        }

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.info("[WriteExceptionReportWithRag] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes, seconds);

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
        String tmpMinioDirPath = "protected/PM/FlowReport/tmp/" + UUID.randomUUID(); // Spark will write here
        String finalMinioPath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

        String reportWidgetFilePath = reportFileName;
        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);
        logger.info("REPORT_WIDGET_FILE_PATH '{}' Set to Job Context Successfully! ✅", reportWidgetFilePath);

        String generatedReportFilePath = "/" + finalMinioPath;
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);
        logger.info("GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully! ✅", generatedReportFilePath);

        AmazonS3 s3client = null;

        try {
            logger.info("Step 1: Writing CSV to MinIO Temporary Path: {}", tmpMinioDirPath);
            expectedDF.coalesce(1)
                    .write()
                    .option("header", "true")
                    .option("delimiter", ",")
                    .option("quote", "\"")
                    .option("escape", "\\")
                    .option("nullValue", "")
                    .mode(SaveMode.Overwrite)
                    .csv("s3a://" + bucketName + "/" + tmpMinioDirPath);

            logger.info("📁 CSV File Written Successfully to MinIO Temporary Path");

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

            logger.info("📁 MinIO S3 Client Created Successfully");

            logger.info("Step 3: Listing Files Under MinIO Temporary Path");
            ListObjectsV2Result result = s3client.listObjectsV2(bucketName, tmpMinioDirPath + "/");
            S3ObjectSummary targetFile = result.getObjectSummaries()
                    .stream()
                    .filter(obj -> obj.getKey().contains("part-") && obj.getKey().endsWith(".csv"))
                    .findFirst()
                    .orElseThrow(() -> new FileNotFoundException("No part CSV file found in: " + tmpMinioDirPath));

            logger.info("📁 Found Spark Part File in MinIO: {}", targetFile.getKey());

            String fileSize = String.valueOf(targetFile.getSize());
            jobContext.setParameters("FILE_SIZE", fileSize);
            logger.info("FILE_SIZE '{}' Set to Job Context Successfully! ✅", fileSize);

            logger.info("Step 4: Copying to Final MinIO Path: {}", finalMinioPath);
            s3client.copyObject(bucketName, targetFile.getKey(), bucketName, finalMinioPath);
            logger.info("📁 CSV File Copied Successfully to Final Path");

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            logger.info("Step 5: Deleting Temporary Files from MinIO");
            for (S3ObjectSummary obj : result.getObjectSummaries()) {
                s3client.deleteObject(bucketName, obj.getKey());
            }
            logger.info("🧹 Temporary MinIO Objects Deleted Successfully");

            logger.info("CSV Report Uploaded Successfully! ✅");
            logger.info("Final file location: {}/{}", bucketName, finalMinioPath);

        } catch (Exception e) {
            logger.error("❌ Error in Processing CSV Report, Message: {}, Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to Process CSV Report", e);
        } finally {
            if (s3client != null) {
                s3client.shutdown();
            }
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
        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);
        logger.info("REPORT_WIDGET_FILE_PATH '{}' Set to Job Context Successfully! ✅", reportWidgetFilePath);

        String generatedReportFilePath = "/" + finalMinioPath;
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);
        logger.info("GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully! ✅", generatedReportFilePath);

        InputStream inputStream = null;
        AmazonS3 s3client = null;

        try {
            logger.info("Step 1: Creating Temporary Directory: {}", tmpDirPath);
            File tmpDir = new File(tmpDirPath);
            if (!tmpDir.exists()) {
                if (!tmpDir.mkdirs()) {
                    throw new RuntimeException("Failed to create temporary directory: " + tmpDirPath);
                }
                logger.info("📁 Temporary Directory Created Successfully");
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
                
                // Map KPI code to column index
                Map<String, Integer> kpiCodeToIndexMap = new HashMap<>();
                for (int i = 0; i < columns.length; i++) {
                    String colName = columns[i];
                    if (colName.contains("-")) {
                        String kpiCode = colName.split("-")[0];
                        kpiCodeToIndexMap.put(kpiCode, i);
                        logger.info("Mapped KPI code {} to column index {}", kpiCode, i);
                    }
                }

                for (int i = 0; i < columns.length; i++) {
                    XSSFCell cell = headerRow.createCell(i);
                    cell.setCellValue(columns[i]);
                    cell.setCellStyle(headerStyle);
                }

                List<org.apache.spark.sql.Row> rows = expectedDF.collectAsList();
                for (int i = 0; i < rows.size(); i++) {
                    XSSFRow dataRow = sheet.createRow(i + 1);
                    org.apache.spark.sql.Row sparkRow = rows.get(i);
                    logger.info("Processing row {} of {}", i + 1, rows.size());

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

                        CellStyle cellStyle = workbook.createCellStyle();
                        cellStyle.cloneStyleFrom(dataStyle);
                        logger.info("Created base cell style for cell[{},{}]", i + 1, j);

                        // RAG logic - only apply to columns that match KPI codes
                        String colName = columns[j];
                        if (colName.contains("-")) {
                            String kpiCode = colName.split("-")[0];
                            Map<String, String> kpiRagConfig = ragConfigurationMap.get(kpiCode);
                            if (kpiRagConfig != null) {
                                logger.info("Found RAG configuration for KPI {} at column {}", kpiCode, j);
                                for (Map.Entry<String, String> condition : kpiRagConfig.entrySet()) {
                                    String expression = condition.getKey();
                                    String hexColor = condition.getValue();
                                    logger.info("Evaluating RAG condition: {} with color {}", expression, hexColor);
                                    try {
                                        if (!cellStr.isEmpty() && !cellStr.equals("-")) {
                                            double val = Double.parseDouble(cellStr);
                                            
                                            // Parse and evaluate the condition
                                            boolean conditionMet = evaluateCondition(expression, val, kpiCode);
                                            logger.info("RAG evaluation result: {}", conditionMet);
                                            
                                            if (conditionMet) {
                                                XSSFColor fillColor = new XSSFColor(java.awt.Color.decode(hexColor), null);
                                                ((XSSFCellStyle) cellStyle).setFillForegroundColor(fillColor);
                                                cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                                                
                                                // Add padding to create oval effect
                                                cellStyle.setIndention((short) 2);
                                                
                                                // Set font color to white for better contrast
                                                Font cellFont = workbook.createFont();
                                                cellFont.setColor(IndexedColors.WHITE.getIndex());
                                                cellStyle.setFont(cellFont);
                                                
                                                logger.info("Applied RAG color {} as background to cell[{},{}]", hexColor, i + 1, j);
                                            }
                                        } else {
                                            logger.info("Skipping RAG evaluation for empty cell or dash value");
                                        }
                                    } catch (Exception ex) {
                                        logger.info("⚠️ RAG evaluation failed for KPI {}: {}", kpiCode, ex.getMessage());
                                        logger.info("Failed RAG evaluation details - Cell[{},{}], Value: {}, Expression: {}", 
                                            i + 1, j, cellStr, expression, ex);
                                    }
                                }
                            }
                        }

                        cell.setCellStyle(cellStyle);
                        logger.info("Final cell style applied to cell[{},{}]", i + 1, j);
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

            logger.info("📊 Excel File Created Successfully at: {}", excelFilePath);

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

            logger.info("📁 MinIO S3 Client Created Successfully");

            logger.info("Step 4: Uploading to Temporary MinIO Path: {}", tmpMinioPath);
            inputStream = new FileInputStream(excelFile);
            ObjectMetadata metadata = new ObjectMetadata();
            long excelFileSize = excelFile.length();
            metadata.setContentLength(excelFileSize);
            s3client.putObject(bucketName, tmpMinioPath, inputStream, metadata);
            logger.info("📁 Excel File Uploaded Successfully to Temporary MinIO Path");

            String fileSize = String.valueOf(excelFileSize);
            jobContext.setParameters("FILE_SIZE", fileSize);
            logger.info("FILE_SIZE '{}' Set to Job Context Successfully! ✅", fileSize);

            logger.info("Step 5: Copying to Final MinIO Path: {}", finalMinioPath);
            s3client.copyObject(bucketName, tmpMinioPath, bucketName, finalMinioPath);
            logger.info("📁 Excel File Copied Successfully to Final Path");

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            logger.info("Step 6: Deleting Temporary Files from MinIO");
            s3client.deleteObject(bucketName, tmpMinioPath);
            logger.info("🧹 Temporary MinIO Objects Deleted Successfully");

            logger.info("Step 7: Cleaning Up Local Temporary Files");
            FileUtils.deleteDirectory(new File(tmpDirPath));
            logger.info("🧹 Local Temporary Files Deleted Successfully");

            logger.info("Excel Report Uploaded Successfully! ✅");
            logger.info("Final file location: {}/{}", bucketName, finalMinioPath);

        } catch (Exception e) {
            logger.error("❌ Error in Processing Excel Report, Message: {}, Error: {}", e.getMessage(), e);
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
        String reportType = "Metric";
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
        // Remove spaces and replace KPI#code with the actual value
        String cleanExpr = expression.replace(" ", "").replace("KPI#" + kpiCode, String.valueOf(value));
        
        // Remove outer parentheses if they exist
        if (cleanExpr.startsWith("(") && cleanExpr.endsWith(")")) {
            cleanExpr = cleanExpr.substring(1, cleanExpr.length() - 1);
        }
        
        // Handle compound conditions with &&
        if (cleanExpr.contains("&&")) {
            String[] parts = cleanExpr.split("&&");
            boolean leftResult = evaluateSimpleCondition(parts[0]);
            boolean rightResult = evaluateSimpleCondition(parts[1]);
            return leftResult && rightResult;
        }
        
        // Handle compound conditions with ||
        if (cleanExpr.contains("||")) {
            String[] parts = cleanExpr.split("\\|\\|");
            boolean leftResult = evaluateSimpleCondition(parts[0]);
            boolean rightResult = evaluateSimpleCondition(parts[1]);
            return leftResult || rightResult;
        }
        
        // Handle simple condition
        return evaluateSimpleCondition(cleanExpr);
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
            } else if (condition.contains("=")) {
                String[] parts = condition.split("=");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val == threshold;
            }
        } catch (Exception e) {
            logger.error("Error evaluating condition: {} - {}", condition, e.getMessage());
        }
        return false;
    }

}
