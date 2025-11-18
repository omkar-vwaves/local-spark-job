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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;

import java.io.*;
import org.apache.poi.xssf.usermodel.XSSFRow;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;

public class WriteMetricReport extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(WriteMetricReport.class);
    private static final int MAX_ROWS_PER_SHEET = 50000;
    private static final int BATCH_SIZE_FOR_LOGGING = 50000;
    private static final int MEMORY_CHECK_INTERVAL = 100000;
    private static String SPARK_MINIO_ENDPOINT_URL = "SPARK_MINIO_ENDPOINT_URL";
    private static String SPARK_MINIO_ACCESS_KEY = "SPARK_MINIO_ACCESS_KEY";
    private static String SPARK_MINIO_SECRET_KEY = "SPARK_MINIO_SECRET_KEY";
    private static String SPARK_MINIO_BUCKET_NAME_PM = "SPARK_MINIO_BUCKET_NAME_PM";
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final AtomicLong peakMemoryUsage = new AtomicLong(0);

    public WriteMetricReport() {
        super();
    }

    public WriteMetricReport(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        if (dataframe != null) {
            long rowCount = dataframe.count();
            if (rowCount > 100000) {
                logger.info("Large DataFrame Detected ({} rows), Applying MEMORY_AND_DISK_SER Storage Level", rowCount);
                this.dataFrame = dataframe.persist(StorageLevel.MEMORY_AND_DISK_SER());
            }
        }
    }

    public WriteMetricReport(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        long executionStartTime = System.currentTimeMillis();
        initializePerformanceMonitoring();

        logger.info("=== WriteMetricReport Execution Started ===");
        logger.info("Timestamp: {}", new java.util.Date());
        logMemoryUsage("Initial Memory State");

        Map<String, String> jobContextMap = jobContext.getParameters();

        SPARK_MINIO_ENDPOINT_URL = jobContextMap.get("SPARK_MINIO_ENDPOINT_URL");
        SPARK_MINIO_ACCESS_KEY = jobContextMap.get("SPARK_MINIO_ACCESS_KEY");
        SPARK_MINIO_SECRET_KEY = jobContextMap.get("SPARK_MINIO_SECRET_KEY");
        SPARK_MINIO_BUCKET_NAME_PM = jobContextMap.get("SPARK_MINIO_BUCKET_NAME_PM");

        logger.info("MinIO Credentials: Endpoint={}, AccessKey={}, SecretKey={}, BucketName={}",
                SPARK_MINIO_ENDPOINT_URL, SPARK_MINIO_ACCESS_KEY, SPARK_MINIO_SECRET_KEY, SPARK_MINIO_BUCKET_NAME_PM);

        String extraParameters = jobContextMap.get("EXTRA_PARAMETERS");
        String reportWidgetDetails = jobContextMap.get("REPORT_WIDGET_DETAILS");
        String kpiGroupMapJson = jobContextMap.get("KPI_GROUP_MAP");

        if (reportWidgetDetails == null) {
            throw new Exception(
                    "REPORT_WIDGET_DETAILS is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        if (extraParameters == null) {
            throw new Exception(
                    "EXTRA_PARAMETERS is NULL. Please Ensure Extra Parameters is Properly Initialized Before Processing.");
        }

        if (kpiGroupMapJson == null) {
            throw new Exception(
                    "KPI_GROUP_MAP is NULL. Please Ensure Input DataFrame is Properly Initialized Before Processing.");
        }

        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extraParameters,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> reportWidgetDetailsMap = new ObjectMapper().readValue(reportWidgetDetails,
                new TypeReference<Map<String, String>>() {
                });

        String reportWidgetIdPk = reportWidgetDetailsMap.get("reportWidgetIdPk");
        jobContext.setParameters("REPORT_WIDGET_ID_PK", reportWidgetIdPk);
        logger.info("REPORT_WIDGET_ID_PK '{}' Set to Job Context Successfully!", reportWidgetIdPk);

        String generatedReportId = reportWidgetDetailsMap.get("generatedReportId");
        jobContext.setParameters("GENERATED_REPORT_ID", generatedReportId);
        logger.info("GENERATED_REPORT_ID '{}' Set to Job Context Successfully!", generatedReportId);

        String reportFileName = getReportFileName(jobContextMap);
        logger.info("Generated Report File Name: {}", reportFileName);

        Dataset<Row> expectedDF = this.dataFrame;

        String reportFormatType = extraParametersMap.get("reportFormatType");

        if (reportFormatType == null) {
            reportFormatType = "csv";
        }

        logger.info("Report Format Type: {}", reportFormatType);

        if (reportFormatType.equalsIgnoreCase("csv")) {
            long csvStartTime = System.currentTimeMillis();
            processCSVReport(expectedDF, extraParametersMap, jobContext, reportFileName);
            recordStepTime("CSV Report Processing", csvStartTime);
        } else if (reportFormatType.equalsIgnoreCase("excel")) {
            long excelStartTime = System.currentTimeMillis();
            processExcelReport(expectedDF, extraParametersMap, jobContext, reportFileName);
            recordStepTime("Excel Report Processing", excelStartTime);
        } else {
            long csvStartTime = System.currentTimeMillis();
            processCSVReport(expectedDF, extraParametersMap, jobContext, reportFileName);
            recordStepTime("CSV Report Processing", csvStartTime);
        }

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - executionStartTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logMemoryUsage("Final Memory State");
        logPerformanceSummary(executionStartTime, endTime, totalProcessingTime.get());

        logger.info("=== WriteMetricReport Execution Completed ===");
        logger.info("Total Execution Time: {} Minutes | {} Seconds", minutes, seconds);
        logger.info("Total Processing Time: {} ms", totalProcessingTime.get());
        logger.info("Peak Memory Usage: {} MB", peakMemoryUsage.get() / (1024 * 1024));

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
        String tmpMinioDirPath = "protected/PM/FlowReport/tmp/" + UUID.randomUUID(); // Spark Will Write Here
        String finalMinioPath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

        String reportWidgetFilePath = reportFileName;
        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);
        logger.info("REPORT_WIDGET_FILE_PATH '{}' Set to Job Context Successfully!", reportWidgetFilePath);

        String generatedReportFilePath = "/" + finalMinioPath;
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);
        logger.info("GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully!", generatedReportFilePath);

        AmazonS3 s3client = null;

        try {
            long stepStartTime = System.currentTimeMillis();

            logger.info("Step 1: Writing CSV to MinIO Temporary Path: {}", tmpMinioDirPath);
            logger.info("DataFrame Partitions: {}, Estimated Rows: {}",
                    expectedDF.rdd().getNumPartitions(), expectedDF.count());

            Dataset<Row> optimizedDF = expectedDF;
            if (expectedDF.rdd().getNumPartitions() > 10) {
                logger.info("Optimizing Partition Count For Large Dataset");
                optimizedDF = expectedDF.coalesce(1);
            }

            optimizedDF.write()
                    .option("header", "true")
                    .option("delimiter", ",")
                    .option("quote", "\"")
                    .option("escape", "\\")
                    .option("nullValue", "")
                    .mode(SaveMode.Overwrite)
                    .csv("s3a://" + bucketName + "/" + tmpMinioDirPath);

            long csvWriteTime = System.currentTimeMillis() - stepStartTime;
            logger.info("CSV Writing Completed In {} ms", csvWriteTime);

            logger.info("CSV File Written Successfully to MinIO Temporary Path");

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
            S3ObjectSummary targetFile = result.getObjectSummaries()
                    .stream()
                    .filter(obj -> obj.getKey().contains("part-") && obj.getKey().endsWith(".csv"))
                    .findFirst()
                    .orElseThrow(() -> new FileNotFoundException("No part CSV file found in: " + tmpMinioDirPath));

            logger.info("Found Spark Part File in MinIO: {}", targetFile.getKey());

            String fileSize = String.valueOf(targetFile.getSize());
            jobContext.setParameters("FILE_SIZE", fileSize);
            logger.info("FILE_SIZE '{}' Set to Job Context Successfully!", fileSize);

            logger.info("Step 4: Copying to Final MinIO Path: {}", finalMinioPath);
            s3client.copyObject(bucketName, targetFile.getKey(), bucketName, finalMinioPath);
            logger.info("CSV File Copied Successfully to Final Path");

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            logger.info("Step 5: Deleting Temporary Files from MinIO");
            for (S3ObjectSummary obj : result.getObjectSummaries()) {
                s3client.deleteObject(bucketName, obj.getKey());
            }
            logger.info("Temporary MinIO Objects Deleted Successfully");

            logger.info("CSV Report Uploaded Successfully!");
            logger.info("Final file location: {}/{}", bucketName, finalMinioPath);

        } catch (Exception e) {
            logger.error("Error in Processing CSV Report, Message: {}, Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to Process CSV Report", e);
        } finally {
            if (s3client != null) {
                s3client.shutdown();
            }
        }
    }

    private static CellStyle getGroupedHeaderStyle(XSSFWorkbook workbook) {
        CellStyle groupedHeaderStyle = workbook.createCellStyle();
        groupedHeaderStyle.setFillForegroundColor(IndexedColors.GREY_50_PERCENT.getIndex());
        groupedHeaderStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        groupedHeaderStyle.setAlignment(HorizontalAlignment.CENTER);
        groupedHeaderStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        groupedHeaderStyle.setBorderTop(BorderStyle.THIN);
        groupedHeaderStyle.setBorderBottom(BorderStyle.THIN);
        groupedHeaderStyle.setBorderLeft(BorderStyle.THIN);
        groupedHeaderStyle.setBorderRight(BorderStyle.THIN);
        groupedHeaderStyle.setTopBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
        groupedHeaderStyle.setBottomBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
        groupedHeaderStyle.setLeftBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
        groupedHeaderStyle.setRightBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());

        logger.info("Grouped Header Style Created Successfully!");
        return groupedHeaderStyle;
    }

    private static Font getGroupHeaderFont(XSSFWorkbook workbook) {
        Font groupHeaderFont = workbook.createFont();
        groupHeaderFont.setBold(true);
        groupHeaderFont.setColor(IndexedColors.WHITE.getIndex());
        groupHeaderFont.setFontHeightInPoints((short) 11);

        logger.info("Group Header Font Created Successfully!");
        return groupHeaderFont;
    }

    private static CellStyle getEmptyGroupHeaderStyle(XSSFWorkbook workbook) {
        CellStyle emptyGroupHeaderStyle = workbook.createCellStyle();
        emptyGroupHeaderStyle.setAlignment(HorizontalAlignment.CENTER);
        emptyGroupHeaderStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        emptyGroupHeaderStyle.setBorderTop(BorderStyle.THIN);
        emptyGroupHeaderStyle.setBorderBottom(BorderStyle.THIN);
        emptyGroupHeaderStyle.setBorderLeft(BorderStyle.THIN);
        emptyGroupHeaderStyle.setBorderRight(BorderStyle.THIN);
        emptyGroupHeaderStyle.setTopBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
        emptyGroupHeaderStyle.setBottomBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
        emptyGroupHeaderStyle.setLeftBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());
        emptyGroupHeaderStyle.setRightBorderColor(IndexedColors.GREY_25_PERCENT.getIndex());

        logger.info("Empty Group Header Style Created Successfully!");
        return emptyGroupHeaderStyle;
    }

    private static void processExcelReport(Dataset<Row> expectedDF, Map<String, String> extraParametersMap,
            JobContext jobContext, String reportFileName) {

        String ragConfigurationJson = jobContext.getParameters().get("RAG_CONFIGURATION");
        Map<String, Map<String, String>> ragConfigurationMap = new HashMap<>();
        try {
            ragConfigurationMap = new ObjectMapper().readValue(ragConfigurationJson,
                    new TypeReference<Map<String, Map<String, String>>>() {
                    });
            logger.info("MetricReport: RAG Configuration: {}", ragConfigurationMap);
        } catch (JsonMappingException e) {
            logger.info("Failed to Parse RAG Configuration JSON (JsonMappingException): {}", e.getMessage());
            ragConfigurationMap = new HashMap<>();
        } catch (JsonProcessingException e) {
            logger.info("Failed to Parse RAG Configuration JSON (JsonProcessingException): {}", e.getMessage());
            ragConfigurationMap = new HashMap<>();
        } catch (Exception e) {
            logger.info("Unexpected Error Parsing RAG Configuration JSON: {}", e.getMessage());
            ragConfigurationMap = new HashMap<>();
        }

        String kpiGroupMapJson = jobContext.getParameters().get("KPI_GROUP_MAP");
        Map<String, List<String>> kpiGroupMap = new HashMap<>();
        try {
            kpiGroupMap = new ObjectMapper().readValue(kpiGroupMapJson,
                    new TypeReference<Map<String, List<String>>>() {
                    });
            logger.info("MetricReport: KPI Group Map: {}", kpiGroupMap);
        } catch (JsonMappingException e) {
            logger.info("Failed to Parse KPI Group JSON (JsonMappingException): {}", e.getMessage());
            kpiGroupMap = new HashMap<>();
        } catch (JsonProcessingException e) {
            logger.info("Failed to Parse KPI Group JSON (JsonProcessingException): {}", e.getMessage());
            kpiGroupMap = new HashMap<>();
        } catch (Exception e) {
            logger.info("Unexpected Error Parsing KPI Group JSON: {}", e.getMessage());
            kpiGroupMap = new HashMap<>();
        }

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
        logger.info("REPORT_WIDGET_FILE_PATH '{}' Set to Job Context Successfully!", reportWidgetFilePath);

        String generatedReportFilePath = "/" + finalMinioPath;
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);
        logger.info("GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully!", generatedReportFilePath);

        InputStream inputStream = null;
        AmazonS3 s3client = null;

        try {
            logger.info("Step 1: Creating Temporary Directory: {}", tmpDirPath);
            File tmpDir = new File(tmpDirPath);
            if (!tmpDir.exists()) {
                if (!tmpDir.mkdirs()) {
                    throw new RuntimeException("Failed to create temporary directory: " + tmpDirPath);
                }
                logger.info("Temporary Directory Created Successfully");
            }

            logger.info("Step 2: Converting DataFrame to Excel");
            // logger.info("DataFrame Partitions: {}, Estimated Rows: {}",
            //         expectedDF.rdd().getNumPartitions(), expectedDF.count());

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

                Map<String, CellStyle> ragStyleCache = new HashMap<>();
                Font whiteFont = workbook.createFont();
                whiteFont.setColor(IndexedColors.WHITE.getIndex());
                CellStyle groupedHeaderStyle = getGroupedHeaderStyle(workbook);
                Font groupHeaderFont = getGroupHeaderFont(workbook);
                groupedHeaderStyle.setFont(groupHeaderFont);
                CellStyle emptyGroupHeaderStyle = getEmptyGroupHeaderStyle(workbook);

                XSSFRow groupHeaderRow = null;
                XSSFRow headerRow;
                int dataStartRowIdx;
                String[] columns = expectedDF.columns();
                Map<String, Integer> kpiCodeToIndexMap = new HashMap<>();
                for (int i = 0; i < columns.length; i++) {
                    String colName = columns[i];
                    if (colName != null && colName.contains("-")) {
                        try {
                            String kpiCode = colName.split("-")[0];
                            if (kpiCode != null && !kpiCode.trim().isEmpty()) {
                                kpiCodeToIndexMap.put(kpiCode, i);
                                logger.info("Mapped KPI Code {} to Column Index {}", kpiCode, i);
                            }
                        } catch (Exception e) {
                            logger.info("Failed to Parse KPI Code From Column {}: {}", colName, e.getMessage());
                        }
                    }
                }

                Map<Integer, String> columnToGroupMap = new HashMap<>();
                Map<String, List<Integer>> groupToColumnsMap = new HashMap<>();

                if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                    logger.info("KPI Group Map Is Present And Not Empty, Creating Grouped Headers!");
                    groupHeaderRow = sheet.createRow(0);
                    headerRow = sheet.createRow(1);
                    dataStartRowIdx = 2;
                } else {
                    logger.info("No KPI Group Map Provided, Skipping Grouped Headers!");
                    headerRow = sheet.createRow(0);
                    dataStartRowIdx = 1;
                }

                if (groupHeaderRow != null) {
                    logger.info("Processing KPI Group Headers: {}", kpiGroupMap);

                    logger.info("Available Columns In DataFrame: {}", Arrays.toString(columns));
                    kpiGroupMap = kpiGroupMap != null ? kpiGroupMap : new HashMap<>();
                    for (Map.Entry<String, List<String>> groupEntry : kpiGroupMap.entrySet()) {
                        String groupName = groupEntry.getKey();
                        List<String> groupColumns = groupEntry.getValue();
                        logger.info("Processing Group: {} With Columns: {}", groupName, groupColumns);

                        for (String groupColumn : groupColumns) {
                            boolean found = false;
                            for (int i = 0; i < columns.length; i++) {
                                if (columns[i].equals(groupColumn)) {
                                    columnToGroupMap.put(i, groupName);
                                    logger.info("Mapped Column {} (Index {}) To Group {}", groupColumn, i, groupName);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                logger.info("Column {} From Group {} Not Found In DataFrame Columns", groupColumn,
                                        groupName);
                            }
                        }
                    }

                    logger.info("Final Column To Group Mapping: {}", columnToGroupMap);

                    for (Map.Entry<Integer, String> entry : columnToGroupMap.entrySet()) {
                        String groupName = entry.getValue();
                        Integer columnIndex = entry.getKey();
                        groupToColumnsMap.computeIfAbsent(groupName, k -> new ArrayList<>()).add(columnIndex);
                    }

                    logger.info("Group To Columns Mapping: {}", groupToColumnsMap);

                    for (Map.Entry<String, List<Integer>> groupEntry : groupToColumnsMap.entrySet()) {
                        String groupName = groupEntry.getKey();
                        List<Integer> columnIndices = groupEntry.getValue();

                        if (columnIndices.size() > 1) {
                            Collections.sort(columnIndices);
                            int startIndex = columnIndices.get(0);
                            int endIndex = columnIndices.get(columnIndices.size() - 1);

                            XSSFCell groupCell = groupHeaderRow.createCell(startIndex);
                            groupCell.setCellValue(groupName);
                            groupCell.setCellStyle(groupedHeaderStyle);

                            sheet.addMergedRegion(new CellRangeAddress(0, 0, startIndex, endIndex));
                            logger.info("Merged Group '{}' Header Cells From Column {} To {}", groupName, startIndex,
                                    endIndex);

                            for (int i = startIndex + 1; i <= endIndex; i++) {
                                XSSFCell emptyCell = groupHeaderRow.createCell(i);
                                emptyCell.setCellValue("");
                                emptyCell.setCellStyle(emptyGroupHeaderStyle);
                            }
                        } else {
                            int columnIndex = columnIndices.get(0);
                            XSSFCell groupCell = groupHeaderRow.createCell(columnIndex);
                            groupCell.setCellValue(groupName);
                            groupCell.setCellStyle(groupedHeaderStyle);
                            logger.info("Group '{}' Has Single Column At Index {}, No Merging Needed", groupName,
                                    columnIndex);
                        }
                    }

                    for (int i = 0; i < columns.length; i++) {
                        if (!columnToGroupMap.containsKey(i)) {
                            XSSFCell emptyCell = groupHeaderRow.createCell(i);
                            emptyCell.setCellValue("");
                            emptyCell.setCellStyle(emptyGroupHeaderStyle);
                            logger.info("No Group Found For Column {} (Index {}), Setting Empty Group Header",
                                    columns[i], i);
                        }
                    }

                    logger.info("=== FINAL GROUP HEADER STRUCTURE ===");
                    logger.info("Total Columns: {}", columns.length);
                    logger.info("Grouped Columns: {} (Total Groups: {})", columnToGroupMap.size(),
                            groupToColumnsMap.size());
                    logger.info("Non-Grouped Columns: {}", columns.length - columnToGroupMap.size());
                    logger.info("Groups: {}", groupToColumnsMap.keySet());
                    logger.info("=====================================");
                }

                for (int i = 0; i < columns.length; i++) {
                    XSSFCell cell = headerRow.createCell(i);
                    cell.setCellValue(columns[i]);
                    cell.setCellStyle(headerStyle);
                }

                logger.info("Starting Streaming Excel Data Processing with Multiple Sheets Support...");
                logger.info("Performance Settings: MAX_ROWS_PER_SHEET={}, BATCH_SIZE_FOR_LOGGING={}",
                        MAX_ROWS_PER_SHEET, BATCH_SIZE_FOR_LOGGING);

                long totalRowCount = 0;
                int currentSheetIndex = 0;
                XSSFSheet currentSheet = sheet;
                XSSFRow currentGroupHeaderRow = groupHeaderRow;
                XSSFRow currentHeaderRow = headerRow;

                long dataProcessingStartTime = System.currentTimeMillis();
                long lastMemoryCheckTime = dataProcessingStartTime;

                Iterator<org.apache.spark.sql.Row> rowIterator = expectedDF.toLocalIterator();

                while (rowIterator.hasNext()) {
                    org.apache.spark.sql.Row sparkRow = rowIterator.next();
                    long currentSheetRowCount = totalRowCount % MAX_ROWS_PER_SHEET;
                    if (currentSheetRowCount == 0 && totalRowCount > 0) {
                        currentSheetIndex++;
                        String sheetName = frequency + "_" + currentSheetIndex;
                        currentSheet = workbook.createSheet(sheetName);
                        logger.info("Created New Sheet: {} for Rows {} to {}", sheetName, totalRowCount + 1,
                                totalRowCount + MAX_ROWS_PER_SHEET);

                        for (int i = 0; i < columns.length; i++) {
                            int columnWidth = sheet.getColumnWidth(i);
                            currentSheet.setColumnWidth(i, columnWidth);
                        }

                        if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                            currentGroupHeaderRow = currentSheet.createRow(0);
                            currentHeaderRow = currentSheet.createRow(1);
                            dataStartRowIdx = 2;

                            currentGroupHeaderRow.setHeight(sheet.getRow(0).getHeight());
                            currentHeaderRow.setHeight(sheet.getRow(1).getHeight());

                            for (int i = 0; i < columns.length; i++) {
                                XSSFCell groupCell = currentGroupHeaderRow.createCell(i);
                                String groupName = columnToGroupMap.get(i);
                                if (groupName != null) {
                                    groupCell.setCellValue(groupName);
                                    groupCell.setCellStyle(groupedHeaderStyle);
                                } else {
                                    groupCell.setCellValue("");
                                    groupCell.setCellStyle(emptyGroupHeaderStyle);
                                }
                            }

                            for (Map.Entry<String, List<Integer>> groupEntry : groupToColumnsMap.entrySet()) {
                                List<Integer> columnIndices = groupEntry.getValue();
                                if (columnIndices.size() > 1) {
                                    Collections.sort(columnIndices);
                                    int startIndex = columnIndices.get(0);
                                    int endIndex = columnIndices.get(columnIndices.size() - 1);
                                    currentSheet.addMergedRegion(new CellRangeAddress(0, 0, startIndex, endIndex));
                                }
                            }
                        } else {
                            currentGroupHeaderRow = null;
                            currentHeaderRow = currentSheet.createRow(0);
                            dataStartRowIdx = 1;
                            currentHeaderRow.setHeight(sheet.getRow(0).getHeight());
                        }

                        for (int i = 0; i < columns.length; i++) {
                            XSSFCell cell = currentHeaderRow.createCell(i);
                            cell.setCellValue(columns[i]);
                            cell.setCellStyle(headerStyle);
                        }

                        if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                            currentSheet.createFreezePane(0, 2);
                        } else {
                            currentSheet.createFreezePane(0, 1);
                        }

                        if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                            currentSheet.setAutoFilter(new CellRangeAddress(1, 1, 0, columns.length - 1));
                        } else {
                            currentSheet.setAutoFilter(new CellRangeAddress(0, 0, 0, columns.length - 1));
                        }
                    }

                    XSSFRow dataRow = currentSheet.createRow((int) (currentSheetRowCount + dataStartRowIdx));

                    for (int j = 0; j < columns.length; j++) {
                        XSSFCell cell = dataRow.createCell(j);
                        Object value = sparkRow.get(j);
                        String cellStr = (value != null) ? value.toString() : "";

                        if (value == null) {
                            cell.setCellValue("");
                        } else if (value instanceof Number) {
                            cell.setCellValue(((Number) value).doubleValue());
                        } else if (value instanceof Boolean) {
                            cell.setCellValue((Boolean) value);
                        } else {
                            cell.setCellValue(cellStr);
                        }

                        CellStyle cellStyle = dataStyle;

                        String colName = columns[j];
                        if (colName != null && colName.contains("-") && ragConfigurationMap != null) {
                            try {
                                String kpiCode = colName.split("-")[0];
                                if (kpiCode != null && !kpiCode.trim().isEmpty()) {
                                    Map<String, String> kpiRagConfig = ragConfigurationMap.get(kpiCode);
                                    if (kpiRagConfig != null && !kpiRagConfig.isEmpty()) {
                                        for (Map.Entry<String, String> condition : kpiRagConfig.entrySet()) {
                                            String expression = condition.getKey();
                                            String hexColor = condition.getValue();
                                            try {
                                                if (!cellStr.isEmpty() && !cellStr.equals("-")) {
                                                    double val = Double.parseDouble(cellStr);
                                                    boolean conditionMet = evaluateCondition(expression, val, kpiCode);

                                                    if (conditionMet) {
                                                        // Use Cached Style Or Create New One
                                                        CellStyle ragStyle = ragStyleCache.get(hexColor);
                                                        if (ragStyle == null) {
                                                            ragStyle = workbook.createCellStyle();
                                                            ragStyle.cloneStyleFrom(dataStyle);
                                                            logger.info("Applying RAG color: {}", hexColor);
                                                            XSSFColor fillColor = new XSSFColor(
                                                                    java.awt.Color.decode(hexColor),
                                                                    null);
                                                            logger.info("Created XSSFColor: {}", fillColor);
                                                            ((XSSFCellStyle) ragStyle)
                                                                    .setFillForegroundColor(fillColor);
                                                            ragStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                                                            ragStyle.setIndention((short) 2);
                                                            ragStyle.setFont(whiteFont);
                                                            ragStyleCache.put(hexColor, ragStyle);
                                                        }
                                                        cellStyle = ragStyle;
                                                    }
                                                }
                                            } catch (Exception ex) {
                                                logger.info("Error evaluating RAG condition: {} for KPI {}: {}",
                                                        expression, kpiCode, ex.getMessage());
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                logger.info("Error processing RAG configuration for column {}: {}", colName,
                                        e.getMessage());
                            }
                        }

                        cell.setCellStyle(cellStyle);
                    }

                    totalRowCount++;

                    if (totalRowCount % BATCH_SIZE_FOR_LOGGING == 0) {
                        long currentTime = System.currentTimeMillis();
                        long elapsedTime = currentTime - dataProcessingStartTime;
                        double rowsPerSecond = (totalRowCount * 1000.0) / elapsedTime;

                        logger.info("Progress: {} Rows Processed in {} ms ({} Rows/Sec)",
                                totalRowCount, elapsedTime, String.format("%.2f", rowsPerSecond));

                        if (currentTime - lastMemoryCheckTime > MEMORY_CHECK_INTERVAL) {
                            logMemoryUsage("During Processing");
                            lastMemoryCheckTime = currentTime;
                        }
                    }
                }

                logger.info("Total Rows Processed: {}", totalRowCount);
                if (currentSheetIndex > 0) {
                    logger.info("Created {} Additional Sheets for Large Dataset", currentSheetIndex);
                }

                for (int i = 0; i < columns.length; i++) {
                    sheet.autoSizeColumn(i);
                    int currentWidth = sheet.getColumnWidth(i);
                    int finalWidth = currentWidth + 1000;
                    sheet.setColumnWidth(i, finalWidth);

                    for (int sheetIdx = 1; sheetIdx <= currentSheetIndex; sheetIdx++) {
                        XSSFSheet additionalSheet = workbook.getSheetAt(sheetIdx);
                        additionalSheet.setColumnWidth(i, finalWidth);
                    }
                }

                if (groupHeaderRow != null) {
                    sheet.createFreezePane(0, 2);
                    int rowsInFirstSheet = Math.min((int) totalRowCount, MAX_ROWS_PER_SHEET);
                    sheet.setAutoFilter(new CellRangeAddress(1, rowsInFirstSheet + 1, 0, columns.length - 1));

                    for (int sheetIdx = 1; sheetIdx <= currentSheetIndex; sheetIdx++) {
                        XSSFSheet additionalSheet = workbook.getSheetAt(sheetIdx);
                        int startRow = sheetIdx * MAX_ROWS_PER_SHEET;
                        int endRow = Math.min(startRow + MAX_ROWS_PER_SHEET - 1, (int) totalRowCount);
                        additionalSheet
                                .setAutoFilter(new CellRangeAddress(1, endRow - startRow + 1, 0, columns.length - 1));
                    }
                } else {
                    sheet.createFreezePane(0, 1);

                    int rowsInFirstSheet = Math.min((int) totalRowCount, MAX_ROWS_PER_SHEET);
                    sheet.setAutoFilter(new CellRangeAddress(0, rowsInFirstSheet, 0, columns.length - 1));

                    for (int sheetIdx = 1; sheetIdx <= currentSheetIndex; sheetIdx++) {
                        XSSFSheet additionalSheet = workbook.getSheetAt(sheetIdx);
                        int startRow = sheetIdx * MAX_ROWS_PER_SHEET;
                        int endRow = Math.min(startRow + MAX_ROWS_PER_SHEET - 1, (int) totalRowCount);
                        additionalSheet
                                .setAutoFilter(new CellRangeAddress(0, endRow - startRow, 0, columns.length - 1));
                    }
                }

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
        String reportType = "Metric";
        String reportFormat = jobContextMap.get("REPORT_FORMAT_TYPE");
        reportFormat = reportFormat == null ? "csv" : reportFormat;
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
            } else if (condition.contains("!=")) {
                String[] parts = condition.split("!=");
                double val = Double.parseDouble(parts[0]);
                double threshold = Double.parseDouble(parts[1]);
                return val != threshold;
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
            logger.error("Error Evaluating Condition: {} - {}", condition, e.getMessage());
        }
        return false;
    }

    private static void initializePerformanceMonitoring() {
        Runtime runtime = Runtime.getRuntime();
        long currentMemory = runtime.totalMemory() - runtime.freeMemory();

        logger.info("Performance Monitoring Initialized");
        logger.info("Initial Memory Usage: {} MB", currentMemory / (1024 * 1024));
        logger.info("Available Memory: {} MB", runtime.maxMemory() / (1024 * 1024));
    }

    private static void logMemoryUsage(String label) {
        Runtime runtime = Runtime.getRuntime();
        long currentMemory = runtime.totalMemory() - runtime.freeMemory();
        long usedMemoryMB = currentMemory / (1024 * 1024);
        long maxMemoryMB = runtime.maxMemory() / (1024 * 1024);
        long freeMemoryMB = runtime.freeMemory() / (1024 * 1024);

        logger.info("Memory Usage [{}]: Used={} MB, Free={} MB, Max={} MB",
                label, usedMemoryMB, freeMemoryMB, maxMemoryMB);
    }

    private static void logPerformanceSummary(long startTime, long endTime, long processingTime) {
        long totalTime = endTime - startTime;
        long overheadTime = totalTime - processingTime;

        logger.info("=== PERFORMANCE SUMMARY ===");
        logger.info("Total Execution Time: {} ms", totalTime);
        logger.info("Data Processing Time: {} ms", processingTime);
        logger.info("Overhead Time: {} ms", overheadTime);
        logger.info("Processing Efficiency: {:.2f}%", (double) processingTime / totalTime * 100);
        logger.info("==========================");
    }

    private static void recordStepTime(String stepName, long startTime) {
        long stepTime = System.currentTimeMillis() - startTime;
        logger.info("Step '{}' Completed In {} ms", stepName, stepTime);
    }

}
