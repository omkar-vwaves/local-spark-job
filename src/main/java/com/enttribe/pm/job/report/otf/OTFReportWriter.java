package com.enttribe.pm.job.report.otf;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.enttribe.sparkrunner.context.JobContext;

import com.enttribe.sparkrunner.processors.Processor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import org.apache.commons.io.FileUtils;

import java.io.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.xssf.streaming.*;

public class OTFReportWriter extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(OTFReportWriter.class);
    private static String SPARK_MINIO_ENDPOINT_URL = "SPARK_MINIO_ENDPOINT_URL";
    private static String SPARK_MINIO_ACCESS_KEY = "SPARK_MINIO_ACCESS_KEY";
    private static String SPARK_MINIO_SECRET_KEY = "SPARK_MINIO_SECRET_KEY";
    private static String SPARK_MINIO_BUCKET_NAME_PM = "SPARK_MINIO_BUCKET_NAME_PM";
    private static final boolean IS_LOG_ENABLED = false;

    public OTFReportWriter() {
        super();
    }

    public OTFReportWriter(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
    }

    public OTFReportWriter(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        if (this.dataFrame != null) {
            this.dataFrame.show(5);
            logger.info("++++++++++[FINAL GENERATED REPORT]++++++++++");
        }
        if (IS_LOG_ENABLED) {
            logger.info("[OTFReportWriter] Execution Started!");
        }

        long startTime = System.currentTimeMillis();

        Map<String, String> jobContextMap = jobContext.getParameters();

        SPARK_MINIO_ENDPOINT_URL = jobContextMap.get("SPARK_MINIO_ENDPOINT_URL");
        SPARK_MINIO_ACCESS_KEY = jobContextMap.get("SPARK_MINIO_ACCESS_KEY");
        SPARK_MINIO_SECRET_KEY = jobContextMap.get("SPARK_MINIO_SECRET_KEY");
        SPARK_MINIO_BUCKET_NAME_PM = jobContextMap.get("SPARK_MINIO_BUCKET_NAME_PM");

        if (IS_LOG_ENABLED) {
            logger.info("MinIO Credentials: Endpoint={}, AccessKey={}, SecretKey={}, BucketName={}",
                    SPARK_MINIO_ENDPOINT_URL, SPARK_MINIO_ACCESS_KEY, SPARK_MINIO_SECRET_KEY,
                    SPARK_MINIO_BUCKET_NAME_PM);
        }

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

        Map<String, String> extraParametersMap = new ObjectMapper().readValue(extraParameters,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, String> reportWidgetDetailsMap = new ObjectMapper().readValue(reportWidgetDetails,
                new TypeReference<Map<String, String>>() {
                });

        Map<String, Map<String, String>> ragConfigurationMap = new ObjectMapper().readValue(ragConfiguration,
                new TypeReference<Map<String, Map<String, String>>>() {
                });

        if (IS_LOG_ENABLED) {
            logger.info("RAG Configuration: {}", ragConfigurationMap);
        }

        String reportWidgetIdPk = reportWidgetDetailsMap.get("REPORT_WIDGET_ID_PK");
        jobContext.setParameters("REPORT_WIDGET_ID_PK", reportWidgetIdPk);

        String generatedReportId = reportWidgetDetailsMap.get("GENERATED_REPORT_ID");
        jobContext.setParameters("GENERATED_REPORT_ID", generatedReportId);

        if (IS_LOG_ENABLED) {
            logger.info("REPORT_WIDGET_ID_PK '{}' Set to Job Context Successfully!", reportWidgetIdPk);
            logger.info("GENERATED_REPORT_ID '{}' Set to Job Context Successfully!", generatedReportId);
        }

        String reportFileName = getReportFileName(jobContextMap, extraParametersMap, reportWidgetDetailsMap);
        if (IS_LOG_ENABLED) {
            logger.info("Report File Name: {}", reportFileName);
        }

        Dataset<Row> expectedDF = this.dataFrame;

        String reportFormatType = extraParametersMap.get("REPORT_FORMAT_TYPE");

        if (reportFormatType == null) {
            throw new Exception(
                    "REPORT_FORMAT_TYPE is NULL. Please Ensure Report Format Type is Properly Set.");
        }

        if (IS_LOG_ENABLED) {
            logger.info("Report Format Type: {}", reportFormatType);
        }

        if (reportFormatType.equalsIgnoreCase("csv")) {
            processCSVReport(expectedDF, extraParametersMap, jobContext, reportFileName);
        } else if (reportFormatType.equalsIgnoreCase("excel")) {
            processExcelReport(expectedDF, extraParametersMap, jobContext, reportFileName, ragConfigurationMap);
        } else {
            throw new Exception("REPORT_FORMAT_TYPE is Invalid. Please Ensure Input Report Format is Properly Set.");
        }

        if (IS_LOG_ENABLED) {
            long endTime = System.currentTimeMillis();
            long durationMillis = endTime - startTime;
            long minutes = durationMillis / 60000;
            long seconds = (durationMillis % 60000) / 1000;
            logger.info("[OTFReportWriter] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes, seconds);
        }

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
        String tmpDirPath = "protected/PM/FlowReport/tmp/CSV_" + UUID.randomUUID();
        String tmpMinioPath = "protected/PM/FlowReport/tmp/" + reportFileName;
        String finalMinioPath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

        String reportWidgetFilePath = reportFileName;
        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);

        String generatedReportFilePath = "/" + finalMinioPath;
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);

        if (IS_LOG_ENABLED) {
            logger.info("REPORT_WIDGET_FILE_PATH '{}' Set to Job Context Successfully!", reportWidgetFilePath);
            logger.info("GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully!", generatedReportFilePath);

        }
        AmazonS3 s3client = null;
        BufferedWriter writer = null;
        File csvFile = null;

        try {
            if (IS_LOG_ENABLED) {
                logger.info("Step 1: Creating Temporary Directory: {}", tmpDirPath);
            }
            File tmpDir = new File(tmpDirPath);
            if (!tmpDir.exists()) {
                if (!tmpDir.mkdirs()) {
                    throw new RuntimeException("Failed to create temporary directory: " + tmpDirPath);
                }
                if (IS_LOG_ENABLED) {
                    logger.info("Temporary Directory Created Successfully");
                }
            }

            if (IS_LOG_ENABLED) {
                logger.info("Step 2: Writing CSV using Streaming Approach");
            }
            csvFile = new File(tmpDirPath + "/" + reportFileName);
            writer = new BufferedWriter(new FileWriter(csvFile, false), 8192); // 8KB buffer

            String[] columns = expectedDF.columns();
            if (columns == null || columns.length == 0) {
                throw new RuntimeException("DataFrame Has No Columns");
            }

            if (IS_LOG_ENABLED) {
                logger.info("Processing DataFrame With {} Columns", columns.length);
            }

            // Write CSV header
            StringBuilder header = new StringBuilder();
            for (int i = 0; i < columns.length; i++) {
                if (i > 0)
                    header.append(",");
                header.append(escapeCsvField(columns[i]));
            }
            writer.write(header.toString());
            writer.newLine();

            // STREAMING DATA PROCESSING - Use toLocalIterator() instead of collectAsList()
            if (IS_LOG_ENABLED) {
                logger.info("Starting Streaming CSV Data Processing...");
            }
            long rowCount = 0;
            Iterator<Row> rowIterator = expectedDF.toLocalIterator();

            while (rowIterator.hasNext()) {
                Row sparkRow = rowIterator.next();

                StringBuilder rowBuilder = new StringBuilder();
                for (int j = 0; j < columns.length; j++) {
                    if (j > 0)
                        rowBuilder.append(",");

                    Object value = sparkRow.get(j);
                    if (value == null) {
                        rowBuilder.append(""); // Empty field
                    } else {
                        rowBuilder.append(escapeCsvField(value.toString()));
                    }
                }

                writer.write(rowBuilder.toString());
                writer.newLine();

                rowCount++;
                if (rowCount % 100000 == 0) {
                    if (IS_LOG_ENABLED) {
                        logger.info("Processed {} rows...", rowCount);
                    }
                    writer.flush(); // Flush buffer periodically
                }
            }

            writer.flush(); // Final flush
            if (IS_LOG_ENABLED) {
                logger.info("Total rows processed: {}", rowCount);
                logger.info("📄 CSV File Created Successfully At: {}", csvFile.getAbsolutePath());
                logger.info("Step 3: Creating MinIO S3 Client");
            }

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

            if (IS_LOG_ENABLED) {
                logger.info("MinIO S3 Client Created Successfully");
                logger.info("Step 4: Uploading to Temporary MinIO Path: {}", tmpMinioPath);
            }
            try (InputStream inputStream = new FileInputStream(csvFile)) {
                ObjectMetadata metadata = new ObjectMetadata();
                long csvFileSize = csvFile.length();
                metadata.setContentLength(csvFileSize);
                s3client.putObject(bucketName, tmpMinioPath, inputStream, metadata);

                String fileSize = String.valueOf(csvFileSize);
                jobContext.setParameters("FILE_SIZE", fileSize);

                if (IS_LOG_ENABLED) {
                    logger.info("CSV File Uploaded Successfully to Temporary MinIO Path");
                    logger.info("FILE_SIZE '{}' Set to Job Context Successfully!", fileSize);
                }
            }

            if (IS_LOG_ENABLED) {
                logger.info("Step 5: Copying to Final MinIO Path: {}", finalMinioPath);
            }
            s3client.copyObject(bucketName, tmpMinioPath, bucketName, finalMinioPath);
            if (IS_LOG_ENABLED) {
                logger.info("CSV File Copied Successfully to Final Path");
            }

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            if (IS_LOG_ENABLED) {
                logger.info("Step 6: Deleting Temporary Files from MinIO");
            }
            s3client.deleteObject(bucketName, tmpMinioPath);
            if (IS_LOG_ENABLED) {
                logger.info("Temporary MinIO Objects Deleted Successfully");
            }

            if (IS_LOG_ENABLED) {
                logger.info("Step 7: Cleaning Up Local Temporary Files");
            }
            FileUtils.deleteDirectory(new File(tmpDirPath));
            if (IS_LOG_ENABLED) {
                logger.info("Local Temporary Files Deleted Successfully");
            }

            if (IS_LOG_ENABLED) {
                logger.info("CSV Report Uploaded Successfully!");
                logger.error("Final file location: {}/{}", bucketName, finalMinioPath);
            }

        } catch (Exception e) {
            logger.error("Error in Processing CSV Report, Message: {}, Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to Process CSV Report", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.error("Error in Closing CSV Writer, Message: {}, Error: {}", e.getMessage(), e);
                }
            }
            if (s3client != null) {
                s3client.shutdown();
            }
        }
    }

    /**
     * Escape CSV field value to ensure proper CSV formatting
     * Handles commas, quotes, and newlines in field values
     */
    private static String escapeCsvField(String field) {
        if (field == null) {
            return "";
        }

        // Check if field contains comma, quote, or newline
        if (field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r")) {
            // Escape quotes by doubling them and wrap in quotes
            String escaped = field.replace("\"", "\"\"");
            return "\"" + escaped + "\"";
        }

        return field;
    }

    private static Font getHeaderFont(Workbook workbook) {
        Font headerFont = workbook.createFont();
        headerFont.setBold(true);
        headerFont.setColor(IndexedColors.WHITE.getIndex());
        headerFont.setFontHeightInPoints((short) 11);
        return headerFont;
    }

    private static CellStyle getHeaderStyle(Workbook workbook, Font headerFont) {

        CellStyle headerStyle = workbook.createCellStyle();
        headerStyle.setFillForegroundColor(IndexedColors.ROYAL_BLUE.getIndex());
        headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerStyle.setAlignment(HorizontalAlignment.CENTER);
        headerStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        headerStyle.setBorderTop(BorderStyle.THIN);
        headerStyle.setBorderBottom(BorderStyle.THIN);
        headerStyle.setBorderLeft(BorderStyle.THIN);
        headerStyle.setBorderRight(BorderStyle.THIN);
        headerStyle.setTopBorderColor(IndexedColors.WHITE.getIndex());
        headerStyle.setBottomBorderColor(IndexedColors.WHITE.getIndex());
        headerStyle.setLeftBorderColor(IndexedColors.WHITE.getIndex());
        headerStyle.setRightBorderColor(IndexedColors.WHITE.getIndex());
        headerStyle.setFont(headerFont);
        return headerStyle;
    }

    private static CellStyle getDataStyle(Workbook workbook) {
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
        return dataStyle;
    }

    private static CellStyle getGroupedHeaderStyle(Workbook workbook) {
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
        return groupedHeaderStyle;
    }

    private static Font getGroupHeaderFont(Workbook workbook) {
        Font groupHeaderFont = workbook.createFont();
        groupHeaderFont.setBold(true);
        groupHeaderFont.setColor(IndexedColors.WHITE.getIndex());
        groupHeaderFont.setFontHeightInPoints((short) 11);
        return groupHeaderFont;
    }

    private static CellStyle getEmptyGroupHeaderStyle(Workbook workbook) {
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
        return emptyGroupHeaderStyle;
    }

    private static void processExcelReport(Dataset<Row> expectedDF, Map<String, String> extraParametersMap,
            JobContext jobContext, String reportFileName, Map<String, Map<String, String>> ragConfigurationMap) {

        if (extraParametersMap == null) {
            throw new RuntimeException("Extra Parameters Map Cannot Be NULL");
        }
        if (jobContext == null) {
            throw new RuntimeException("Job Context Cannot Be NULL");
        }
        if (reportFileName == null || reportFileName.trim().isEmpty()) {
            throw new RuntimeException("Report File Name Cannot Be NULL/Empty");
        }

        // Added For KPI Group Headers
        String kpiGroup = jobContext.getParameters().get("EXCEL_HEADER_GROUP");
        Map<String, List<String>> kpiGroupMap = new HashMap<>();

        if (kpiGroup != null && !kpiGroup.trim().isEmpty()) {
            try {
                kpiGroupMap = new ObjectMapper().readValue(kpiGroup,
                        new TypeReference<Map<String, List<String>>>() {
                        });
                if (IS_LOG_ENABLED) {
                    logger.info("OTFReportWriter KPI Group Map: {}", kpiGroupMap);
                }
            } catch (JsonMappingException e) {
                logger.error("Failed to Parse KPI Group JSON (JsonMappingException): {}", e.getMessage());
                kpiGroupMap = new HashMap<>();
            } catch (JsonProcessingException e) {
                logger.error("Failed to Parse KPI Group JSON (JsonProcessingException): {}", e.getMessage());
                kpiGroupMap = new HashMap<>();
            } catch (Exception e) {
                logger.error("Unexpected Error Parsing KPI Group JSON: {}", e.getMessage());
                kpiGroupMap = new HashMap<>();
            }
        } else {
            if (IS_LOG_ENABLED) {
                logger.info("No KPI Group Configuration Provided or Empty");
            }
        }

        String bucketName = SPARK_MINIO_BUCKET_NAME_PM;
        String minioEndpointUrl = SPARK_MINIO_ENDPOINT_URL;
        String minioAccessKey = SPARK_MINIO_ACCESS_KEY;
        String minioSecretKey = SPARK_MINIO_SECRET_KEY;
        String minioRegion = "us-east-1";

        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new RuntimeException("MinIO Bucket Name Cannot Be NULL/Empty");
        }
        if (minioEndpointUrl == null || minioEndpointUrl.trim().isEmpty()) {
            throw new RuntimeException("MinIO Endpoint URL Cannot Be NULL/Empty");
        }
        if (minioAccessKey == null || minioAccessKey.trim().isEmpty()) {
            throw new RuntimeException("MinIO Access Key Cannot Be NULL/Empty");
        }
        if (minioSecretKey == null || minioSecretKey.trim().isEmpty()) {
            throw new RuntimeException("MinIO Secret Key Cannot Be NULL/Empty");
        }

        String pushedDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String tmpDirPath = "protected/PM/FlowReport/tmp/FlowReport_" + UUID.randomUUID();
        String tmpMinioPath = "protected/PM/FlowReport/tmp/" + reportFileName;
        String finalMinioPath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

        String reportWidgetFilePath = reportFileName;
        jobContext.setParameters("REPORT_WIDGET_FILE_PATH", reportWidgetFilePath);

        String generatedReportFilePath = "/" + finalMinioPath;
        jobContext.setParameters("GENERATED_REPORT_FILE_PATH", generatedReportFilePath);
        if (IS_LOG_ENABLED) {
            logger.info("REPORT_WIDGET_FILE_PATH '{}' Set to Job Context Successfully!", reportWidgetFilePath);
            logger.info("GENERATED_REPORT_FILE_PATH '{}' Set to Job Context Successfully!", generatedReportFilePath);

        }
        InputStream inputStream = null;
        AmazonS3 s3client = null;
        SXSSFWorkbook workbook = null;
        FileOutputStream fileOut = null;

        try {
            if (IS_LOG_ENABLED) {
                logger.info("Step 1: Creating Temporary Directory: {}", tmpDirPath);
            }
            File tmpDir = new File(tmpDirPath);
            if (!tmpDir.exists()) {
                if (!tmpDir.mkdirs()) {
                    throw new RuntimeException("Failed to create temporary directory: " + tmpDirPath);
                }
                if (IS_LOG_ENABLED) {
                    logger.info("Temporary Directory Created Successfully");
                }
            }

            if (IS_LOG_ENABLED) {
                logger.info("Step 2: Converting DataFrame to Excel using Streaming Approach");
            }
            String excelFilePath = tmpDirPath + "/" + reportFileName;
            File excelFile = new File(excelFilePath);

            String frequency = jobContext.getParameters().get("FREQUENCY");
            if (frequency == null || frequency.trim().isEmpty()) {
                frequency = "DEFAULT";
                if (IS_LOG_ENABLED) {
                    logger.info("FREQUENCY parameter is null or empty, using default value: {}", frequency);
                }
            } else {
                frequency = frequency.toUpperCase();
            }

            // Create SXSSFWorkbook with row window for streaming
            workbook = new SXSSFWorkbook(1000); // Keep 1000 rows in memory
            workbook.setCompressTempFiles(true); // Compress temp files to save disk space

            SXSSFSheet sheet = workbook.createSheet(frequency);

            // Create Consistent Font For Both Header Types
            Font headerFont = getHeaderFont(workbook);
            CellStyle headerStyle = getHeaderStyle(workbook, headerFont);
            CellStyle dataStyle = getDataStyle(workbook);

            // Create Style Cache for RAG Colors - REUSE STYLES
            Map<String, CellStyle> ragStyleCache = new HashMap<>();
            Font whiteFont = workbook.createFont();
            whiteFont.setColor(IndexedColors.WHITE.getIndex());

            String[] columns = expectedDF.columns();
            if (columns == null || columns.length == 0) {
                throw new RuntimeException("DataFrame Has No Columns");
            }

            if (IS_LOG_ENABLED) {
                logger.info("Processing DataFrame With {} Columns", columns.length);
            }

            // Enable column tracking for auto-sizing in SXSSF
            List<Integer> autoSizeColumnIndices = new ArrayList<>();
            for (int i = 0; i < columns.length; i++) {
                autoSizeColumnIndices.add(i);
            }
            sheet.trackColumnsForAutoSizing(autoSizeColumnIndices);

            // Map KPI Code to Column Index
            Map<String, Integer> kpiCodeToIndexMap = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                String colName = columns[i];
                if (colName != null && colName.contains("-")) {
                    try {
                        String kpiCode = colName.split("-")[0];
                        if (kpiCode != null && !kpiCode.trim().isEmpty()) {
                            kpiCodeToIndexMap.put(kpiCode, i);
                            if (IS_LOG_ENABLED) {
                                logger.info("Mapped KPI Code {} to Column Index {}", kpiCode, i);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Failed to Parse KPI Code From Column {}: {}", colName, e.getMessage());
                    }
                }
            }

            // Create Grouped Header Style With Grey Background and White Text
            CellStyle groupedHeaderStyle = getGroupedHeaderStyle(workbook);
            Font groupHeaderFont = getGroupHeaderFont(workbook);
            groupedHeaderStyle.setFont(groupHeaderFont);

            // Create Empty Group Header Style With White Background
            CellStyle emptyGroupHeaderStyle = getEmptyGroupHeaderStyle(workbook);

            // Create KPI Group Header Row (Row 0) and Regular Header Row (Row 1)
            SXSSFRow groupHeaderRow = null;
            SXSSFRow headerRow;
            int dataStartRowIdx;
            if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                if (IS_LOG_ENABLED) {
                    logger.info("KPI Group Map Is Present And Not Empty, Creating Grouped Headers!");
                }
                groupHeaderRow = sheet.createRow(0);
                headerRow = sheet.createRow(1);
                dataStartRowIdx = 2;
            } else {
                if (IS_LOG_ENABLED) {
                    logger.info("No KPI Group Map Provided, Skipping Grouped Headers!");
                }
                headerRow = sheet.createRow(0);
                dataStartRowIdx = 1;
            }

            // Create Grouped headers - Each Column Gets Its Own Cell Initially
            Map<Integer, String> columnToGroupMap = new HashMap<>();
            Map<String, List<Integer>> groupToColumnsMap = new HashMap<>();

            if (groupHeaderRow != null) {
                if (IS_LOG_ENABLED) {
                    logger.info("Processing KPI Group Headers: {}", kpiGroupMap);
                    logger.info("Available Columns In DataFrame: {}", Arrays.toString(columns));
                }

                for (Map.Entry<String, List<String>> groupEntry : kpiGroupMap.entrySet()) {
                    String groupName = groupEntry.getKey();
                    List<String> groupColumns = groupEntry.getValue();
                    if (IS_LOG_ENABLED) {
                        logger.info("Processing Group: {} With Columns: {}", groupName, groupColumns);
                    }
                    for (String groupColumn : groupColumns) {
                        boolean found = false;
                        for (int i = 0; i < columns.length; i++) {
                            if (columns[i].equals(groupColumn)) {
                                columnToGroupMap.put(i, groupName);
                                if (IS_LOG_ENABLED) {
                                    logger.info("Mapped Column {} (Index {}) To Group {}", groupColumn, i, groupName);
                                }
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            if (IS_LOG_ENABLED) {
                                logger.info("Column {} From Group {} Not Found In DataFrame Columns", groupColumn,
                                        groupName);
                            }
                        }
                    }
                }
                if (IS_LOG_ENABLED) {
                    logger.info("Final Column To Group Mapping: {}", columnToGroupMap);
                    logger.info("Creating Group Headers For {} Columns", columns.length);
                }

                for (int i = 0; i < columns.length; i++) {
                    SXSSFCell groupCell = groupHeaderRow.createCell(i);
                    String groupName = columnToGroupMap.get(i);
                    if (groupName != null) {
                        groupCell.setCellValue(groupName);
                        groupCell.setCellStyle(groupedHeaderStyle);
                        if (IS_LOG_ENABLED) {
                            logger.info("Set Group Header For Column {} (Index {}): {} With Grey Background",
                                    columns[i], i, groupName);
                        }
                    } else {
                        groupCell.setCellValue("");
                        groupCell.setCellStyle(emptyGroupHeaderStyle);
                        if (IS_LOG_ENABLED) {
                            logger.info(
                                    "No Group Found For Column {} (Index {}), Setting Empty Group Header With White Background",
                                    columns[i], i);
                        }
                    }
                }

                // Merge Columns By Their Groups
                if (IS_LOG_ENABLED) {
                    logger.info("Merging Columns By Their Groups...");
                }
                for (Map.Entry<Integer, String> entry : columnToGroupMap.entrySet()) {
                    String groupName = entry.getValue();
                    Integer columnIndex = entry.getKey();
                    groupToColumnsMap.computeIfAbsent(groupName, k -> new ArrayList<>()).add(columnIndex);
                }

                if (IS_LOG_ENABLED) {
                    logger.info("Group To Columns Mapping: {}", groupToColumnsMap);
                }
                for (Map.Entry<String, List<Integer>> groupEntry : groupToColumnsMap.entrySet()) {
                    String groupName = groupEntry.getKey();
                    List<Integer> columnIndices = groupEntry.getValue();
                    if (columnIndices.size() > 1) {
                        Collections.sort(columnIndices);
                        int startIndex = columnIndices.get(0);
                        int endIndex = columnIndices.get(columnIndices.size() - 1);
                        sheet.addMergedRegion(new CellRangeAddress(0, 0, startIndex, endIndex));
                        if (IS_LOG_ENABLED) {
                            logger.info("Merged Group '{}' Header Cells From Column {} To {}", groupName, startIndex,
                                    endIndex);
                        }
                    } else {
                        if (IS_LOG_ENABLED) {
                            logger.info("Group '{}' Has Single Column At Index {}, No Merging Needed", groupName,
                                    columnIndices.get(0));
                        }
                    }
                }
                if (IS_LOG_ENABLED) {
                    logger.info("=== FINAL GROUP HEADER STRUCTURE ===");
                    logger.info("Total Columns: {}", columns.length);
                    logger.info("Grouped Columns: {} (Total Groups: {})", columnToGroupMap.size(),
                            groupToColumnsMap.size());
                    logger.info("Non-Grouped Columns: {}", columns.length - columnToGroupMap.size());
                    logger.info("Groups: {}", groupToColumnsMap.keySet());
                    logger.info("=====================================");
                }
            }

            // Create regular headers
            for (int i = 0; i < columns.length; i++) {
                SXSSFCell cell = headerRow.createCell(i);
                cell.setCellValue(columns[i]);
                cell.setCellStyle(headerStyle);
            }

            // STREAMING DATA PROCESSING - Use toLocalIterator() instead of collectAsList()
            // Support multiple sheets for large datasets (50k rows per sheet)
            if (IS_LOG_ENABLED) {
                logger.info("Starting Streaming Data Processing with Multiple Sheets Support...");
            }
            long totalRowCount = 0;
            int currentSheetIndex = 0;
            final int MAX_ROWS_PER_SHEET = 50000; // 50k rows per sheet
            SXSSFSheet currentSheet = sheet;
            SXSSFRow currentGroupHeaderRow = groupHeaderRow;
            SXSSFRow currentHeaderRow = headerRow;

            Iterator<Row> rowIterator = expectedDF.toLocalIterator();

            while (rowIterator.hasNext()) {
                Row sparkRow = rowIterator.next();

                // Check if we need to create a new sheet
                long currentSheetRowCount = totalRowCount % MAX_ROWS_PER_SHEET;
                if (currentSheetRowCount == 0 && totalRowCount > 0) {
                    // Create new sheet
                    currentSheetIndex++;
                    String sheetName = frequency + "_" + (currentSheetIndex + 1);
                    currentSheet = workbook.createSheet(sheetName);
                    // Enable column tracking for auto-sizing in the new sheet
                    currentSheet.trackColumnsForAutoSizing(autoSizeColumnIndices);
                    if (IS_LOG_ENABLED) {
                        logger.info("Created new sheet: {} for rows {} to {}", sheetName, totalRowCount + 1,
                                totalRowCount + MAX_ROWS_PER_SHEET);
                    }
                    // Recreate headers for new sheet
                    if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                        currentGroupHeaderRow = currentSheet.createRow(0);
                        currentHeaderRow = currentSheet.createRow(1);
                        dataStartRowIdx = 2;

                        // Recreate group headers
                        for (int i = 0; i < columns.length; i++) {
                            SXSSFCell groupCell = currentGroupHeaderRow.createCell(i);
                            String groupName = columnToGroupMap.get(i);
                            if (groupName != null) {
                                groupCell.setCellValue(groupName);
                                groupCell.setCellStyle(groupedHeaderStyle);
                            } else {
                                groupCell.setCellValue("");
                                groupCell.setCellStyle(emptyGroupHeaderStyle);
                            }
                        }

                        // Recreate merged regions for group headers
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
                    }

                    // Recreate regular headers
                    for (int i = 0; i < columns.length; i++) {
                        SXSSFCell cell = currentHeaderRow.createCell(i);
                        cell.setCellValue(columns[i]);
                        cell.setCellStyle(headerStyle);
                    }
                }

                SXSSFRow dataRow = currentSheet.createRow((int) (currentSheetRowCount + dataStartRowIdx));

                for (int j = 0; j < columns.length; j++) {
                    SXSSFCell cell = dataRow.createCell(j);
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

                    // Start With Base Data Style
                    CellStyle cellStyle = dataStyle;

                    // RAG Logic - Only Apply To Columns That Match KPI Codes
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
                                                        XSSFColor fillColor = new XSSFColor(
                                                                java.awt.Color.decode(hexColor),
                                                                null);
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
                                            logger.error("RAG Evaluation Failed For KPI {}: {}", kpiCode,
                                                    ex.getMessage());
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Error Processing RAG For Column {}: {}", colName, e.getMessage());
                        }
                    }

                    cell.setCellStyle(cellStyle);
                }

                totalRowCount++;
                if (totalRowCount % 100000 == 0) {
                    if (IS_LOG_ENABLED) {
                        logger.info("Processed {} total rows across {} sheets...", totalRowCount,
                                currentSheetIndex + 1);
                    }
                }
            }

            if (IS_LOG_ENABLED) {
                logger.info("Total rows processed: {} across {} sheets", totalRowCount, currentSheetIndex + 1);
            }

            // Auto-size columns and set formatting for all sheets
            for (int sheetIdx = 0; sheetIdx <= currentSheetIndex; sheetIdx++) {
                SXSSFSheet sheetToFormat = (sheetIdx == 0) ? sheet : workbook.getSheetAt(sheetIdx);

                // Auto-size columns
                for (int i = 0; i < columns.length; i++) {
                    sheetToFormat.autoSizeColumn(i);
                    int currentWidth = sheetToFormat.getColumnWidth(i);
                    sheetToFormat.setColumnWidth(i, currentWidth + 1000);
                }

                // Set freeze panes and auto filter for each sheet
                long sheetRowCount = Math.min(MAX_ROWS_PER_SHEET, totalRowCount - (sheetIdx * MAX_ROWS_PER_SHEET));
                if (kpiGroupMap != null && !kpiGroupMap.isEmpty()) {
                    sheetToFormat.createFreezePane(0, 2);
                    sheetToFormat
                            .setAutoFilter(new CellRangeAddress(0, (int) (sheetRowCount + 1), 0, columns.length - 1));
                } else {
                    sheetToFormat.createFreezePane(0, 1);
                    sheetToFormat.setAutoFilter(new CellRangeAddress(0, (int) sheetRowCount, 0, columns.length - 1));
                }
            }

            // Write workbook to file
            fileOut = new FileOutputStream(excelFile);
            workbook.write(fileOut);
            fileOut.flush();

            if (IS_LOG_ENABLED) {
                logger.info("Excel File Created Successfully At: {}", excelFilePath);
                logger.info("Step 3: Creating MinIO S3 Client");
            }

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

            if (IS_LOG_ENABLED) {
                logger.info("MinIO S3 Client Created Successfully");
                logger.info("Step 4: Uploading to Temporary MinIO Path: {}", tmpMinioPath);
            }
            inputStream = new FileInputStream(excelFile);
            ObjectMetadata metadata = new ObjectMetadata();
            long excelFileSize = excelFile.length();
            metadata.setContentLength(excelFileSize);
            s3client.putObject(bucketName, tmpMinioPath, inputStream, metadata);

            String fileSize = String.valueOf(excelFileSize);
            jobContext.setParameters("FILE_SIZE", fileSize);

            if (IS_LOG_ENABLED) {
                logger.info("Excel File Uploaded Successfully to Temporary MinIO Path");
                logger.info("FILE_SIZE '{}' Set to Job Context Successfully!", fileSize);
                logger.info("Step 5: Copying to Final MinIO Path: {}", finalMinioPath);
            }

            s3client.copyObject(bucketName, tmpMinioPath, bucketName, finalMinioPath);
            if (IS_LOG_ENABLED) {
                logger.info("Excel File Copied Successfully to Final Path");
            }

            if (!s3client.doesObjectExist(bucketName, finalMinioPath)) {
                throw new RuntimeException("Failed to Verify Final File Exists: " + finalMinioPath);
            }

            if (IS_LOG_ENABLED) {
                logger.info("Step 6: Deleting Temporary Files from MinIO");
            }

            s3client.deleteObject(bucketName, tmpMinioPath);

            if (IS_LOG_ENABLED) {
                logger.info("Temporary MinIO Objects Deleted Successfully");
                logger.info("Step 7: Cleaning Up Local Temporary Files");
            }
            FileUtils.deleteDirectory(new File(tmpDirPath));
            if (IS_LOG_ENABLED) {
                logger.info("Local Temporary Files Deleted Successfully");
                logger.info("Excel Report Uploaded Successfully!");
                logger.info("Final file location: {}/{}", bucketName, finalMinioPath);
            }

        } catch (Exception e) {
            logger.error("Error in Processing Excel Report, Message: {}, Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to Process Excel Report", e);
        } finally {
            // Properly close resources
            if (fileOut != null) {
                try {
                    fileOut.close();
                } catch (IOException e) {
                    logger.error("Error in Closing File Output Stream, Message: {}, Error: {}", e.getMessage(), e);
                }
            }
            if (workbook != null) {
                try {
                    workbook.dispose(); // Clean up temporary files
                } catch (Exception e) {
                    logger.error("Error in Disposing Workbook, Message: {}, Error: {}", e.getMessage(), e);
                }
            }
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

    private static String getReportFileName(Map<String, String> jobContextMap, Map<String, String> extraParametersMap,
            Map<String, String> reportWidgetDetailsMap) {

        String reportName = reportWidgetDetailsMap.get("REPORT_NAME");
        String level = jobContextMap.get("AGGREGATION_LEVEL");
        String domain = jobContextMap.get("DOMAIN");
        String vendor = jobContextMap.get("VENDOR");
        String frequency = jobContextMap.get("FREQUENCY");
        String reportType = "OTF";
        String reportFormat = extraParametersMap.get("REPORT_FORMAT_TYPE");
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
