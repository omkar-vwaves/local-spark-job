package com.enttribe.custom.processor;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AdvaPerformanceDataGenerator extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(AdvaPerformanceDataGenerator.class);

    private static final String S3_BUCKET = "performance";
    private static final String S3_BASE_PATH = "JOB/RawFiles/ADVA";

    private static final String ROUTER_INTERFACE_QUERY = """
            SELECT parent.NE_NAME AS router_nename,
                   GROUP_CONCAT(child.NE_NAME ORDER BY child.NE_NAME SEPARATOR ',') AS interface_nenames
            FROM NETWORK_ELEMENT child
            JOIN NETWORK_ELEMENT parent
              ON child.PARENT_NE_ID_FK = parent.NETWORK_ELEMENT_ID_PK
            WHERE parent.DOMAIN = 'TRANSPORT'
              AND parent.VENDOR = 'ADVA'
              AND parent.PM_EMS_ID IS NOT NULL
              AND child.PM_EMS_ID IS NOT NULL
            GROUP BY parent.NE_NAME
            """;

    private static final String CISCO_COUNTER_QUERY = """
            SELECT T1.RAW_FILE_COUNTER_ID
            FROM KPI_COUNTER AS T1
            JOIN PM_CATEGORY AS T2 ON T1.PM_CATEGORY_ID_FK = T2.PM_CATEGORY_ID_PK
            JOIN PM_NODE_VENDOR AS T3 ON T2.PM_NODE_VENDOR_ID_FK = T3.PM_NODE_VENDOR_ID_PK
            WHERE T3.VENDOR = 'ADVA'
            """;

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 5000;

    private static final Random random = new Random();

    public AdvaPerformanceDataGenerator() {
        super();
    }

    public AdvaPerformanceDataGenerator(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
    }

    public AdvaPerformanceDataGenerator(Integer id, String processorName) {
        super(id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        logger.info("🚀 Starting ADVA Performance Data Generation");

        long startTime = System.currentTimeMillis();

        String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
        logger.info("📊 Timestamp: {}", timestamp);

        try {
            processPerformanceDataGeneration(timestamp);

            logger.info("✅ ADVA Performance Data Generation Completed Successfully");

        } catch (Exception e) {
            logger.error("❌ Error in ADVA Performance Data Generation: {}", e.getMessage(), e);
            throw e;
        } finally {
            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            logger.info("⏱️ Execution Completed In {} Minutes {} Seconds",
                    executionTime / 60000, (executionTime % 60000) / 1000);
        }

        return this.dataFrame;
    }

    private void processPerformanceDataGeneration(String timestamp) {
        logger.info("🔄 Starting Performance Data Generation Process");

        Connection connection = null;
        try {
            connection = getDatabaseConnection();
            if (connection == null) {
                throw new RuntimeException("Failed to Establish Database Connection");
            }

            List<RouterInterfaceData> routerData = queryRouterInterfaceData(connection);
            logger.info("📊 Retrieved {} Routers With Interfaces", routerData.size());

            List<String> counterNames = queryAdvaCounterNames(connection);
            logger.info("📊 Retrieved {} ADVA Counter Names", counterNames.size());

            TimeSlot currentSlot = getTimeSlotFromTimestamp(timestamp);
            logger.info("⏰ Processing Time Slot: {}", currentSlot);

            List<String> zipFilePaths = new ArrayList<>();
            for (RouterInterfaceData router : routerData) {
                String csvFilePath = generateCsvFileForRouter(router, currentSlot, timestamp, counterNames);
                logger.info("📄 Generated CSV File For Router {}: {}", router.getRouterNeid(), csvFilePath);

                String zipFilePath = createZipFileForRouter(csvFilePath, router, currentSlot);
                logger.info("📦 Created Zip File For Router {}: {}", router.getRouterNeid(), zipFilePath);

                zipFilePaths.add(zipFilePath);

                uploadToMinIOForRouter(zipFilePath, router, currentSlot);
                logger.info("☁️ Uploaded router {} to MinIO/S3 successfully", router.getRouterNeid());

                cleanupLocalFiles(csvFilePath, zipFilePath);
            }

            logger.info("🧹 Cleaned up all local files for {} routers", routerData.size());

            logger.info("✅ Performance data generation completed successfully");

        } catch (Exception e) {
            logger.error("❌ Error in performance data generation: {}", e.getMessage(), e);
        } finally {
            closeDatabaseConnection(connection);
        }
    }

    private Connection getDatabaseConnection() {
        String jdbcDriver = "org.mariadb.jdbc.Driver";
        String jdbcUrl = "jdbc:mysql://mysql-nst-cluster.nstdb.svc.cluster.local:6446/PERFORMANCE?autoReconnect=true";
        String jdbcUsername = "PERFORMANCE";
        String jdbcPassword = "perform!123";

        logger.info("🔌 Connecting to database: {}", jdbcUrl);

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                Class.forName(jdbcDriver);
                Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
                connection.setAutoCommit(false);
                logger.info("✅ Database Connection Established (Attempt {})", attempt);
                return connection;

            } catch (ClassNotFoundException e) {
                logger.error("❌ JDBC Driver not found (Attempt {}): {}", attempt, e.getMessage());
            } catch (SQLException e) {
                logger.error("❌ Database connection error (Attempt {}): {}", attempt, e.getMessage());
            } catch (Exception e) {
                logger.error("❌ Unexpected error (Attempt {}): {}", attempt, e.getMessage());
            }

            if (attempt < MAX_RETRY_ATTEMPTS) {
                try {
                    Thread.sleep(RETRY_DELAY_MS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        throw new RuntimeException("Failed to establish database connection after " + MAX_RETRY_ATTEMPTS + " attempts");
    }

    private List<RouterInterfaceData> queryRouterInterfaceData(Connection connection) throws SQLException {
        List<RouterInterfaceData> routerData = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(ROUTER_INTERFACE_QUERY);
                ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {
                String parentNename = resultSet.getString("router_nename");
                String childNenames = resultSet.getString("interface_nenames");

                RouterInterfaceData data = new RouterInterfaceData(parentNename, childNenames);
                routerData.add(data);
            }
        }

        return routerData;
    }

    private List<String> queryAdvaCounterNames(Connection connection) throws SQLException {
        List<String> counterNames = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(CISCO_COUNTER_QUERY);
                ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {
                String counterName = resultSet.getString("RAW_FILE_COUNTER_ID");
                counterNames.add(counterName);
            }
        }

        return counterNames;
    }

    private String generateCsvFileForRouter(RouterInterfaceData router, TimeSlot timeSlot, String timestamp,
            List<String> counterNames)
            throws IOException {
        String fileName = "ADVA_Performance_" + router.getRouterNeid() + "_" + timeSlot.getSlotId() + "_" + timestamp
                + ".csv";
        Path filePath = Paths.get(System.getProperty("java.io.tmpdir"), fileName);

        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            // Write CSV header comment
            String currentDateTime = LocalDateTime.now(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yy-MM-dd HH:mm:ss"));
            writer.write("# " + fileName + ", created on " + currentDateTime);
            writer.newLine();
            writer.newLine();

            // Write CSV header
            writer.write("Connection|Granularity|Type|Unit|Aggregation|NE|Entity|Period|Value");
            writer.newLine();

            String[] interfaceIds = router.getInterfaceNeids().split(",");
            for (String interfaceId : interfaceIds) {
                for (String counterName : counterNames) {
                    String csvLine = generatePerformanceCsvLine(router.getRouterNeid(), interfaceId.trim(),
                            counterName, timeSlot.getPeriodTimestamp());
                    writer.write(csvLine);
                    writer.newLine();
                }
            }
        }

        logger.info("📄 Generated CSV File For Router {} With {} Interfaces and {} Counters",
                router.getRouterNeid(), router.getInterfaceNeids().split(",").length, counterNames.size());

        return filePath.toString();
    }

    private String generatePerformanceCsvLine(String routerNeid, String interfaceId, String counterName,
            String timestamp) {
        // Use the timestamp directly as Period (it's already in yyyyMMddHHmmss format)
        String period = timestamp;

        // Extract entity by removing router_neid prefix from interface_id
        String entity = extractEntityFromInterfaceId(routerNeid, interfaceId);

        // Generate random value
        double value = generateRandomFloat();

        // Format: Connection|Granularity|Type|Unit|Aggregation|NE|Entity|Period|Value
        return String.format("NA|15|%s||other|%s|%s|%s|%.4f",
                counterName, routerNeid, entity, period, value);
    }

    private double generateRandomFloat() {
        return 100.000 + (random.nextDouble() * (999.999 - 100.000));
    }

    private String extractEntityFromInterfaceId(String routerNeid, String interfaceId) {
        // Remove router_neid prefix from interface_id to get entity
        if (interfaceId.startsWith(routerNeid + "_")) {
            return interfaceId.substring(routerNeid.length() + 1); // +1 to skip the underscore
        }
        // Fallback: if interfaceId doesn't start with router_neid, return the whole
        // interfaceId
        return interfaceId;
    }

    private String createZipFileForRouter(String csvFilePath, RouterInterfaceData router, TimeSlot timeSlot)
            throws IOException {
        String zipFileName = router.getRouterNeid() + "_" + timeSlot.getSlotId() + ".zip";
        Path zipPath = Paths.get(System.getProperty("java.io.tmpdir"), zipFileName);

        try (FileOutputStream fos = new FileOutputStream(zipPath.toFile());
                ZipOutputStream zos = new ZipOutputStream(fos);
                FileInputStream fis = new FileInputStream(csvFilePath)) {

            ZipEntry zipEntry = new ZipEntry(
                    "pmDumpOTUFECPerformance" + router.getRouterNeid() + "_" + timeSlot.getSlotId() + "_Main.csv");
            zos.putNextEntry(zipEntry);

            byte[] buffer = new byte[1024];
            int length;
            while ((length = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, length);
            }

            zos.closeEntry();
        }

        logger.info("📦 Created Zip File For Router {}: {} ({} bytes)", router.getRouterNeid(), zipPath,
                Files.size(zipPath));
        return zipPath.toString();
    }

    private void uploadToMinIOForRouter(String zipFilePath, RouterInterfaceData router, TimeSlot timeSlot) {
        String endpointUrl = "http://seaweedfs-s3.swf.svc.cluster.local:8333";
        String accessKey = "bootadmin";
        String secretKey = "bootadmin";

        logger.info("☁️ Uploading Router {} to MinIO: {}", router.getRouterNeid(), endpointUrl);

        try {
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointUrl, "us-east-1"))
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();

            String s3Key = String.format("%s/%s/%s/%s_%s.zip",
                    S3_BASE_PATH, timeSlot.getDateString(), timeSlot.getSlotId(), router.getRouterNeid(),
                    timeSlot.getSlotId());

            logger.info("📤 Uploading Router {} to S3 Key: {}", router.getRouterNeid(), s3Key);

            File zipFile = new File(zipFilePath);
            PutObjectRequest putRequest = new PutObjectRequest(S3_BUCKET, s3Key, zipFile);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(zipFile.length());
            metadata.setContentType("application/zip");
            putRequest.setMetadata(metadata);

            s3Client.putObject(putRequest);

            logger.info("✅ Successfully Uploaded Router {} to S3: s3://{}/{}", router.getRouterNeid(), S3_BUCKET,
                    s3Key);

        } catch (Exception e) {
            logger.error("❌ Error Uploading Router {} to MinIO/S3: {}", router.getRouterNeid(), e.getMessage(), e);
            throw new RuntimeException("Failed to Upload Router " + router.getRouterNeid() + " to MinIO/S3", e);
        }
    }

    private void cleanupLocalFiles(String csvFilePath, String zipFilePath) {
        try {
            Files.deleteIfExists(Paths.get(csvFilePath));
            Files.deleteIfExists(Paths.get(zipFilePath));
            logger.info("🧹 Cleaned Up Local Files");
        } catch (IOException e) {
            logger.warn("⚠️ Error Cleaning Up Local Files: {}", e.getMessage());
        }
    }

    private void closeDatabaseConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                logger.info("🔌 Database Connection Closed");
            } catch (SQLException e) {
                logger.error("❌ Error Closing Database Connection: {}", e.getMessage());
            }
        }
    }

    private TimeSlot getTimeSlotFromTimestamp(String timestamp) {
        long timestampSeconds = Long.parseLong(timestamp);
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(timestampSeconds, 0, ZoneOffset.UTC);
        int minute = dateTime.getMinute();
        int quarterMinute = (minute / 15) * 15;

        LocalDateTime quarterTime = dateTime.withMinute(quarterMinute).withSecond(0).withNano(0);
        String slotId = quarterTime.format(DateTimeFormatter.ofPattern("HHmm"));
        String dateString = quarterTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String periodTimestamp = quarterTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        return new TimeSlot(quarterTime, slotId, dateString, periodTimestamp);
    }

    private static class RouterInterfaceData {
        private final String routerNeid;
        private final String interfaceNeids;

        public RouterInterfaceData(String routerNeid, String interfaceNeids) {
            this.routerNeid = routerNeid;
            this.interfaceNeids = interfaceNeids;
        }

        public String getRouterNeid() {
            return routerNeid;
        }

        public String getInterfaceNeids() {
            return interfaceNeids;
        }
    }

    private static class TimeSlot {
        private final String slotId;
        private final String dateString;
        private final String periodTimestamp;

        public TimeSlot(LocalDateTime slotTime, String slotId, String dateString, String periodTimestamp) {
            this.slotId = slotId;
            this.dateString = dateString;
            this.periodTimestamp = periodTimestamp;
        }

        public String getSlotId() {
            return slotId;
        }

        public String getDateString() {
            return dateString;
        }

        public String getPeriodTimestamp() {
            return periodTimestamp;
        }

        @Override
        public String toString() {
            return String.format("%s/%s", dateString, slotId);
        }
    }
}
