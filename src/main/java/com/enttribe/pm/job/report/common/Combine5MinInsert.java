package com.enttribe.pm.job.report.common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class Combine5MinInsert {
    
    private static final String OUTPUT_FILE_PATH = "/Users/ent-00356/Documents/dummydata/insert_combine5minpm.txt";
    private static final Random random = new Random();
    
    public static void main(String[] args) {
        System.out.println("Starting to Generate Cassandra INSERT Queries for pm.combine5minpm Table...");
        
        try {
            // Create directory if it doesn't exist
            Files.createDirectories(Paths.get("/Users/ent-00356/Documents/dummydata/"));
            
            // Generate INSERT queries
            generateInsertQueries();
            
            System.out.println("Successfully Generated INSERT Queries in: " + OUTPUT_FILE_PATH);
            
        } catch (Exception e) {
            System.err.println("Error Generating INSERT Queries: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void generateInsertQueries() throws IOException {
        // Define date range: July 1, 2025 00:00:00 to August 31, 2025 23:55:00 UTC
        LocalDateTime startDateTime = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        LocalDateTime endDateTime = LocalDateTime.of(2025, 8, 31, 23, 55, 0);
        
        // 5-minute interval
        LocalDateTime currentDateTime = startDateTime;
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH))) {
            int queryCount = 0;
            
            while (!currentDateTime.isAfter(endDateTime)) {
                String insertQuery = generateInsertQuery(currentDateTime);
                writer.write(insertQuery);
                writer.newLine();
                
                currentDateTime = currentDateTime.plusMinutes(5);
                queryCount++;
                
                if (queryCount % 1000 == 0) {
                    System.out.println("Generated " + queryCount + " Queries...");
                }
            }
            
            System.out.println("Total Queries Generated: " + queryCount);
        }
    }
    
    private static String generateInsertQuery(LocalDateTime dateTime) {
        // Format date and time components
        String yyyyMMdd = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String ddMMyyyy = dateTime.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
        String ddMMyy = dateTime.format(DateTimeFormatter.ofPattern("ddMMyy"));
        String HHmm = dateTime.format(DateTimeFormatter.ofPattern("HHmm"));
        
        // Format timestamp with microseconds and UTC offset
        String timestamp = dateTime.atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS+0000"));
        
        // Generate random KPI values (101-999)
        StringBuilder kpiJson = new StringBuilder();
        kpiJson.append("{'1010': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1011': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1012': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1013': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1014': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1015': '").append(random.nextInt(899) + 101).append("'}");
        
        // Build meta JSON with dynamic values
        StringBuilder metaJson = new StringBuilder();
        metaJson.append("{'D': 'TRANSPORT'");
        metaJson.append(", 'DL1': 'India'");
        metaJson.append(", 'DT': '").append(ddMMyyyy).append("'");
        metaJson.append(", 'Date': '").append(ddMMyy).append("'");
        metaJson.append(", 'ENTITY_ID': 'India'");
        metaJson.append(", 'ENTITY_NAME': 'India'");
        metaJson.append(", 'ENTITY_TYPE': 'ROUTER'");
        metaJson.append(", 'HR': '").append(HHmm).append("'");
        metaJson.append(", 'L1': 'India'");
        metaJson.append(", 'NAM': 'India'");
        metaJson.append(", 'NODE_TYPE': 'ROUTER'");
        metaJson.append(", 'NS': 'READY'");
        metaJson.append(", 'PT': '").append(HHmm).append("'");
        metaJson.append(", 'T': 'COMMON'");
        metaJson.append(", 'Time': '").append(HHmm).append("'");
        metaJson.append(", 'V': 'CISCO'");
        metaJson.append(", 'datalevel': 'GEOGRAPHY'");
        metaJson.append(", 'date': '").append(yyyyMMdd).append("'");
        metaJson.append(", 'displaynodename': 'India'");
        metaJson.append(", 'emstype': 'NA'");
        metaJson.append(", 'parquetLevel': 'L0'}");
        
        // Build the complete INSERT query
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO pm.combine5minpm (");
        query.append("domain, vendor, technology, datalevel, date, nodename, timestamp, networktype, kpijson, metajson");
        query.append(") VALUES (");
        query.append("'TRANSPORT', 'CISCO', 'COMMON', 'L0_COMMON_Router', '").append(yyyyMMdd).append("', ");
        query.append("'India', '").append(timestamp).append("', 'Router', ");
        query.append(kpiJson).append(", ");
        query.append(metaJson);
        query.append(");");
        
        return query.toString();
    }
}
