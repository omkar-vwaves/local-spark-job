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

public class Combine5MinNodeInsert {
    
    private static final String OUTPUT_FILE_PATH = "/Users/ent-00356/Documents/dummydata/node_insert_combine5minpm.txt";
    private static final Random random = new Random();
    
    public static void main(String[] args) {
        System.out.println("Starting to Generate Cassandra INSERT Queries for pm.combine5minpm Table...");
        System.out.println("Generating data for 200 routers from August 1-2, 2025...");
        
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
        // Define date range: August 1, 2025 00:00:00 to August 2, 2025 23:55:00 UTC
        LocalDateTime startDateTime = LocalDateTime.of(2025, 8, 1, 0, 0, 0);
        LocalDateTime endDateTime = LocalDateTime.of(2025, 8, 2, 23, 55, 0);
        
        // 5-minute interval
        LocalDateTime currentDateTime = startDateTime;
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH))) {
            int queryCount = 0;
            
            while (!currentDateTime.isAfter(endDateTime)) {
                // Generate 200 INSERT queries for each 5-minute interval (one for each router)
                for (int routerIndex = 1; routerIndex <= 200; routerIndex++) {
                    String insertQuery = generateInsertQuery(currentDateTime, routerIndex);
                    writer.write(insertQuery);
                    writer.newLine();
                    queryCount++;
                }
                
                currentDateTime = currentDateTime.plusMinutes(5);
                
                if (queryCount % 3000 == 0) {
                    System.out.println("Generated " + queryCount + " Queries...");
                }
            }
            
            System.out.println("Total Queries Generated: " + queryCount);
        }
    }
    
    private static String generateInsertQuery(LocalDateTime dateTime, int routerIndex) {
        // Format date and time components
        String yyyyMMdd = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String ddMMyyyy = dateTime.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
        String ddMMyy = dateTime.format(DateTimeFormatter.ofPattern("ddMMyy"));
        String HHmm = dateTime.format(DateTimeFormatter.ofPattern("HHmm"));
        
        // Format timestamp with microseconds and UTC offset
        String timestamp = dateTime.atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss+0000"));
        
        // Generate router IP and name
        String routerIP = String.format("11.11.11.%03d", routerIndex);
        String routerName = String.format("ABC-DE-GH-%03d", routerIndex);
        
        // Generate random KPI values (101-999)
        StringBuilder kpiJson = new StringBuilder();
        kpiJson.append("{'1010': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1011': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1012': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1013': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1014': '").append(random.nextInt(899) + 101).append("'");
        kpiJson.append(", '1015': '").append(random.nextInt(899) + 101).append("'}");
        
        // Build meta JSON with router-specific values
        StringBuilder metaJson = new StringBuilder();
        metaJson.append("{'D': 'TRANSPORT'");
        metaJson.append(", 'DL1': 'East'");
        metaJson.append(", 'DL2': 'Maharashtra'");
        metaJson.append(", 'DL3': 'Pune'");
        metaJson.append(", 'DL4': 'Kharadi'");
        metaJson.append(", 'DT': '").append(ddMMyyyy).append("'");
        metaJson.append(", 'Date': '").append(ddMMyy).append("'");
        metaJson.append(", 'ENTITY_ID': '").append(routerIP).append("'");
        metaJson.append(", 'ENTITY_NAME': '").append(routerName).append("'");
        metaJson.append(", 'ENTITY_TYPE': 'ROUTER'");
        metaJson.append(", 'HR': '").append(HHmm).append("'");
        metaJson.append(", 'L1': 'East'");
        metaJson.append(", 'L2': 'Maharashtra'");
        metaJson.append(", 'L3': 'Pune'");
        metaJson.append(", 'L4': 'Kharadi'");
        metaJson.append(", 'NAM': '").append(routerName).append("'");
        metaJson.append(", 'NODE_TYPE': 'ROUTER'");
        metaJson.append(", 'NS': 'READY'");
        metaJson.append(", 'PT': '").append(HHmm).append("'");
        metaJson.append(", 'T': 'COMMON'");
        metaJson.append(", 'Time': '").append(HHmm).append("'");
        metaJson.append(", 'V': 'CISCO'");
        metaJson.append(", 'datalevel': 'NODE'");
        metaJson.append(", 'date': '").append(yyyyMMdd).append("'");
        metaJson.append(", 'displaynodename': '").append(routerName).append("'");
        metaJson.append(", 'emstype': 'NA'");
        metaJson.append(", 'parquetLevel': 'Router'}");
        
        // Build the complete INSERT query
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO pm.combine5minpm (");
        query.append("domain, vendor, technology, datalevel, date, nodename, timestamp, networktype, kpijson, metajson");
        query.append(") VALUES (");
        query.append("'TRANSPORT', 'CISCO', 'COMMON', 'ROUTER_COMMON_Router', '").append(yyyyMMdd).append("', ");
        query.append("'").append(routerIP).append("', '").append(timestamp).append("', 'Router', ");
        query.append(kpiJson).append(", ");
        query.append(metaJson);
        query.append(");");
        
        return query.toString();
    }
}
