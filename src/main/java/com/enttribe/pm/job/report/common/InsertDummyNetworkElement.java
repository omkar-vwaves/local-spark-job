package com.enttribe.pm.job.report.common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class InsertDummyNetworkElement {

    public static void main(String[] args) {
        String outputPath = "/Users/ent-00356/Documents/dummydata/router_inserts.txt";
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            
            System.out.println("Generating INSERT queries for 100,000 routers...");
            System.out.println("Output file: " + outputPath);
            
            for (int i = 1; i <= 100000; i++) {
                String insertQuery = generateInsertQuery(i, currentTime);
                writer.write(insertQuery);
                writer.newLine();
                if (i % 10000 == 0) {
                    System.out.println("Generated " + i + " InsertQueries...");
                }
            }
            
            System.out.println("Successfully Generated 1,00,000 INSERT Queries!");
            System.out.println("File Saved to: " + outputPath);
            
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static String generateInsertQuery(int sequenceNumber, String currentTime) {
        String paddedNumber = String.format("%06d", sequenceNumber);
        
        return "INSERT INTO NETWORK_ELEMENT (LATITUDE, LONGITUDE, NE_FREQUENCY, NE_NAME, HOSTNAME, NE_ID, NE_TYPE, NE_STATUS, DOMAIN, VENDOR, TECHNOLOGY, GEOGRAPHY_L4_ID_FK, GEOGRAPHY_L3_ID_FK, GEOGRAPHY_L2_ID_FK, GEOGRAPHY_L1_ID_FK, CREATION_TIME, MODIFICATION_TIME, PARENT_NE_ID_FK, DELETED) VALUES (18.56231, 73.92345, 1200, 'PNE-J2-C-T1-RR-" + paddedNumber + "', 'PNE-J2-C-T1-RR-" + paddedNumber + "', '11.11.11." + paddedNumber + "', 'ROUTER', 'ONAIR', 'TRANSPORT', 'JUNIPER', 'COMMON', 47106, 24944, 2607, 71, '" + currentTime + "', '" + currentTime + "', NULL, 0);";
    }
}
