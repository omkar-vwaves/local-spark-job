package com.enttribe.pm.job.report.common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class InsertChildNetworkElement {

    public static void main(String[] args) {
        String baseOutputPath = "/Users/ent-00356/Documents/dummydata/";
        
        System.out.println("Generating INSERT Queries for Multiple Nodes (11.11.11.000003 to 11.11.11.010000)...");
        System.out.println("Each node will have 300 interfaces (a/0/1 to a/0/300)");
        System.out.println("Will create 10 files with 100 routers each");
        
        int totalQueries = 0;
        
        // Generate 10 files, each with 100 routers
        for (int fileNum = 1; fileNum <= 10; fileNum++) {
            String outputPath = baseOutputPath + "child_interface_inserts_file_" + String.format("%02d", fileNum) + ".txt";
            
            System.out.println("\n=== Generating File " + fileNum + " ===");
            System.out.println("Output file: " + outputPath);
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
                String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                
                // Calculate start and end node numbers for this file
                int startNode = 2 + (fileNum - 1) * 1000 + 1; // 000003, 01003, 02003, etc.
                int endNode = startNode + 999; // 01002, 02002, 03002, etc.
                
                System.out.println("Generating nodes from 11.11.11." + String.format("%06d", startNode) + 
                                 " to 11.11.11." + String.format("%06d", endNode));
                
                int fileQueries = 0;
                
                // Generate for nodes in this file range
                for (int nodeNum = startNode; nodeNum <= endNode; nodeNum++) {
                    String paddedNodeNumber = String.format("%06d", nodeNum);
                    int parentNeId = 361682009 + (nodeNum - 1); // Calculate parent NE ID
                    
                    // Generate 300 interfaces for each node
                    for (int interfaceNum = 1; interfaceNum <= 300; interfaceNum++) {
                        String insertQuery = generateInsertQuery(paddedNodeNumber, interfaceNum, parentNeId, currentTime);
                        writer.write(insertQuery);
                        writer.newLine();
                        fileQueries++;
                        totalQueries++;
                    }
                    
                    // Progress indicator every 100 nodes
                    if ((nodeNum - startNode + 1) % 100 == 0) {
                        System.out.println("Completed " + (nodeNum - startNode + 1) + " nodes in file " + fileNum + 
                                         ", Generated " + fileQueries + " InsertQueries...");
                    }
                }
                
                System.out.println("File " + fileNum + " completed: " + fileQueries + " INSERT queries");
                
            } catch (IOException e) {
                System.err.println("Error writing to file " + fileNum + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        System.out.println("\n=== SUMMARY ===");
        System.out.println("Successfully Generated " + totalQueries + " INSERT Queries!");
        System.out.println("Created 10 files in: " + baseOutputPath);
        System.out.println("Each file contains 100 routers with 300 interfaces each");
    }
    
    private static String generateInsertQuery(String nodeNumber, int interfaceNumber, int parentNeId, String currentTime) {
        String slotPort = "a/0/" + interfaceNumber;
        
        return "INSERT INTO NETWORK_ELEMENT (LATITUDE, LONGITUDE, NE_FREQUENCY, NE_NAME, HOSTNAME, NE_ID, NE_TYPE, NE_STATUS, DOMAIN, VENDOR, TECHNOLOGY, GEOGRAPHY_L4_ID_FK, GEOGRAPHY_L3_ID_FK, GEOGRAPHY_L2_ID_FK, GEOGRAPHY_L1_ID_FK, CREATION_TIME, MODIFICATION_TIME, PARENT_NE_ID_FK, DELETED) VALUES (18.56231, 73.92345, 1200, 'PNE-J2-C-T1-RR-" + nodeNumber + "_" + slotPort + "', 'PNE-J2-C-T1-RR-" + nodeNumber + "_" + slotPort + "', '11.11.11." + nodeNumber + "_" + slotPort + "', 'INTERFACE', 'ONAIR', 'TRANSPORT', 'JUNIPER', 'COMMON', 47106, 24944, 2607, 71, '" + currentTime + "', '" + currentTime + "', " + parentNeId + ", 0);";
    }
}
