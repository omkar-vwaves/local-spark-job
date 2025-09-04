package com.enttribe.custom;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Generates dynamic Cassandra INSERT queries for combinequarterlypm table
 */
public class CassandraQueryGenerator {
    
    private static final String OUTPUT_FILE_PATH = "/Users/ent-00356/Desktop/query/generated_queries.txt";
    private static final String METAJSON_TEMPLATE = "{'NEID': 'India','NAM':'India','NET':'ROUTER','ENTITY_ID':'India','ENTITY_NAME':'India','ENTITY_TYPE':'ROUTER'}";
    private static final String NODENAME = "India";
    private static final Random random = new Random();
    
    public static void main(String[] args) {
        // Example usage with dynamic inputs
        String domain = "TRANSPORT";
        String vendor = "JUNIPER";
        String technology = "COMMON";
        String datalevel = "L0_COMMON_ROUTER";
        String networktype = "ROUTER";
        String date = "20250808";
        String frequency = "15min"; // or "5min"
        
        generateQueries(domain, vendor, technology, datalevel, networktype, date, frequency);
    }
    
    /**
     * Generates Cassandra INSERT queries based on the provided parameters
     */
    public static void generateQueries(String domain, String vendor, String technology, 
                                     String datalevel, String networktype, String date, String frequency) {
        
        try {
            // Create output directory if it doesn't exist
            Files.createDirectories(Paths.get("/Users/ent-00356/Desktop/query/"));
            
            // Determine number of queries based on frequency
            int queryCount = frequency.equals("15min") ? 96 : 288;
            int minutesIncrement = frequency.equals("15min") ? 15 : 5;
            
            // Parse date to LocalDateTime
            LocalDateTime baseDateTime = LocalDateTime.parse(
                date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8) + "T00:00:00"
            );
            
            // Format for timestamp
            DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS+0000");
            
            StringBuilder allQueries = new StringBuilder();
            
            for (int i = 0; i < queryCount; i++) {
                // Calculate timestamp
                LocalDateTime currentTime = baseDateTime.plusMinutes(i * minutesIncrement);
                String timestamp = currentTime.format(timestampFormatter);
                
                // Generate random KPI JSON
                String kpijson = generateRandomKPIJSON();
                
                // Build the INSERT query
                String query = buildInsertQuery(domain, vendor, technology, datalevel, date, 
                                              NODENAME, timestamp, kpijson, METAJSON_TEMPLATE, networktype);
                
                allQueries.append(query).append("\n");
            }
            
            // Write to file
            Files.write(Paths.get(OUTPUT_FILE_PATH), allQueries.toString().getBytes());
            
            System.out.println("Successfully generated " + queryCount + " queries and saved to: " + OUTPUT_FILE_PATH);
            
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error generating queries: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Generates KPI JSON with all specific KPI codes
     */
    private static String generateRandomKPIJSON() {
        Map<String, String> kpiMap = new HashMap<>();
        
        // Specific KPI codes as provided - ALL codes will be included in every query
        String[] kpiCodes = {"1045", "1046", "1047", "1048", "1049", "1050", "1051", "1054", "1055", "1056", "1057", "1059", "1061", "1063", "1065", "1066", "1133"};
        
        // Include ALL KPI codes in every query with random values
        for (String kpiCode : kpiCodes) {
            String kpiValue = String.valueOf(random.nextInt(1001)); // 0 to 1000
            kpiMap.put(kpiCode, kpiValue);
        }
        
        // Convert to JSON format
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : kpiMap.entrySet()) {
            if (!first) {
                json.append(", ");
            }
            json.append("'").append(entry.getKey()).append("': '").append(entry.getValue()).append("'");
            first = false;
        }
        json.append("}");
        
        return json.toString();
    }
    
    /**
     * Builds the complete INSERT query
     */
    private static String buildInsertQuery(String domain, String vendor, String technology, 
                                         String datalevel, String date, String nodename, 
                                         String timestamp, String kpijson, String metajson, String networktype) {
        
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO combinequarterlypm (domain, vendor, technology, datalevel, date, nodename, timestamp, kpijson, metajson, networktype) ");
        query.append("VALUES ('").append(domain).append("', '").append(vendor).append("', '").append(technology).append("', '")
              .append(datalevel).append("', '").append(date).append("', '").append(nodename).append("', '")
              .append(timestamp).append("', ").append(kpijson).append(", ").append(metajson).append(", '")
              .append(networktype).append("');");
        
        return query.toString();
    }
} 