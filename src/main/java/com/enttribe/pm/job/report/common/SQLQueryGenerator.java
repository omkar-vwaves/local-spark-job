package com.enttribe.pm.job.report.common;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SQLQueryGenerator {
    
    public static void main(String[] args) {
        // Input parameters
        String inputDate = "20270718";
        List<String> kpiCodes = List.of("1010", "1011", "1012", "1013", "1014", "1015");
        String frequency = "15 MIN"; // Options: "15 MIN", "PER HOUR", "PER DAY"
        String dataLevel = "L0"; // Options: "L0", "L1", "L2", etc.
        
        // Generate SQL queries
        List<String> sqlQueries = generateSQLQueries(inputDate, kpiCodes, frequency, dataLevel);
        
        // Save to file
        String filePath = "/Users/ent-00356/Desktop/sql_queries.txt";
        saveToFile(sqlQueries, filePath);
        
        System.out.println("SQL queries generated successfully!");
        System.out.println("Total queries generated: " + sqlQueries.size());
        System.out.println("File saved to: " + filePath);
    }
    
    private static List<String> generateSQLQueries(String inputDate, List<String> kpiCodes, String frequency, String dataLevel) {
        List<String> queries = new ArrayList<>();
        
        // Parse the input date
        LocalDateTime baseDate = LocalDateTime.parse(inputDate + "000000", 
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        
        int numberOfQueries = getNumberOfQueries(frequency);
        int intervalMinutes = getIntervalMinutes(frequency);
        
        System.out.println("Generating " + numberOfQueries + " queries for frequency: " + frequency);
        System.out.println("Data level: " + dataLevel);
        
        for (int i = 0; i < numberOfQueries; i++) {
            // Calculate timestamp for this query
            LocalDateTime timestamp = baseDate.plusMinutes(i * intervalMinutes);
            String formattedTimestamp = timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
            
            // Generate KPI JSON
            String kpiJson = generateKPIJson(kpiCodes);
            
            // Generate Meta JSON with dynamic data level
            String metaJson = generateMetaJson(dataLevel);
            
            // Get table name based on frequency
            String tableName = getTableName(frequency);
            
            // Create the SQL query with dynamic data level
            String sqlQuery = String.format(
                "INSERT INTO %s(domain, vendor, technology, datalevel, date, nodename, timestamp, kpijson, metajson, networktype) " +
                "VALUES('TRANSPORT','ALL','COMMON','%s_COMMON','%s','%s','%s',%s,%s,'ALL');",
                tableName, dataLevel, inputDate, "India", formattedTimestamp, kpiJson, metaJson
            );
            
            queries.add(sqlQuery);
        }
        
        return queries;
    }
    
    private static int getNumberOfQueries(String frequency) {
        switch (frequency.toUpperCase()) {
            case "15 MIN":
                return 96; // 24 hours * 4 (15-minute intervals)
            case "PER HOUR":
                return 24; // 24 hours
            case "PER DAY":
                return 1; // 1 day
            default:
                throw new IllegalArgumentException("Invalid frequency: " + frequency + 
                    ". Supported values: 15 MIN, PER HOUR, PER DAY");
        }
    }
    
    private static int getIntervalMinutes(String frequency) {
        switch (frequency.toUpperCase()) {
            case "15 MIN":
                return 15;
            case "PER HOUR":
                return 60;
            case "PER DAY":
                return 1440; // 24 hours * 60 minutes
            default:
                throw new IllegalArgumentException("Invalid frequency: " + frequency);
        }
    }
    
    private static String getTableName(String frequency) {
        switch (frequency.toUpperCase()) {
            case "15 MIN":
                return "combinequarterlypm";
            case "PER HOUR":
                return "combinehourlypm";
            case "PER DAY":
                return "combinedailypm";
            default:
                throw new IllegalArgumentException("Invalid frequency: " + frequency + 
                    ". Supported values: 15 MIN, PER HOUR, PER DAY");
        }
    }
    
    private static String generateKPIJson(List<String> kpiCodes) {
        StringBuilder kpiJson = new StringBuilder("{");
        for (int i = 0; i < kpiCodes.size(); i++) {
            String kpiCode = kpiCodes.get(i);
            // Generate a random value between 1 and 9999 for each KPI
            int randomValue = (int) (Math.random() * 9999) + 1;
            kpiJson.append("'").append(kpiCode).append("':'").append(randomValue).append("'");
            
            if (i < kpiCodes.size() - 1) {
                kpiJson.append(",");
            }
        }
        kpiJson.append("}");
        return kpiJson.toString();
    }
    
    private static String generateMetaJson(String dataLevel) {
        return "{'AGGREGATION_LEVEL': '" + dataLevel + "'}";
    }
    
    private static void saveToFile(List<String> queries, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (String query : queries) {
                writer.write(query + "\n");
            }
            System.out.println("File written successfully to: " + filePath);
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 