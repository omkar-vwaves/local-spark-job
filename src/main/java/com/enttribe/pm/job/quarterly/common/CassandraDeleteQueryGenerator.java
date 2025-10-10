package com.enttribe.pm.job.quarterly.common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class CassandraDeleteQueryGenerator {

    public static void main(String[] args) {
        // Output file path
        String filePath = "/Users/ent-00356/Desktop/deletequery.txt";

        // Cassandra parameters
        String domain = "TRANSPORT";
        String vendor = "JUNIPER";
        String technology = "COMMON";
        String[] datalevels = {
            "L0_COMMON_ROUTER", "L1_COMMON_ROUTER", "L2_COMMON_ROUTER",
            "L3_COMMON_ROUTER", "L4_COMMON_ROUTER",
            "ROUTER_COMMON_ROUTER", "INTERFACE_COMMON_ROUTER"
        };

        // Date range: 01 Jan 2025 to 31 Aug 2025
        LocalDate startDate = LocalDate.of(2025, 1, 1);
        LocalDate endDate = LocalDate.of(2025, 8, 31);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                String dateStr = date.format(formatter);
                // Build DELETE query
                StringBuilder query = new StringBuilder();
                query.append("DELETE FROM pm.combinequarterlypm ")
                     .append("WHERE domain='").append(domain).append("' ")
                     .append("AND vendor='").append(vendor).append("' ")
                     .append("AND technology='").append(technology).append("' ")
                     .append("AND datalevel IN (");

                // Append datalevels
                for (int i = 0; i < datalevels.length; i++) {
                    query.append("'").append(datalevels[i]).append("'");
                    if (i < datalevels.length - 1) {
                        query.append(", ");
                    }
                }
                query.append(") ")
                     .append("AND date='").append(dateStr).append("';");

                // Write query to file
                writer.write(query.toString());
                writer.newLine();
            }

            System.out.println("Delete queries generated successfully: " + filePath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
