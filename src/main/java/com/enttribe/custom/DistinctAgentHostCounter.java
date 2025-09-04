package com.enttribe.custom;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DistinctAgentHostCounter {

    public static void main(String[] args) {
        String filePath = "/Users/ent-00356/Desktop/test-juniper.json";
        Map<String, Integer> agentHostCounts = getDistinctAgentHostCounts(filePath);

        System.out.println("Distinct agent_host values and their counts:");
        agentHostCounts.forEach((host, count) -> 
            System.out.println("agent_host: " + host + ", count: " + count)
        );
        // System.out.println("agentHostCounts Size: {}", String.valueOf(agentHostCounts.size()));
    }

    public static Map<String, Integer> getDistinctAgentHostCounts(String filePath) {
        Map<String, Integer> hostCountMap = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    JsonNode root = mapper.readTree(line);
                    JsonNode tags = root.get("tags");
                    if (tags != null && tags.has("agent_host")) {
                        String agentHost = tags.get("agent_host").asText();
                        hostCountMap.put(agentHost, hostCountMap.getOrDefault(agentHost, 0) + 1);
                    }
                } catch (Exception e) {
                    System.err.println("Skipping invalid JSON line: " + e.getMessage());
                }
            });
        } catch (IOException e) {
            System.err.println("Failed to read file: " + e.getMessage());
        }

        return hostCountMap;
    }
}
