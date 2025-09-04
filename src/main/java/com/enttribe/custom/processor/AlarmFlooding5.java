package com.enttribe.custom.processor;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class AlarmFlooding5 implements Runnable {

    public static final boolean IS_LOCAL = false;
    private Set<String> errorList = new HashSet<>();
    private Map<String, String> successFlowFileAttributes = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(AlarmFlooding5.class);

    @Override
    public void run() {

    }

    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) {

        logger.info("🔍 Running Alarm Flooding BlackList Processor...");

        try {
            flowFile = startFMAlarmFloodingBlackList(flowFile, session, context, connection, cacheValue);

            handleError(session, flowFile);
            updateFlowFileAttributes(session, flowFile);

            session.transfer(flowFile, new Relationship.Builder()
                    .name("success")
                    .description("Successfully Processed FlowFile!")
                    .build());

            logger.info("FlowFile Transferred to Success Relationship!");

        } catch (Exception e) {
            logger.error("Exception Occured in runSnippet Method, Message: " + e.getMessage());
            createFailureJson(session, flowFile, e.getMessage());

        } finally {
            logger.info("Closing Connection...");
            closeConnection(connection);
            logger.info("Connection Closed!");
        }

        logger.info("🔍 Alarm Flooding BlackList Processor Completed!");

        return flowFile;

    }

    private void closeConnection(java.sql.Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("❌ Exception Occured in closeConnection Method, Message: " + e.getMessage());
        }
    }

    private void updateFlowFileAttributes(ProcessSession session, FlowFile flowFile) {

        logger.info("🔍 Success Flow File Attributes: " + successFlowFileAttributes.toString());

        for (Map.Entry<String, String> entry : successFlowFileAttributes.entrySet()) {
            session.putAttribute(flowFile, entry.getKey(), entry.getValue());
        }
    }

    private void handleError(ProcessSession session, FlowFile flowFile) {

        logger.info("🔍 Error List: " + errorList.toString());

        if (!errorList.isEmpty()) {

            FlowFile failureFlowFile = session.create(flowFile);
            String errorMessage = "Exception in BlackList IPs: " + errorList.toString();
            createFailureJson(session, failureFlowFile, errorMessage);
        } else {
            logger.info("🔍 No Errors Found in Error List");
        }
    }

    private void createFailureJson(ProcessSession session, FlowFile failureFlowFile, String errorMessage) {

        JSONObject errorLogs = new JSONObject();

        errorLogs.put("errorDescription", errorMessage);
        errorLogs.put("processorName", "ALARM FLOODING BLACKLIST");
        errorLogs.put("processorType", "JavaSnippet");

        session.putAttribute(failureFlowFile, "ErrorLogs", errorLogs.toString());

        session.transfer(
                failureFlowFile,
                new Relationship.Builder()
                        .name("failure")
                        .description("FlowFiles that fail for error: " + errorMessage)
                        .build());

        logger.info("Transferred FlowFile to Failure Relationship Due to Error: " + errorMessage);

    }

    private FlowFile startFMAlarmFloodingBlackList(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) {

        logger.info("🔍 Starting Alarm Flooding BlackList Rule Query...");

        String alarmFloodindRuleQuery = "SELECT r.ALARM_FLOODING_BLACKLIST_RULE_ID_PK, r.TIME_WINDOW_SECONDS, r.ALARM_THRESHOLD_COUNT, n.TO_EMAILS, n.CC_EMAILS, n.BCC_EMAILS FROM ALARM_FLOODING_BLACKLIST_RULE r LEFT JOIN ALARM_FLOODING_BLACKLIST_RULE_NOTIFICATION n ON r.ALARM_FLOODING_BLACKLIST_RULE_ID_PK = n.ALARM_FLOODING_BLACKLIST_RULE_ID_FK WHERE r.DOMAIN = ? AND r.VENDOR = ? AND r.TECHNOLOGY = ? AND r.IS_ENABLED = TRUE";

        logger.info("🔍 Alarm Flooding BlackList Rule Query: " + alarmFloodindRuleQuery);

        String domain = flowFile.getAttribute("domain");
        String vendor = flowFile.getAttribute("vendor");
        String technology = flowFile.getAttribute("technology");

        logger.info("🔍 Domain: " + domain);
        logger.info("🔍 Vendor: " + vendor);
        logger.info("🔍 Technology: " + technology);

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            preparedStatement = connection.prepareStatement(alarmFloodindRuleQuery);
            preparedStatement.setString(1, domain);
            preparedStatement.setString(2, vendor);
            preparedStatement.setString(3, technology);

            resultSet = preparedStatement.executeQuery();

            boolean hasRecords = false;
            while (resultSet.next()) {
                hasRecords = true;

                String alarmFloodindRuleId = resultSet.getString("ALARM_FLOODING_BLACKLIST_RULE_ID_PK");
                String timeWindowSeconds = resultSet.getString("TIME_WINDOW_SECONDS");
                String alarmThresholdCount = resultSet.getString("ALARM_THRESHOLD_COUNT");
                String toEmails = resultSet.getString("TO_EMAILS");
                String ccEmails = resultSet.getString("CC_EMAILS");
                String bccEmails = resultSet.getString("BCC_EMAILS");

                Map<String, String> resultMap = new HashMap<>();
                resultMap.put("ALARM_FLOODING_BLACKLIST_RULE_ID_PK", alarmFloodindRuleId);
                resultMap.put("TIME_WINDOW_SECONDS", timeWindowSeconds);
                resultMap.put("ALARM_THRESHOLD_COUNT", alarmThresholdCount);
                resultMap.put("TO_EMAILS", toEmails);
                resultMap.put("CC_EMAILS", ccEmails);
                resultMap.put("BCC_EMAILS", bccEmails);

                processResultMap(resultMap, flowFile);
            }

            if (!hasRecords) {

                logger.info("No Alarm Flooding BlackList Rule Found!");
                errorList.add("No Alarm Flooding BlackList Rule Found!");
                return flowFile;
            }

        } catch (SQLException e) {
            logger.error("Exception Occured in startFMAlarmFloodingBlackList Method, Message: " + e.getMessage());
            errorList.add(e.getMessage());
            return flowFile;
        }

        return flowFile;
    }

    private void processResultMap(Map<String, String> resultMap, FlowFile flowFile) {

        logger.info("🔍 Processing Result Map: " + resultMap.toString());

        String getAllStatsUrl = flowFile.getAttribute("GET_ALL_STATS_URL");
        logger.info("🔍 Get All Stats URL: " + getAllStatsUrl);

        String getAllStatsResponse = null;

        if (IS_LOCAL) {
            getAllStatsResponse = flowFile.getAttribute("GET_ALL_STATS_RESPONSE");
        } else {
            getAllStatsResponse = getGetAllStatsResponse(getAllStatsUrl);
        }

        logger.info("🔍 Get All Stats Response: " + getAllStatsResponse);

        JSONObject getAllStatsResponseJson = new JSONObject(getAllStatsResponse);

        if (getAllStatsResponseJson.isEmpty() || getAllStatsResponseJson == null) {
            logger.info("🔍 Get All Stats Response is Empty!");
            errorList.add("Get All Stats Response is Empty!");
            return;
        } else {
            logger.info("🔍 Get All Stats Response is Not Empty!");
        }

        JSONObject stats = getAllStatsResponseJson.getJSONObject("stats");

        int minTotalCount = Integer.MAX_VALUE;
        int maxTotalCount = Integer.MIN_VALUE;

        Iterator<String> keys = stats.keys();
        while (keys.hasNext()) {
            String ip = keys.next();
            int totalCount = stats.getJSONObject(ip).getInt("totalCount");

            if (totalCount > maxTotalCount) {
                maxTotalCount = totalCount;
            }
            if (totalCount < minTotalCount) {
                minTotalCount = totalCount;
            }
        }

        logger.info("🟢 maxTotalCount = " + maxTotalCount);
        logger.info("🟡 minTotalCount = " + minTotalCount);
        resultMap.put("MAX_THRESHOLD_COUNT", String.valueOf(maxTotalCount));
        resultMap.put("MIN_THRESHOLD_COUNT", String.valueOf(minTotalCount));

        boolean isWindowEnabled = getAllStatsResponseJson.getBoolean("isWindowEnabled");

        logger.info("🔍 Is Window Enabled: " + isWindowEnabled);

        if (isWindowEnabled) {

            String alarmThresholdCount = resultMap.get("ALARM_THRESHOLD_COUNT");
            List<String> blackIps = new ArrayList<>();

            for (String ip : stats.keySet()) {
                JSONObject ipStats = stats.getJSONObject(ip);
                int totalCount = ipStats.getInt("totalCount");

                logger.info("🔍 Total Count: " + totalCount);
                logger.info("🔍 Alarm Threshold Count: " + alarmThresholdCount);

                if (totalCount >= Integer.parseInt(alarmThresholdCount)) {
                    blackIps.add(ip);
                } else {
                    logger.info("🔍 Total Count is Less than Alarm Threshold Count");
                }
            }

            List<String> blackIpsToAdd = new ArrayList<>();

            try {
                if (blackIps != null && !blackIps.isEmpty()) {

                    logger.info("🔍 Black IPs: " + blackIps.toString());

                    String blackAllIpListUrl = flowFile.getAttribute("BLACK_ALL_IP_LIST_URL");
                    logger.info("🔍 Black All IP List URL: " + blackAllIpListUrl);

                    String blackAllIpListResponse = null;
                    if (IS_LOCAL) {
                        blackAllIpListResponse = flowFile.getAttribute("BLACK_ALL_IP_LIST_RESPONSE");
                    } else {
                        blackAllIpListResponse = getBlackAllIpListResponse(blackAllIpListUrl, blackIps);
                    }

                    if (blackAllIpListResponse == null || blackAllIpListResponse.isEmpty()) {
                        errorList.add("Black All IP List Response is Empty!");
                        return;
                    }

                    logger.info("🔍 Black All IP List Response: " + blackAllIpListResponse);

                    JSONObject blackAllIpListResponseJson = new JSONObject(blackAllIpListResponse);

                    JSONArray blackIpsJson = blackAllIpListResponseJson.getJSONArray("ips");

                    for (int i = 0; i < blackIpsJson.length(); i++) {
                        String blackIp = blackIpsJson.getString(i);
                        blackIpsToAdd.add(blackIp);
                    }

                    if (blackIpsToAdd.size() > 0) {

                        logger.info("🔍 Black IPs to Add: " + blackIpsToAdd.toString());

                        successFlowFileAttributes.put("DOMAIN", flowFile.getAttribute("DOMAIN"));
                        successFlowFileAttributes.put("VENDOR", flowFile.getAttribute("VENDOR"));
                        successFlowFileAttributes.put("TECHNOLOGY", flowFile.getAttribute("TECHNOLOGY"));

                        successFlowFileAttributes.put("TIME_WINDOW_SECONDS", resultMap.get("TIME_WINDOW_SECONDS"));
                        successFlowFileAttributes.put("MIN_THRESHOLD_COUNT", resultMap.get("ALARM_THRESHOLD_COUNT"));
                        successFlowFileAttributes.put("MAX_THRESHOLD_COUNT", resultMap.get("MAX_THRESHOLD_COUNT"));

                        successFlowFileAttributes.put("ALARM_FLOODING_BLACKLIST_RULE_ID_PK",
                                resultMap.get("ALARM_FLOODING_BLACKLIST_RULE_ID_PK"));

                        successFlowFileAttributes.put("BLACK_IPS_TO_ADD", String.join(",", blackIpsToAdd));
                        successFlowFileAttributes.put("TO_EMAILS", resultMap.get("TO_EMAILS"));
                        successFlowFileAttributes.put("CC_EMAILS", resultMap.get("CC_EMAILS"));
                        successFlowFileAttributes.put("BCC_EMAILS", resultMap.get("BCC_EMAILS"));

                        String jdbcBatchInsertQuery = buildJDBCBatchInsertQuery(blackIpsToAdd, resultMap);
                        successFlowFileAttributes.put("JDBC_BATCH_INSERT_QUERY", jdbcBatchInsertQuery);

                    } else {
                        logger.info("🔍 No IPs to be Added to the BlackList");
                        errorList.add("No IPs to be Added to the BlackList");
                        return;
                    }

                } else {
                    logger.info("🔍 No IPs to be Added to the BlackList!");
                    errorList.add("No IPs to be Added to the BlackList!");
                    return;
                }
            } catch (Exception e) {
                logger.error("⚠️ Exception Occured in Process Black IPs, Message: " + e.getMessage()
                        + ", Error: " + e);
            }
        } else {
            logger.info("---------- Window-Based Monitoring is Inactive ----------");
            errorList.add("Window-Based Monitoring is Inactive");
            return;
        }

    }

    private String buildJDBCBatchInsertQuery(List<String> blackIpsToAdd, Map<String, String> resultMap) {

        logger.info("🔍 Building JDBC Batch Insert Query...");

        try {
            if (blackIpsToAdd == null || blackIpsToAdd.isEmpty() || resultMap == null) {
                throw new IllegalArgumentException("Input parameters cannot be null or empty");
            }

            String alarmFloodingRuleId = resultMap.get("ALARM_FLOODING_BLACKLIST_RULE_ID_PK");
            String status = "BLACKLISTED";
            String thresholdCount = resultMap.get("ALARM_THRESHOLD_COUNT");

            if (alarmFloodingRuleId == null || thresholdCount == null) {
                errorList.add("Required Keys are Missing in Result Map");
                return null;
            }

            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("INSERT INTO BLACKLIST_NODE_DETAILS ")
                    .append("(ALARM_FLOODING_BLACKLIST_RULE_ID_FK, NODE_NAME, STATUS, THRESHOLD_COUNT, BLACKLIST_TIME) VALUES ");

            for (String blackIp : blackIpsToAdd) {
                queryBuilder.append("(")
                        .append(alarmFloodingRuleId).append(", ")
                        .append("'").append(blackIp).append("', ")
                        .append("'").append(status).append("', ")
                        .append(thresholdCount).append(", ")
                        .append("NOW()),");
            }

            queryBuilder.setLength(queryBuilder.length() - 1);

            logger.info("🔍 Batch Insert Query: " + queryBuilder.toString());

            return queryBuilder.toString();

        } catch (Exception e) {
            logger.error("Error Building Batch Insert Query: " + e.getMessage());
            errorList.add(e.getMessage());
            return null;
        }
    }

    private String getGetAllStatsResponse(String getAllStatsUrl) {
        try {
            logger.info("Starting to Get All Stats from URL: {}", getAllStatsUrl);

            HttpClient httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(getAllStatsUrl))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .GET()
                    .build();

            logger.info("HTTP Request Created for URL: {}", getAllStatsUrl);

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            logger.info("Received Response with Status Code: {} for URL: {}",
                    response.statusCode(), getAllStatsUrl);

            if (response.statusCode() == 200) {
                logger.info("Successfully Retrieved Stats Data : {}", response.body());
                return response.body();
            } else {
                logger.error("Failed to Retrieve Stats Data. Status code: {}, Response: {}, URL: {}",
                        response.statusCode(), response.body(), getAllStatsUrl);
                errorList.add("Failed to Retrieve Stats Data. Status code: " + response.statusCode() + ", Response: "
                        + response.body() + ", URL: " + getAllStatsUrl);
                return null;
            }

        } catch (Exception e) {
            logger.error("Error while Retrieving Stats Data from URL: {}. Error: {}",
                    getAllStatsUrl, e.getMessage(), e);
            errorList.add("Error while Retrieving Stats Data from URL: " + getAllStatsUrl + ", Error: " + e.getMessage());
            return null;
        }
    }

    private String getBlackAllIpListResponse(String blackAllIpListUrl, List<String> blackIps) {
        try {
            logger.info("Starting to Blacklist IPs from URL: {}", blackAllIpListUrl);

            HttpClient httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(blackAllIpListUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(new JSONArray(blackIps).toString()))
                    .build();

            logger.info("HTTP Request Created for URL: {}", blackAllIpListUrl);

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            logger.info("Received Response with Status Code: {} for URL: {}",
                    response.statusCode(), blackAllIpListUrl);

            if (response.statusCode() == 200) {
                logger.info("Successfully Blacklisted IPs. Response: {}", response.body());
                return response.body();
            } else {
                logger.error("Failed to Blacklist IPs. Status code: {}, Response: {}, URL: {}",
                        response.statusCode(), response.body(), blackAllIpListUrl);
                errorList.add("Failed to Blacklist IPs. Status code: " + response.statusCode() + ", Response: "
                        + response.body() + ", URL: " + blackAllIpListUrl);
                return null;
            }

        } catch (Exception e) {
            logger.error("Error while Blacklisting IPs from URL: {}. Error: {}",
                    blackAllIpListUrl, e.getMessage(), e);
            errorList.add("Error while Blacklisting IPs from URL: " + blackAllIpListUrl + ", Error: " + e.getMessage());
            return null;
        }
    }
}
