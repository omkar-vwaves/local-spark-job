package com.enttribe.custom.processor;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class TopologyTest implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TopologyTest.class);
    private static String globalVertexMaxTime = null;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile Successfully Processed!")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile Failed to Process!")
            .build();

    @Override
    public void run() {
        logger.info("DwdmAndRouterQueryProcessor131 thread started.");
    }

    // Replace the existing runSnippet(...) method with this version
    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) throws SQLException {

        logger.info("runSnippet invoked. cacheValue present: {} | initialConnectionNull: {}", (cacheValue != null),
                (connection == null));

        if (flowFile == null) {
            logger.info("Input FlowFile is null. Exiting runSnippet.");
            return null;
        }

        final String[] vertexMaxTimeHolder = new String[1]; // holder for extracted value

        // Read content of flowfile
        logger.info("About to read content of FlowFile to extract vertexMaxTime.");
        session.read(flowFile, in -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                logger.info("Reading FlowFile content to extract vertexMaxTime...");
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }

                // Full JSON string
                String jsonString = sb.toString();
                logger.info("FlowFile content length: {} characters", jsonString.length());

                // Parse JSON (using Jackson / org.json.simple)
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(jsonString);

                // Get the value
                if (node.has("vertexMaxTime")) {
                    vertexMaxTimeHolder[0] = node.get("vertexMaxTime").asText();
                    logger.info("vertexMaxTime extracted from FlowFile: {}", vertexMaxTimeHolder[0]);
                } else {
                    logger.warn("vertexMaxTime not found in FlowFile JSON. Will fallback to current UTC time.");
                }
            } catch (Exception e) {
                logger.error("Exception while reading FlowFile content: {}", e.getMessage(), e);
            }
        });

        String vertexMaxTime = vertexMaxTimeHolder[0]; // yeh actual value hogi from JSON

        // Example: log ya use karo
        if (vertexMaxTime == null || vertexMaxTime.isBlank()) {
            // fallback to current UTC datetime
            vertexMaxTime = java.time.LocalDateTime.now(java.time.Clock.systemUTC())
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            logger.info("Using fallback vertexMaxTime (UTC now): {}", vertexMaxTime);
        } else {
            logger.info("Using provided vertexMaxTime: {}", vertexMaxTime);
        }

        globalVertexMaxTime = vertexMaxTime;
        logger.info("globalVertexMaxTime set: {}", globalVertexMaxTime);

        // Connection connectionNew = null;
        // try {
        // logger.info("Ensuring JDBC connection is open before stages...");
        // connectionNew = ensureOpen(connection);

        // // 1️. Parent Stage
        // logger.info("Preparing to start 'parent' stage.");
        // connectionNew = ensureOpen(connectionNew);
        // logger.info("Starting 'parent' stage. Query window from date: {}",
        // globalVertexMaxTime);
        // boolean parentSuccess = executeStage("parent", getParentQueryForLLDP(),
        // flowFile, session, connectionNew);
        // logger.info("'parent' stage completed. Success: {}", parentSuccess);
        // if (!parentSuccess) {
        // logger.info("Parent stage failed or was incomplete. Returning FlowFile.");
        // return flowFile;
        // }

        // // 2️. Neighbour Stage
        // logger.info("Preparing to start 'neighbour' stage.");
        // connectionNew = ensureOpen(connectionNew);
        // logger.info("Starting 'neighbour' stage. Query window from date: {}",
        // globalVertexMaxTime);
        // boolean neighbourSuccess = executeStage("neighbour",
        // getNeighbourQueryForLLDP(), flowFile, session,
        // connectionNew);
        // logger.info("'neighbour' stage completed. Success: {}", neighbourSuccess);
        // if (!neighbourSuccess) {
        // logger.info("Neighbour stage failed or was incomplete. Returning FlowFile.");
        // return flowFile;
        // }

        // // 3️. Child Stage
        // logger.info("Preparing to start 'child' stage.");
        // connectionNew = ensureOpen(connectionNew);
        // logger.info("Starting 'child' stage. Query window from date: {}",
        // globalVertexMaxTime);
        // boolean childSuccess = executeStage("child", getLinkQueryForLLDP(), flowFile,
        // session, connectionNew);
        // logger.info("'child' stage completed. Success: {}", childSuccess);
        // if (!childSuccess) {
        // logger.info("Child stage failed or was incomplete. Returning FlowFile.");
        // return flowFile;
        // }

        // // All success
        // flowFile = session.putAttribute(flowFile, "topology.status", "ALL_SUCCESS");
        // logger.info("All stages completed successfully. Transferring to
        // REL_SUCCESS.");
        // session.transfer(flowFile, REL_SUCCESS);

        // } catch (Exception e) {
        // logger.error("Exception in DwdmProcessor: {}", e.getMessage(), e);
        // flowFile = session.putAttribute(flowFile, "topology.status", "EXCEPTION");
        // flowFile = session.putAttribute(flowFile, "topology.error", e.getMessage());
        // session.transfer(flowFile, REL_FAILURE);
        // } finally {
        // try {
        // if (connectionNew != null && !connectionNew.isClosed()) {
        // logger.info("Closing JDBC connection after stages.");
        // connectionNew.close();
        // } else {
        // logger.info("No JDBC connection to close after stages.");
        // }
        // } catch (SQLException ignore) {
        // logger.warn("Exception while closing JDBC connection: {}",
        // ignore.getMessage());
        // }
        // }

        flowFile = session.putAttribute(flowFile, "vertexMaxTime", globalVertexMaxTime);
        logger.info("Added attribute vertexMaxTime to FlowFile and transferred to REL_SUCCESS.");
        session.transfer(flowFile, REL_SUCCESS);

        logger.info("runSnippet completed for FlowFile.");
        return flowFile;
    }

    // Add inside class
    private Connection ensureOpen(Connection current) {
        logger.info("Checking if JDBC connection is open...");
        try {
            if (current == null || current.isClosed()) {
                logger.info("JDBC connection is null/closed. Creating a new connection...");
                return getJDBCConnection(current);
            }
            logger.trace("Reusing existing open JDBC connection.");
            return current;
        } catch (SQLException e) {
            logger.warn("Error while checking connection state: {}. Attempting to create a new connection...",
                    e.getMessage());
            return getJDBCConnection(current);
        }
    }

    private static Connection getJDBCConnection(Connection connection) {
        String jdbcUrl = "jdbc:mysql://mysql-nstdb-cluster.nstdb.svc.cluster.local:6446/LCM?autoReconnect=true&permitMysqlScheme=true";
        String username = "LCM";
        String password = "lcm%lcm2";
        logger.info("Attempting to establish new JDBC connection to: {}", jdbcUrl);
        try {
            long start = System.nanoTime();
            Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            long tookMs = (System.nanoTime() - start) / 1_000_000;
            logger.info("JDBC connection established in {} ms to URL: {}", tookMs, jdbcUrl);
            return conn;
        } catch (SQLException e) {
            logger.error("Failed to obtain JDBC connection: {}", e.getMessage(), e);
            return connection;
        }
    }

    private boolean executeStage(String stageName, String query, FlowFile flowFile,
            ProcessSession session, Connection connection) {

        logger.info("executeStage called for stage: {}", stageName);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            logger.info("Creating statement for stage: {}", stageName);
            stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(1000);
            logger.info("Executing '{}' stage query...", stageName);
            logger.info("Query: {}", query);
            long queryStart = System.nanoTime();
            rs = stmt.executeQuery(query);
            long queryMs = (System.nanoTime() - queryStart) / 1_000_000;
            logger.info("'{}' stage query executed in {} ms", stageName, queryMs);

            List<Map<String, Object>> dataList = getListOfMapFromResultSet(rs);
            flowFile = session.putAttribute(flowFile, stageName + ".record.count", String.valueOf(dataList.size()));
            logger.info("'{}' stage fetched {} records", stageName, dataList.size());

            if (dataList.isEmpty()) {
                flowFile = session.putAttribute(flowFile, stageName + ".status", "NO_RECORDS");
                logger.warn("{} stage returned 0 records. Skipping to next stage.", stageName);
                return true;
            }

            // Batch send 1000
            ObjectMapper mapper = new ObjectMapper();
            int batchSize = 1000;
            for (int i = 0; i < dataList.size(); i += batchSize) {
                int end = Math.min(i + batchSize, dataList.size());
                List<Map<String, Object>> batch = dataList.subList(i, end);

                String payload = mapper.writeValueAsString(batch);
                int batchNumber = (i / batchSize) + 1;
                logger.info("{} stage batch {} - {} records | payloadSize={} bytes", stageName, batchNumber,
                        batch.size(), payload.getBytes(StandardCharsets.UTF_8).length);

                long apiStart = System.nanoTime();
                logger.info("Calling API for {} stage, batch {}", stageName, batchNumber);
                boolean apiSuccess = callAPI(payload);
                long apiMs = (System.nanoTime() - apiStart) / 1_000_000;
                logger.info("{} stage batch {} API call duration: {} ms", stageName, batchNumber, apiMs);
                if (!apiSuccess) {
                    logger.error("API call failed for {} stage, batch {}", stageName, batchNumber);
                    flowFile = session.putAttribute(flowFile, stageName + ".status", "API_FAILED");
                    session.transfer(flowFile, REL_FAILURE);
                    return false;
                }
            }

            flowFile = session.putAttribute(flowFile, stageName + ".status", "SUCCESS");
            logger.info("'{}' stage completed successfully.", stageName);
            return true;

        } catch (Exception e) {
            logger.error("{} stage exception: {}", stageName, e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, stageName + ".status", "EXCEPTION");
            flowFile = session.putAttribute(flowFile, stageName + ".error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return false;

        } finally {
            try {
                if (rs != null) {
                    logger.info("Closing ResultSet for stage: {}", stageName);
                    rs.close();
                }
            } catch (SQLException ignore) {
                logger.warn("Exception while closing ResultSet: {}", ignore.getMessage());
            }
            try {
                if (stmt != null) {
                    logger.info("Closing Statement for stage: {}", stageName);
                    stmt.close();
                }
            } catch (SQLException ignore) {
                logger.warn("Exception while closing Statement: {}", ignore.getMessage());
            }
        }
    }

    private boolean callAPI(String payload) {
        try {
            String apiUrl = "http://topology-service/topology/graphdb/createMultipleVertices";
            HttpClient client = HttpClient.newHttpClient();
            logger.info("Calling API: {} | payloadSize={} bytes", apiUrl,
                    payload.getBytes(StandardCharsets.UTF_8).length);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            long start = System.nanoTime();
            logger.info("Sending HTTP request to API: {}", apiUrl);
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            long tookMs = (System.nanoTime() - start) / 1_000_000;
            logger.info("API call completed in {} ms. Response Code: {}, Body: {}", tookMs, response.statusCode(),
                    response.body());
            return response.statusCode() == 200;

        } catch (Exception e) {
            logger.error("API call exception: {}", e.getMessage(), e);
            return false;
        }
    }

    private List<Map<String, Object>> getListOfMapFromResultSet(ResultSet resultSet) throws SQLException {
        long start = System.nanoTime();
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols = metaData.getColumnCount();
        logger.info("Transforming ResultSet to List<Map>. Column count: {}", cols);
        int rowCount = 0;
        while (resultSet.next()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++)
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            list.add(map);
            rowCount++;
        }
        long tookMs = (System.nanoTime() - start) / 1_000_000;
        logger.info("ResultSet transformed: {} rows in {} ms", rowCount, tookMs);
        return list;
    }

    // SQL queries of DWDM AND ROUTER
    private String getParentQueryForLLDP() {
        logger.info("Building parent query for LLDP with globalVertexMaxTime: {}", globalVertexMaxTime);
        return "SELECT 'Router' AS relation, ne.ID AS id, ne.NE_NAME AS neName, ne.LATITUDE AS latitude, ne.LONGITUDE AS longitude, ne.NE_SOURCE AS neSource, ne.MODIFICATION_TIME AS ModificationTime, ne.NE_TYPE AS neType, ne.NE_CATEGORY AS neCategory, ne.VENDOR AS vendor, ne.CKT_ID AS cktId, ne.FREQUENCY AS neFrequency, ne.IS_DELETED AS isDeleted, ne.STATUS AS neStatus, ne.TECHNOLOGY AS technology, ne.NE_CATEGORY AS srcCategory, ne.NE_CATEGORY AS destCategory, ne.DOMAIN AS domain, ne.SOFTWARE_VERSION AS softwareVersion, ne.NE_ID AS neId, ne.PM_EMS_ID AS pmEmsId, ne.FM_EMS_ID AS fmEmsId, ne.CM_EMS_ID AS cmEmsId, ne.IPV4 AS ipv4, ne.IPV6 AS ipv6, ne.MODEL AS model, ne.MAC_ADDRESS AS macAddress, ne.FRIENDLY_NAME AS friendlyName, ne.DUPLEX AS duplex, ne.RADIUS AS radius, ne.CATEGORY AS category, ne.HOST_NAME AS hostname, ne.FQDN AS fqdn, ne.UUID AS uuid, ne.SERIAL_NUMBER AS serialNumber, ne.INSERVICE_DATE AS inServiceDate, l1.id AS l1Id, l1.GEO_CODE AS l1GeoCode, l1.LONGITUDE AS l1Long, l1.LATITUDE AS l1Lat, l2.ID AS l2Id, l2.GEO_CODE AS l2GeoCode, l2.LONGITUDE AS l2Long, l2.LATITUDE AS l2Lat, l3.ID AS l3Id, l3.GEO_CODE AS l3GeoCode, l3.LONGITUDE AS l3Long, l3.LATITUDE AS l3Lat, l4.ID AS l4Id, l4.GEO_CODE AS l4GeoCode, l4.LONGITUDE AS l4Long, l4.LATITUDE AS l4Lat, neighbourNE.NE_ID AS neighbourNEId, neighbourNE.NE_NAME AS neighbourNEName, neighbourNE.LATITUDE AS neighbourNELat, neighbourNE.LONGITUDE AS neighbourNELong, parentNE.NE_NAME AS parentNE, neighbourParentNE.NE_NAME AS neighbourParentNEName, neighbourParentNE.LONGITUDE AS neighbourParentNELong, neighbourParentNE.LATITUDE AS neighbourParentNELat FROM NETWORK_ELEMENT ne LEFT JOIN PRIMARY_GEO_L1 l1 ON ne.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN PRIMARY_GEO_L2 l2 ON ne.GEOGRAPHY_L2_ID_FK = l2.id LEFT JOIN PRIMARY_GEO_L3 l3 ON ne.GEOGRAPHY_L3_ID_FK = l3.id LEFT JOIN PRIMARY_GEO_L4 l4 ON ne.GEOGRAPHY_L4_ID_FK = l4.id LEFT JOIN NETWORK_ELEMENT parentNE ON ne.PARENT_NE_ID_FK = parentNE.ID LEFT JOIN NETWORK_ELEMENT neighbourNE ON ne.NETWORK_ELEMENT_ID_FK = neighbourNE.ID LEFT JOIN NETWORK_ELEMENT neighbourParentNE ON neighbourNE.PARENT_NE_ID_FK = neighbourParentNE.ID WHERE ne.IS_DELETED = 0 AND ne.NE_TYPE IN ('GNE','ILA','MDWDM','NIC','NODE','OADM','OTN','ROUTER') AND ne.NE_CATEGORY IN ('GATEWAY_INTERFACE','OPTICAL_AMPLIFIERS','DWDM','OTN','NODE','ROUTER') AND DATE(ne.MODIFICATION_TIME) >= '"
                + globalVertexMaxTime + "'";
    }

    private String getNeighbourQueryForLLDP() {
        logger.info("Building neighbour query for LLDP with globalVertexMaxTime: {}", globalVertexMaxTime);
        return "SELECT DISTINCT 'interface' AS relation, src.ID AS id, src.NE_NAME AS neName, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.NE_SOURCE AS neSource, l.MODIFICATION_TIME AS ModificationTime, src.NE_TYPE AS neType, src.NE_CATEGORY AS neCategory, src.VENDOR AS vendor, src.CKT_ID AS cktId, src.FREQUENCY AS neFrequency, src.IS_DELETED AS isDeleted, src.STATUS AS neStatus, src.TECHNOLOGY AS technology, src.NE_CATEGORY AS srcCategory, src.DOMAIN AS domain, src.SOFTWARE_VERSION AS softwareVersion, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.DUPLEX AS duplex, src.RADIUS AS radius, src.CATEGORY AS category, l.LLDP_LINK_NAME AS hostname, src.FQDN AS fqdn, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, src.INSERVICE_DATE AS inServiceDate, l1.id AS l1Id, l1.GEO_CODE AS l1GeoCode, l1.LONGITUDE AS l1Long, l1.LATITUDE AS l1Lat, l2.ID AS l2Id, l2.GEO_CODE AS l2GeoCode, l2.LONGITUDE AS l2Long, l2.LATITUDE AS l2Lat, l3.ID AS l3Id, l3.GEO_CODE AS l3GeoCode, l3.LONGITUDE AS l3Long, l3.LATITUDE AS l3Lat, l4.ID AS l4Id, l4.GEO_CODE AS l4GeoCode, l4.LONGITUDE AS l4Long, l4.LATITUDE AS l4Lat, parentSrc.NE_NAME AS parentNE FROM LLDP_LINK l JOIN NETWORK_ELEMENT src ON l.SOURCE_INTERFACE_NE_ID = src.ID or l.DESTINATION_INTERFACE_NE_ID = src.ID LEFT JOIN PRIMARY_GEO_L1 l1 ON src.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN PRIMARY_GEO_L2 l2 ON src.GEOGRAPHY_L2_ID_FK = l2.id LEFT JOIN PRIMARY_GEO_L3 l3 ON src.GEOGRAPHY_L3_ID_FK = l3.id LEFT JOIN PRIMARY_GEO_L4 l4 ON src.GEOGRAPHY_L4_ID_FK = l4.id LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID WHERE parentSrc.IS_DELETED = 0 AND (parentSrc.NE_TYPE IN ('ROUTER') OR parentSrc.NE_CATEGORY IN ('ROUTER')) AND DATE(l.MODIFICATION_TIME) >= '"
                + globalVertexMaxTime + "'";
    }

    private String getLinkQueryForLLDP() {
        logger.info("Building child/link query for LLDP with globalVertexMaxTime: {}", globalVertexMaxTime);
        return "SELECT 'lldp' AS relation, src.ID AS id, src.NE_NAME AS neName, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.NE_SOURCE AS neSource, src.NE_TYPE AS neType, src.NE_CATEGORY AS neCategory, src.NE_CATEGORY AS srcCategory, src.NE_CATEGORY AS destCategory,src.VENDOR AS vendor, link.MODIFICATION_TIME AS ModificationTime, src.CKT_ID AS cktId, src.FREQUENCY AS neFrequency, src.IS_DELETED AS isDeleted, src.STATUS AS neStatus, src.TECHNOLOGY AS technology, src.DOMAIN AS domain, src.SOFTWARE_VERSION AS softwareVersion, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.DUPLEX AS duplex, src.RADIUS AS radius, src.CATEGORY AS category, link.LLDP_LINK_NAME AS hostname, src.FQDN AS fqdn, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, src.INSERVICE_DATE AS inServiceDate, l1.ID AS l1Id, l1.GEO_CODE AS l1GeoCode, l1.LONGITUDE AS l1Long, l1.LATITUDE AS l1Lat, dest.NE_ID AS neighbourNEId, dest.NE_NAME AS neighbourNEName, dest.LATITUDE AS neighbourNELat, dest.LONGITUDE AS neighbourNELong, parentSrc.NE_NAME AS parentNE, parentDest.NE_NAME AS neighbourParentNEName, parentDest.LONGITUDE AS neighbourParentNELong, parentDest.LATITUDE AS neighbourParentNELat FROM LLDP_LINK link JOIN NETWORK_ELEMENT src ON src.ID = link.SOURCE_INTERFACE_NE_ID JOIN NETWORK_ELEMENT dest ON dest.ID = link.DESTINATION_INTERFACE_NE_ID LEFT JOIN PRIMARY_GEO_L1 l1 ON src.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID LEFT JOIN NETWORK_ELEMENT parentDest ON dest.PARENT_NE_ID_FK = parentDest.ID WHERE link.IS_DELETED = 0 AND src.IS_DELETED = 0 AND dest.IS_DELETED = 0 AND DATE(link.MODIFICATION_TIME) >= '"
                + globalVertexMaxTime + "'";
    }
}