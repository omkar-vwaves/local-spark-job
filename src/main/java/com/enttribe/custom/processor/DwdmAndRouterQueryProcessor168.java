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

public class DwdmAndRouterQueryProcessor168 implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DwdmAndRouterQueryProcessor168.class);
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
    }

    // Replace the existing runSnippet(...) method with this version
    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) throws SQLException {

        if (flowFile == null)
            return null;

        final String[] vertexMaxTimeHolder = new String[1]; // holder for extracted value

        // Read content of flowfile
        session.read(flowFile, in -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }

                // Full JSON string
                String jsonString = sb.toString();

                // Parse JSON (using Jackson / org.json.simple)
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(jsonString);

                // Get the value
                if (node.has("vertexMaxTime")) {
                    vertexMaxTimeHolder[0] = node.get("vertexMaxTime").asText();
                }
            }
        });

        String vertexMaxTime = vertexMaxTimeHolder[0]; // yeh actual value hogi from JSON

        // Example: log ya use karo
        if (vertexMaxTime == null || vertexMaxTime.isBlank()) {
            // fallback to current UTC datetime
            vertexMaxTime = java.time.LocalDateTime.now(java.time.Clock.systemUTC())
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

        globalVertexMaxTime = vertexMaxTime;

        Connection connectionNew = null;
        try {
            connectionNew = ensureOpen(connection);

            // 1️. Parent Stage
            connectionNew = ensureOpen(connectionNew);
            boolean parentSuccess = executeStage("parent", getParentQueryForLLDP(),
                    flowFile, session, connectionNew);
            if (!parentSuccess)
                return flowFile;

            // 2️. Neighbour Stage
            connectionNew = ensureOpen(connectionNew);
            boolean neighbourSuccess = executeStage("neighbour",
                    getNeighbourQueryForLLDP(), flowFile, session,
                    connectionNew);
            if (!neighbourSuccess)
                return flowFile;

            // 3️. Child Stage
            connectionNew = ensureOpen(connectionNew);
            boolean childSuccess = executeStage("child", getLinkQueryForLLDP(), flowFile,
                    session, connectionNew);
            if (!childSuccess)
                return flowFile;

            // All success
            flowFile = session.putAttribute(flowFile, "topology.status", "ALL_SUCCESS");
            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            logger.error("Exception in DwdmProcessor: {}", e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "topology.status", "EXCEPTION");
            flowFile = session.putAttribute(flowFile, "topology.error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            try {
                if (connectionNew != null && !connectionNew.isClosed()) {
                    connectionNew.close();
                }
            } catch (SQLException ignore) {
            }
        }

        flowFile = session.putAttribute(flowFile, "vertexMaxTime", vertexMaxTime);
        session.transfer(flowFile, REL_SUCCESS);

        return flowFile;
    }

    // Add inside class
    private Connection ensureOpen(Connection current) {
        try {
            if (current == null || current.isClosed()) {
                return getJDBCConnection(current);
            }
            return current;
        } catch (SQLException e) {
            return getJDBCConnection(current);
        }
    }

    private static Connection getJDBCConnection(Connection connection) {
        String jdbcUrl = "jdbc:mysql://mysql-nstdb-cluster.nstdb.svc.cluster.local:6446/LCM?autoReconnect=true&permitMysqlScheme=true";
        String username = "LCM";
        String password = "lcm%lcm2";
        try {
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            return connection;
        }
    }

    private boolean executeStage(String stageName, String query, FlowFile flowFile,
            ProcessSession session, Connection connection) {

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(1000);
            rs = stmt.executeQuery(query);

            List<Map<String, Object>> dataList = getListOfMapFromResultSet(rs);
            flowFile = session.putAttribute(flowFile, stageName + ".record.count", String.valueOf(dataList.size()));

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
                logger.info("{} stage batch {} - {} records", stageName, (i / batchSize) + 1, batch.size());

                boolean apiSuccess = callAPI(payload);
                if (!apiSuccess) {
                    flowFile = session.putAttribute(flowFile, stageName + ".status", "API_FAILED");
                    session.transfer(flowFile, REL_FAILURE);
                    return false;
                }
            }

            flowFile = session.putAttribute(flowFile, stageName + ".status", "SUCCESS");
            return true;

        } catch (Exception e) {
            logger.error("{} stage exception: {}", stageName, e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, stageName + ".status", "EXCEPTION");
            flowFile = session.putAttribute(flowFile, stageName + ".error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return false;

        } finally {
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException ignore) {
            }
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException ignore) {
            }
        }
    }

    private boolean callAPI(String payload) {
        try {
            String apiUrl = "http://topology-service/topology/graphdb/createMultipleVertices";
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            logger.info("API Response Code: {}, Body: {}", response.statusCode(), response.body());
            return response.statusCode() == 200;

        } catch (Exception e) {
            logger.error("API call exception: {}", e.getMessage(), e);
            return false;
        }
    }

    private List<Map<String, Object>> getListOfMapFromResultSet(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols = metaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++)
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            list.add(map);
        }
        return list;
    }

    // SQL queries of DWDM AND ROUTER
    private String getParentQueryForLLDP() {
        return "SELECT 'Router' AS relation, ne.ID AS id, ne.NE_NAME AS neName, ne.LATITUDE AS latitude, ne.LONGITUDE AS longitude, ne.NE_SOURCE AS neSource, ne.MODIFICATION_TIME AS ModificationTime, ne.NE_TYPE AS neType, ne.NE_CATEGORY AS neCategory, ne.VENDOR AS vendor, ne.CKT_ID AS cktId, ne.FREQUENCY AS neFrequency, ne.IS_DELETED AS isDeleted, ne.STATUS AS neStatus, ne.TECHNOLOGY AS technology, ne.NE_CATEGORY AS srcCategory, ne.NE_CATEGORY AS destCategory, ne.DOMAIN AS domain, ne.SOFTWARE_VERSION AS softwareVersion, ne.NE_ID AS neId, ne.PM_EMS_ID AS pmEmsId, ne.FM_EMS_ID AS fmEmsId, ne.CM_EMS_ID AS cmEmsId, ne.IPV4 AS ipv4, ne.IPV6 AS ipv6, ne.MODEL AS model, ne.MAC_ADDRESS AS macAddress, ne.FRIENDLY_NAME AS friendlyName, ne.DUPLEX AS duplex, ne.RADIUS AS radius, ne.CATEGORY AS category, ne.HOST_NAME AS hostname, ne.FQDN AS fqdn, ne.UUID AS uuid, ne.SERIAL_NUMBER AS serialNumber, ne.INSERVICE_DATE AS inServiceDate, l1.id AS l1Id, l1.GEO_CODE AS l1GeoCode, l1.LONGITUDE AS l1Long, l1.LATITUDE AS l1Lat, l2.ID AS l2Id, l2.GEO_CODE AS l2GeoCode, l2.LONGITUDE AS l2Long, l2.LATITUDE AS l2Lat, l3.ID AS l3Id, l3.GEO_CODE AS l3GeoCode, l3.LONGITUDE AS l3Long, l3.LATITUDE AS l3Lat, l4.ID AS l4Id, l4.GEO_CODE AS l4GeoCode, l4.LONGITUDE AS l4Long, l4.LATITUDE AS l4Lat, neighbourNE.NE_ID AS neighbourNEId, neighbourNE.NE_NAME AS neighbourNEName, neighbourNE.LATITUDE AS neighbourNELat, neighbourNE.LONGITUDE AS neighbourNELong, parentNE.NE_NAME AS parentNE, neighbourParentNE.NE_NAME AS neighbourParentNEName, neighbourParentNE.LONGITUDE AS neighbourParentNELong, neighbourParentNE.LATITUDE AS neighbourParentNELat FROM NETWORK_ELEMENT ne LEFT JOIN PRIMARY_GEO_L1 l1 ON ne.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN PRIMARY_GEO_L2 l2 ON ne.GEOGRAPHY_L2_ID_FK = l2.id LEFT JOIN PRIMARY_GEO_L3 l3 ON ne.GEOGRAPHY_L3_ID_FK = l3.id LEFT JOIN PRIMARY_GEO_L4 l4 ON ne.GEOGRAPHY_L4_ID_FK = l4.id LEFT JOIN NETWORK_ELEMENT parentNE ON ne.PARENT_NE_ID_FK = parentNE.ID LEFT JOIN NETWORK_ELEMENT neighbourNE ON ne.NETWORK_ELEMENT_ID_FK = neighbourNE.ID LEFT JOIN NETWORK_ELEMENT neighbourParentNE ON neighbourNE.PARENT_NE_ID_FK = neighbourParentNE.ID WHERE ne.IS_DELETED = 0 AND ne.NE_TYPE IN ('GNE','ILA','MDWDM','NIC','NODE','OADM','OTN','ROUTER') AND ne.NE_CATEGORY IN ('GATEWAY_INTERFACE','OPTICAL_AMPLIFIERS','DWDM','OTN','NODE','ROUTER') AND DATE(ne.MODIFICATION_TIME) >= '"
                + globalVertexMaxTime + "'";
    }

    private String getNeighbourQueryForLLDP() {
        return "SELECT DISTINCT 'interface' AS relation, src.ID AS id, src.NE_NAME AS neName, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.NE_SOURCE AS neSource, l.MODIFICATION_TIME AS ModificationTime, src.NE_TYPE AS neType, src.NE_CATEGORY AS neCategory, src.VENDOR AS vendor, src.CKT_ID AS cktId, src.FREQUENCY AS neFrequency, src.IS_DELETED AS isDeleted, src.STATUS AS neStatus, src.TECHNOLOGY AS technology, src.NE_CATEGORY AS srcCategory, src.DOMAIN AS domain, src.SOFTWARE_VERSION AS softwareVersion, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.DUPLEX AS duplex, src.RADIUS AS radius, src.CATEGORY AS category, l.LLDP_LINK_NAME AS hostname, src.FQDN AS fqdn, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, src.INSERVICE_DATE AS inServiceDate, l1.id AS l1Id, l1.GEO_CODE AS l1GeoCode, l1.LONGITUDE AS l1Long, l1.LATITUDE AS l1Lat, l2.ID AS l2Id, l2.GEO_CODE AS l2GeoCode, l2.LONGITUDE AS l2Long, l2.LATITUDE AS l2Lat, l3.ID AS l3Id, l3.GEO_CODE AS l3GeoCode, l3.LONGITUDE AS l3Long, l3.LATITUDE AS l3Lat, l4.ID AS l4Id, l4.GEO_CODE AS l4GeoCode, l4.LONGITUDE AS l4Long, l4.LATITUDE AS l4Lat, parentSrc.NE_NAME AS parentNE FROM LLDP_LINK l JOIN NETWORK_ELEMENT src ON l.SOURCE_INTERFACE_NE_ID = src.ID or l.DESTINATION_INTERFACE_NE_ID = src.ID LEFT JOIN PRIMARY_GEO_L1 l1 ON src.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN PRIMARY_GEO_L2 l2 ON src.GEOGRAPHY_L2_ID_FK = l2.id LEFT JOIN PRIMARY_GEO_L3 l3 ON src.GEOGRAPHY_L3_ID_FK = l3.id LEFT JOIN PRIMARY_GEO_L4 l4 ON src.GEOGRAPHY_L4_ID_FK = l4.id LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID WHERE parentSrc.IS_DELETED = 0 AND (parentSrc.NE_TYPE IN ('ROUTER') OR parentSrc.NE_CATEGORY IN ('ROUTER')) AND DATE(l.MODIFICATION_TIME) >= '"
                + globalVertexMaxTime + "'";
    }

    private String getLinkQueryForLLDP() {
        return "SELECT 'lldp' AS relation, src.ID AS id, src.NE_NAME AS neName, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.NE_SOURCE AS neSource, src.NE_TYPE AS neType, src.NE_CATEGORY AS neCategory, src.NE_CATEGORY AS srcCategory, src.NE_CATEGORY AS destCategory,src.VENDOR AS vendor, link.MODIFICATION_TIME AS ModificationTime, src.CKT_ID AS cktId, src.FREQUENCY AS neFrequency, src.IS_DELETED AS isDeleted, src.STATUS AS neStatus, src.TECHNOLOGY AS technology, src.DOMAIN AS domain, src.SOFTWARE_VERSION AS softwareVersion, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.DUPLEX AS duplex, src.RADIUS AS radius, src.CATEGORY AS category, link.LLDP_LINK_NAME AS hostname, src.FQDN AS fqdn, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, src.INSERVICE_DATE AS inServiceDate, l1.ID AS l1Id, l1.GEO_CODE AS l1GeoCode, l1.LONGITUDE AS l1Long, l1.LATITUDE AS l1Lat, dest.NE_ID AS neighbourNEId, dest.NE_NAME AS neighbourNEName, dest.LATITUDE AS neighbourNELat, dest.LONGITUDE AS neighbourNELong, parentSrc.NE_NAME AS parentNE, parentDest.NE_NAME AS neighbourParentNEName, parentDest.LONGITUDE AS neighbourParentNELong, parentDest.LATITUDE AS neighbourParentNELat FROM LLDP_LINK link JOIN NETWORK_ELEMENT src ON src.ID = link.SOURCE_INTERFACE_NE_ID JOIN NETWORK_ELEMENT dest ON dest.ID = link.DESTINATION_INTERFACE_NE_ID LEFT JOIN PRIMARY_GEO_L1 l1 ON src.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID LEFT JOIN NETWORK_ELEMENT parentDest ON dest.PARENT_NE_ID_FK = parentDest.ID WHERE link.IS_DELETED = 0 AND src.IS_DELETED = 0 AND dest.IS_DELETED = 0 AND DATE(link.MODIFICATION_TIME) >= '" + globalVertexMaxTime + "'";
    }
}