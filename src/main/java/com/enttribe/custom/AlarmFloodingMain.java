// package com.enttribe.custom;

// import org.apache.nifi.flowfile.FlowFile;
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.util.HashMap;
// import java.util.Map;

// public class AlarmFloodingMain {

//     public static void main(String[] args) {
//         try {
//             AlarmFloodingMain main = new AlarmFloodingMain();
//             Connection connection = main.getDatabaseConnection();

//             Map<String, String> attributes = new HashMap<>();

//             attributes.put("domain", "TRANSPORT");
//             attributes.put("vendor", "JUNIPER");
//             attributes.put("technology", "COMMON");

//             attributes.put("GET_ALL_STATS_URL", "http://localhost:9090/api/stats");
//             attributes.put("GET_ALL_STATS_RESPONSE",
//                     "{\"stats\":{\"127.0.0.1\":{\"totalCount\":7,\"windows\":{\"1749655170000\":1}},\"127.0.0.2\":{\"totalCount\":9,\"windows\":{\"1749655170000\":1}}},\"isWindowEnabled\":true}");

//             attributes.put("BLACK_ALL_IP_LIST_URL", "http://localhost:9090/api/blacklist?ips=");
//             attributes.put("BLACK_ALL_IP_LIST_RESPONSE", "{\"ips\": [\"127.0.0.1\", \"127.0.0.2\"]}");

//             FlowFile flowFile = new FlowFile() {

//                 private final Map<String, String> flowFileAttributes = attributes;

//                 private void setAttribute(String key, String value) {
//                     flowFileAttributes.put(key, value);
//                 }

//                 @Override
//                 public Map<String, String> getAttributes() {
//                     return flowFileAttributes;
//                 }

//                 @Override
//                 public String getAttribute(String key) {
//                     return flowFileAttributes.get(key);
//                 }

//                 @Override
//                 public long getId() {
//                     return 1L;
//                 }

//                 @Override
//                 public long getEntryDate() {
//                     return System.currentTimeMillis();
//                 }

//                 @Override
//                 public long getLineageStartDate() {
//                     return System.currentTimeMillis();
//                 }

//                 @Override
//                 public long getLineageStartIndex() {
//                     return 0;
//                 }

//                 @Override
//                 public long getSize() {
//                     return 0;
//                 }

//                 @Override
//                 public Long getLastQueueDate() {
//                     return System.currentTimeMillis();
//                 }

//                 @Override
//                 public long getQueueDateIndex() {
//                     return 0;
//                 }

//                 @Override
//                 public boolean isPenalized() {
//                     return false;
//                 }

//                 @Override
//                 public int compareTo(FlowFile other) {
//                     return 0;
//                 }
//             };

//             AlarmFloodingBlackList1 blackList = new AlarmFloodingBlackList1();
//             FlowFile result = blackList.runSnippet(flowFile, null, null, connection, "cacheValue");
//             System.out.println("------------------------------------------------------------");
//             System.out.println("📋 Processed FlowFile Attributes: " + result.getAttributes());

//         } catch (Exception e) {
//             System.out.println("❌ Exception Occured in main Method, Message: " + e.getMessage() + ", Error: " + e);
//         }
//     }

//     private Connection getDatabaseConnection() throws Exception {
//         String url = "jdbc:mysql://localhost:3306/FMS?autoReconnect=true";
//         String username = "root";
//         String password = "root";
//         return DriverManager.getConnection(url, username, password);
//     }
// }