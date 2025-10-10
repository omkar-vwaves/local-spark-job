package com.enttribe.custom.processor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportEmailNotificationService26 implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ReportEmailNotificationService26.class);

    // Constants
    public static final String API_ENDPOINT_EMAIL_LOCAL = "http://localhost:6677/base/util/rest/NotificationMail/sendEmail?viaQueue=true";
    public static final String PM_REPORT_DOWNLOAD_END_POINT = "http://pm-service.ansible.svc.cluster.local/pmy/rest/Report/downloadReport?";

    public static final String API_ENDPOINT_EMAIL_PROD = "http://base-utility-service.ansible.svc.cluster.local/base/util/rest/NotificationMail/sendEmail?viaQueue=true";;
    public static final boolean IS_PRODUCTION = true;

    public static final String API_ENDPOINT_EMAIL = IS_PRODUCTION ? API_ENDPOINT_EMAIL_PROD : API_ENDPOINT_EMAIL_LOCAL;

    public static final String USER_NAME = "akshay.admin";
    public static final String USER_PASSWORD = "Vision@123";
    public static final String CLIENT_ID = "bluewaves";
    public static final String API_ENDPOINT_FOR_TOKEN_LOCAL = "https://dev.visionwaves.com/auth/realms/BNTV/protocol/openid-connect/token";
    public static final String API_ENDPOINT_FOR_TOKEN_PROD = "http://keycloak-http.ansible.svc.cluster.local/auth/realms/BNTV/protocol/openid-connect/token";
    public static final String API_ENDPOINT_FOR_TOKEN = IS_PRODUCTION ? API_ENDPOINT_FOR_TOKEN_PROD
            : API_ENDPOINT_FOR_TOKEN_LOCAL;
    public static final String SCOPE = "openid";

    // Messages
    public static final String SUCCESS_MESSAGE = "Report email notification sent successfully";
    public static final String ERROR_MESSAGE = "Failed to send report notification";

    // Email Configuration
    public static final String FROM_EMAIL = "no-reply@visionwaves.com";
    public static final String EMAIL_SUBJECT = "Performance Report Generated - Please Find Attachment";
    public static final String COMPONENT_NAME = "REPORT_MANAGEMENT";

    // Report Notification Settings
    public static final String TEMPLATE_REPORT = "PM_REPORT_EMAIL";
    public static final String PRIORITY_NORMAL = "NORMAL";
    public static final String CATEGORY_REPORT = "report";

    // HTTP Headers
    public static final String HEADER_ACCESS_TOKEN = "access-token";
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HEADER_ACCEPT = "Accept";
    public static final String HEADER_ACCEPT_LANGUAGE = "Accept-Language";
    public static final String HEADER_AUDIENCE = "audience";
    public static final String HEADER_NGSW_BYPASS = "ngsw-bypass";
    public static final String HEADER_X_MODULE = "x-module";
    public static final String HEADER_X_SUBMODULE = "x-submodule";

    // Timeouts
    public static final int CONNECT_TIMEOUT = 30000;
    public static final int READ_TIMEOUT = 60000;

    private String token;

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flow File Successfully Processed!")
            .build();

    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flow File Failed to Process!")
            .build();

    @Override
    public void run() {

    }

    public void runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            java.sql.Connection connection, String cacheValue) {

        String subscriber = flowFile.getAttribute("subscriber");
        subscriber = subscriber != null ? subscriber : "";

        try {
            String reportName = flowFile.getAttribute("report_name");
            String generatedReportId = flowFile.getAttribute("generated_report_id");

            String extension = flowFile.getAttribute("extension");

            if (extension.equalsIgnoreCase("excel")) {
                extension = "xlsx";
            } else {
                extension = "csv";
            }

            logger.info(
                    "Receiving Attributes From Flow File: Report Name={}, Generated Report Id={}, Subscriber={}, Extension={}",
                    reportName, generatedReportId, subscriber, extension);

            this.token = fetchAccessToken(USER_NAME, USER_PASSWORD, CLIENT_ID, API_ENDPOINT_FOR_TOKEN);

            Map<String, String> resolveVariable = createReportVariables(reportName, generatedReportId);
            sendReportEmailNotification(TEMPLATE_REPORT, subscriber, resolveVariable, token, generatedReportId,
                    reportName, extension);

            logger.info("Report Notification Sent Successfully to: {}", subscriber);
            Map<String, String> attributes = new HashMap<>();
            attributes.put("STATUS", "SUCCESS");
            attributes.put("MESSAGE", "Report Notification Sent Successfully to: " + subscriber);
            session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, RELATIONSHIP_SUCCESS);
        } catch (Exception e) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("STATUS", "FAILURE");
            attributes.put("MESSAGE", "Failed to Send Report Notification to: " + subscriber);
            session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
            logger.error("Error Sending Report Notification from FlowFile", e);
            throw new RuntimeException("Failed to Send Report Notification to: " + subscriber, e);
        }
    }

    public static void main(String[] args) {
        String token = fetchAccessToken(USER_NAME, USER_PASSWORD, CLIENT_ID, API_ENDPOINT_FOR_TOKEN);
        Map<String, String> resolveVariable = createReportVariables("Test Report", "15833");
        sendReportEmailNotification("REPORT_EMAIL", "abhishek.lodha@visionwaves.com, omkar.khilari@visionwaves.com",
                resolveVariable, token, "15700", "Test Report", "csv");
        System.out.println("Report email notification sent successfully");
    }

    public static void sendReportEmailNotification(String templateName, String toEmailIds,
            Map<String, String> resolveVariable, String token, String id, String reportName, String extension) {

        logger.info(
                "Sending Report Email Notification: Template Name={}, To Email Ids={}, Resolve Variable={}, Token={}, Id={}, Report Name={}, Extension={}",
                templateName, toEmailIds, resolveVariable, token, id, reportName, extension);
        try {
            NotificationMailWrapper notificationWrapper = new NotificationMailWrapper();

            java.util.Set<String> emailSet = new java.util.HashSet<>();
            String[] emails = toEmailIds.split(",");
            for (String email : emails) {
                emailSet.add(email.trim());
            }

            notificationWrapper.setTemplateName(templateName);
            notificationWrapper.setToEmailIds(emailSet);
            notificationWrapper.setFromEmail(FROM_EMAIL);

            notificationWrapper.setSubject(EMAIL_SUBJECT);

            notificationWrapper.setEmailContent("report");
            notificationWrapper.setIsVariableResolve(false);
            notificationWrapper.setResolveVariable(resolveVariable);
            notificationWrapper.setBySmtp(false);
            notificationWrapper.setComponentName(COMPONENT_NAME);

            String attachmentUrl = PM_REPORT_DOWNLOAD_END_POINT + "id=" + id;
            notificationWrapper.setIcon(attachmentUrl);
            NotificationAttachment attachment = new NotificationAttachment();
            attachment.setName(reportName + "." + extension);
            attachment.setPath(attachmentUrl);
            attachment.setBucketName("performance");
            attachment.setIsPublic(true);

            List<NotificationAttachment> attachmentList = new ArrayList<>();
            attachmentList.add(attachment);
            notificationWrapper.setAttachment(attachmentList);
            ObjectMapper mapper = new ObjectMapper();
            String jsonPayload = mapper.writeValueAsString(notificationWrapper);
            logger.info("Report Email Notification JSON Payload: " + jsonPayload);

            logger.info("Sending Report Email Notification to API: {}", API_ENDPOINT_EMAIL);

            URL url = new URL(API_ENDPOINT_EMAIL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty(HEADER_CONTENT_TYPE, "application/json");
            connection.setRequestProperty(HEADER_ACCESS_TOKEN, "Bearer " + token);
            connection.setRequestProperty(HEADER_NGSW_BYPASS, "bypass-service-worker");
            connection.setRequestProperty(HEADER_ACCEPT_LANGUAGE,
                    "en-GB,en-US;q=0.9,en;q=0.8");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            StringBuilder response = new StringBuilder();

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream(),
                            StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
            }

            logger.info("Report Email Notification Response Code: " + responseCode);
            logger.info("Report Email Notification Response Body: " + response.toString());

            if (responseCode >= 200 && responseCode < 300) {
                logger.info("Report Email Notification Sent Successfully with Attachment: ");
            } else {
                throw new RuntimeException("Report Email Notification Failed");
            }

        } catch (Exception e) {
            logger.error("Error Calling Report Email Notification API", e);
            throw new RuntimeException("Failed to Send Report Email Notification", e);
        }
    }

    public static String fetchAccessToken(String username, String password, String clientId, String tokenUrl) {

        logger.info("Fetching Access Token From: {}, Username={}, Client Id={}, Token Url={}", username, clientId,
                tokenUrl);
        try {
            String formData = String.format("client_id=%s&grant_type=password&scope=%s&username=%s&password=%s",
                    URLEncoder.encode(clientId, StandardCharsets.UTF_8),
                    URLEncoder.encode(SCOPE, StandardCharsets.UTF_8),
                    URLEncoder.encode(username, StandardCharsets.UTF_8),
                    URLEncoder.encode(password, StandardCharsets.UTF_8));

            URL url = new URL(tokenUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty(HEADER_CONTENT_TYPE,
                    "application/x-www-form-urlencoded");
            connection.setRequestProperty("Cookie", "KEYCLOAK_LOCALE=en; Secure");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = formData.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            StringBuilder response = new StringBuilder();

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream(),
                            StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
            }

            if (responseCode == 200) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(response.toString());

                String accessToken = root.path("access_token").asText();
                logger.info(
                        "Access Token Fetched Successfully From: {}, Username={}, Client Id={}, Token Url={}, Access Token={}",
                        username, clientId, tokenUrl, accessToken);
                return accessToken;
            } else {
                logger.error(
                        "Failed to Fetch Access Token From: {}, Username={}, Client Id={}, Token Url={}, Response Code={}",
                        username, clientId, tokenUrl, responseCode);
            }
        } catch (Exception e) {
            logger.error("Error Fetching Access Token From: {}, Username={}, Client Id={}, Token Url={}", username,
                    clientId, tokenUrl);
        }

        return null;
    }

    private static Map<String, String> createReportVariables(String reportName, String generatedReportId) {
        logger.info("Creating Report Variables: Report Name={}, Generated Report Id={}", reportName, generatedReportId);

        reportName = reportName != null ? reportName : "";
        generatedReportId = generatedReportId != null ? generatedReportId : "";
        String generatedDate = java.time.LocalDateTime.now().toString();
        String fileName = reportName + ".xlsx";

        Map<String, String> resolveVariable = new HashMap<>();
        resolveVariable.put("reportName", reportName);
        resolveVariable.put("generatedReportId", generatedReportId);
        resolveVariable.put("generatedDate", generatedDate);
        resolveVariable.put("fileName", fileName);

        logger.info(
                "Report Variables Created Successfully: Report Name={}, Generated Report Id={}, Resolve Variable={}",
                reportName, generatedReportId, resolveVariable);
        return resolveVariable;
    }

    public static class NotificationMailWrapper {
        private String emailContent;
        private String subject;
        private String fromEmail;
        private String templateName;
        private Set<String> toEmailIds;
        private Set<String> ccEmailIds;
        private Set<String> bccEmailIds;
        private String componentName;
        private String icon;
        private Boolean isVariableResolve = true;
        private Map<String, String> resolveVariable;
        private Boolean bySmtp = true;
        private Set<String> delegationEmails;
        private List<NotificationAttachment> attachment;

        public NotificationMailWrapper() {
        }

        public String getEmailContent() {
            return this.emailContent;
        }

        public String getSubject() {
            return this.subject;
        }

        public String getFromEmail() {
            return this.fromEmail;
        }

        public String getTemplateName() {
            return this.templateName;
        }

        public Set<String> getToEmailIds() {
            return this.toEmailIds;
        }

        public Set<String> getCcEmailIds() {
            return this.ccEmailIds;
        }

        public Set<String> getBccEmailIds() {
            return this.bccEmailIds;
        }

        public String getComponentName() {
            return this.componentName;
        }

        public String getIcon() {
            return this.icon;
        }

        public Boolean getIsVariableResolve() {
            return this.isVariableResolve;
        }

        public Map<String, String> getResolveVariable() {
            return this.resolveVariable;
        }

        public Boolean getBySmtp() {
            return this.bySmtp;
        }

        public Set<String> getDelegationEmails() {
            return this.delegationEmails;
        }

        public List<NotificationAttachment> getAttachment() {
            return this.attachment;
        }

        public void setEmailContent(final String emailContent) {
            this.emailContent = emailContent;
        }

        public void setSubject(final String subject) {
            this.subject = subject;
        }

        public void setFromEmail(final String fromEmail) {
            this.fromEmail = fromEmail;
        }

        public void setTemplateName(final String templateName) {
            this.templateName = templateName;
        }

        public void setToEmailIds(final Set<String> toEmailIds) {
            this.toEmailIds = toEmailIds;
        }

        public void setCcEmailIds(final Set<String> ccEmailIds) {
            this.ccEmailIds = ccEmailIds;
        }

        public void setBccEmailIds(final Set<String> bccEmailIds) {
            this.bccEmailIds = bccEmailIds;
        }

        public void setComponentName(final String componentName) {
            this.componentName = componentName;
        }

        public void setIcon(final String icon) {
            this.icon = icon;
        }

        public void setIsVariableResolve(final Boolean isVariableResolve) {
            this.isVariableResolve = isVariableResolve;
        }

        public void setResolveVariable(final Map<String, String> resolveVariable) {
            this.resolveVariable = resolveVariable;
        }

        public void setBySmtp(final Boolean bySmtp) {
            this.bySmtp = bySmtp;
        }

        public void setDelegationEmails(final Set<String> delegationEmails) {
            this.delegationEmails = delegationEmails;
        }

        public void setAttachment(final List<NotificationAttachment> attachment) {
            this.attachment = attachment;
        }
    }

    public static class NotificationAttachment {
        private Integer id;
        private String name;
        private String path;
        private String bucketName;
        private Boolean isPublic;

        public NotificationAttachment() {
        }

        public NotificationAttachment(String name, String path, String bucketName, Boolean isPublic) {
            this.name = name;
            this.path = path;
            this.bucketName = bucketName;
            this.isPublic = isPublic;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getBucketName() {
            return bucketName;
        }

        public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }

        public Boolean getIsPublic() {
            return isPublic;
        }

        public void setIsPublic(Boolean isPublic) {
            this.isPublic = isPublic;
        }
    }

}
