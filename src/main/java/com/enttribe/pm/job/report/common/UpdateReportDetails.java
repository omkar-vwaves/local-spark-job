package com.enttribe.pm.job.report.common;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateReportDetails extends Processor {

    private static final Logger logger = LoggerFactory.getLogger(UpdateReportDetails.class);
    private static String SPARK_PM_JDBC_DRIVER = "SPARK_PM_JDBC_DRIVER";
    private static String SPARK_PM_JDBC_URL = "SPARK_PM_JDBC_URL";
    private static String SPARK_PM_JDBC_USERNAME = "SPARK_PM_JDBC_USERNAME";
    private static String SPARK_PM_JDBC_PASSWORD = "SPARK_PM_JDBC_PASSWORD";

    public UpdateReportDetails() {
        super();
        logger.debug("UpdateReportDetails No Argument Constructor Called!");
    }

    public UpdateReportDetails(Dataset<Row> dataframe, Integer id, String processorName) {
        super(id, processorName);
        this.dataFrame = dataframe;
        logger.debug("UpdateReportDetails Constructor Called with Input DataFrame With ID: {} and Processor Name: {}",
                id,
                processorName);
    }

    public UpdateReportDetails(Integer id, String processorName) {
        super(id, processorName);
        logger.debug("UpdateReportDetails Constructor Called with ID: {} and Processor Name: {}", id, processorName);
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

        logger.error("[UpdateReportDetails] Execution Started!");

        long startTime = System.currentTimeMillis();

        Map<String, String> jobContextMap = jobContext.getParameters();

        String reportWidgetIdPk = jobContextMap.get("REPORT_WIDGET_ID_PK");
        String generatedReportId = jobContextMap.get("GENERATED_REPORT_ID");
        String reportWidgetFilePath = jobContextMap.get("REPORT_WIDGET_FILE_PATH");
        String generatedReportFilePath = jobContextMap.get("GENERATED_REPORT_FILE_PATH");
        String fileSize = jobContextMap.get("FILE_SIZE");

        SPARK_PM_JDBC_DRIVER = jobContextMap.get(SPARK_PM_JDBC_DRIVER);
        SPARK_PM_JDBC_URL = jobContextMap.get(SPARK_PM_JDBC_URL);
        SPARK_PM_JDBC_USERNAME = jobContextMap.get(SPARK_PM_JDBC_USERNAME);
        SPARK_PM_JDBC_PASSWORD = jobContextMap.get(SPARK_PM_JDBC_PASSWORD);

        logger.error("JDBC Credentials: Driver={}, URL={}, Username={}, Password={}", SPARK_PM_JDBC_DRIVER,
                SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);
                
        logger.error("Update Parameters: REPORT_WIDGET_ID_PK={}, GENERATED_REPORT_ID={}, REPORT_WIDGET_FILE_PATH={}, GENERATED_REPORT_FILE_PATH={}, FILE_SIZE={}",
                reportWidgetIdPk, generatedReportId, reportWidgetFilePath, generatedReportFilePath, fileSize);

        updateReportDetails(reportWidgetFilePath, Long.parseLong(fileSize), generatedReportFilePath, reportWidgetIdPk, generatedReportId);

        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        long minutes = durationMillis / 60000;
        long seconds = (durationMillis % 60000) / 1000;

        logger.error("[UpdateReportDetails] Execution Completed! Time Taken: {} Minutes | {} Seconds", minutes, seconds);

        return this.dataFrame;
    }

    private static void updateReportDetails(String reportWidgetFilePath, long fileSizeInBytes,
            String generateReportFilePath, String reportWidgetIdPk, String generatedReportId) {

        logger.error("========== Updating Report Details ==========");
        logger.error("Report File Name = {} | Generate Report File Path = {} | File Size = {} Bytes | {} KB",
        reportWidgetFilePath, generateReportFilePath, fileSizeInBytes,
                String.format("%.2f", fileSizeInBytes / 1024.0));


        String updateReportWidgetQuery = "UPDATE REPORT_WIDGET SET FILE_PATH = ?, STATUS = 'CREATED' WHERE REPORT_WIDGET_ID_PK = ?";

        try (Connection connection = getDatabaseConnection();
                PreparedStatement statement = connection.prepareStatement(updateReportWidgetQuery)) {
            statement.setString(1, reportWidgetFilePath);
            statement.setInt(2, Integer.parseInt(reportWidgetIdPk));
            statement.executeUpdate();

            logger.error("Report Widget Updated Successfully With Status = CREATED & File Path = {}",
                    reportWidgetFilePath);

        } catch (SQLException e) {
            logger.error("Error in Updating Report Details, Message: {}, Error: {}", e.getMessage(), e);
        }

        String updateGenerateReportQuery = "UPDATE GENERATED_REPORT SET PROGRESS_STATE = 'GENERATED', FILE_SIZE = ?, FILE_PATH = ?, GENERATED_DATE = NOW() WHERE GENERATED_REPORT_ID_PK = ?";

        try (Connection connection = getDatabaseConnection();
                PreparedStatement statement = connection.prepareStatement(updateGenerateReportQuery)) {
            statement.setString(1, String.valueOf(fileSizeInBytes));
            statement.setString(2, generateReportFilePath);
            statement.setInt(3, Integer.parseInt(generatedReportId));
            statement.executeUpdate();

            logger.error(
                    "Generated Report Updated Successfully With Status = GENERATED & File Size = {} Bytes & File Path = {}",
                    fileSizeInBytes, generateReportFilePath);
        } catch (SQLException e) {
            logger.error("Error in Updating Generated Report Details, Message: {}, Error: {}", e.getMessage(),
                    e);
        }

        logger.error("========== Report Details Updated Successfully ==========");
    }

    private static Connection getDatabaseConnection() {
        Connection connection = null;
        try {

            Class.forName(SPARK_PM_JDBC_DRIVER);
            return DriverManager.getConnection(SPARK_PM_JDBC_URL, SPARK_PM_JDBC_USERNAME, SPARK_PM_JDBC_PASSWORD);

        } catch (ClassNotFoundException e) {
            logger.error("MySQL JDBC Driver Not Found @getDatabaseConnection | Message: {}, Error: {}",
                    e.getMessage(), e);
        } catch (SQLException e) {
            logger.error("Database Connection Error @getDatabaseConnection | Message: {}, Error: {}", e.getMessage(),
                    e);
        } catch (Exception e) {
            logger.error("Unexpected Exception @getDatabaseConnection | Message: {}, Error: {}", e.getMessage(), e);
        }
        return connection;
    }

}
