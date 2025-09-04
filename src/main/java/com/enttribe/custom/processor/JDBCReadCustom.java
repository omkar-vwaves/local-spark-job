package com.enttribe.custom.processor;


import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processor.audit.ProcessorAudit;
import com.enttribe.sparkrunner.processors.dataset.jdbc.JDBCRead;
import com.enttribe.sparkrunner.util.SparkRunnerUtils;
import com.enttribe.sparkrunner.util.Utils;

public class JDBCReadCustom extends JDBCRead {


    private static final Logger logger = LogManager.getLogger(JDBCReadCustom.class);

    public JDBCReadCustom() {
        super();
    }

    public JDBCReadCustom(Dataset<Row> dataFrame, int processorId, String processorName, String dbDriver, String dbConnectionURL, String dbUsername, String dbPassword, String query, String tempTable, String noOfPartition, String partitionColumn, String lowerBound, String upperBound, String fetchsize) {
        super(processorId, processorName, dbDriver, dbConnectionURL, dbUsername, dbPassword, query, tempTable, noOfPartition, partitionColumn, lowerBound, upperBound, fetchsize);
        this.dataFrame = dataFrame;
       
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        execute(jobContext);
        return this.dataFrame;
    }

    public void execute(JobContext jobContext) throws Exception {
        try {
            logger.debug("fields");
            Field[] fields = super.getClass().getSuperclass().getDeclaredFields();
            logger.debug("fields are {}", Arrays.toString(fields));
            for (Field field : fields) {
                field.setAccessible(true);
                String fieldName = field.getName();
                logger.debug("Field name: {}, Field type: {}", fieldName, field.getType());
            }
            this.getFieldAndValueInMap(fields);
            applicationId = jobContext.applicationId();
            replaceAllDynamicPropertyFromJobcontext(jobContext, fields);
            SparkRunnerUtils.setJobDescription(jobContext, this.processorName);
            this.dataFrame = super.executeAndGetResultDataframe(jobContext);
            logger.debug("[{}] inside execute", this.processorName);
            this.reAssignInitialValueOfSubClassFields(fields);
            this.passDatasetToNextProcessorAndExecute(jobContext, this.dataFrame);
        } catch (Exception var8) {
            logger.error("ERROR While passing dataframe to next processor STACKTRACE");
            ProcessorAudit.exceptionLog("MAIN PROCESSOR", this.processorName, applicationId, "ERROR_DETAIL", var8.getMessage());
            boolean ignore = Boolean.parseBoolean(this.ignoreNullDataset);
            logger.error("[{}] ignoreNullDataset :[{}] , error : {}", new Object[]{this.processorName, ignore, ExceptionUtils.getMessage(var8)});
            if (Utils.hasValidValue(this.ignoreNullDataset) && !ignore) {
                throw var8;
            }
        }
    }

    private void replaceAllDynamicPropertyFromJobcontext(JobContext jobContext, Field[] fields) throws IllegalAccessException {
        Field[] var3 = fields;
        int var4 = fields.length;
  
        for(int var5 = 0; var5 < var4; ++var5) {
           Field field = var3[var5];
           Dynamic dynamic = (Dynamic)field.getAnnotation(Dynamic.class);
           if (dynamic != null) {
              String value = (String)field.get(this);
              value = Utils.replaceDollarKeyFromContext(value, jobContext, this.processorName);
              field.set(this, value);
           }
        }
  
     }
    
}
