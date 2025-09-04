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
import com.enttribe.sparkrunner.processors.transformation.CacheDataset;
import com.enttribe.sparkrunner.util.SparkRunnerUtils;
import com.enttribe.sparkrunner.util.Utils;

public class CacheDatasetCustom extends CacheDataset {

    private static final Logger logger = LogManager.getLogger(CacheDatasetCustom.class);

    public CacheDatasetCustom() {
        super();
    }

    public CacheDatasetCustom(Dataset<Row> dataFrame, int processorId, String processorName, String storageLevel, String tempTable) {
        super(processorId, processorName, storageLevel, tempTable);
        this.dataFrame = dataFrame;
    }

    @Override
    public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
        execute(jobContext);
        return this.dataFrame;
    }



    public void execute(JobContext jobContext) throws Exception {
        try {
            logger.debug("Getting declared fields from parent class...");
            Field[] fields = super.getClass().getSuperclass().getDeclaredFields();
            logger.debug("Fields: {}", Arrays.toString(fields));

            this.getFieldAndValueInMap(fields);
            applicationId = jobContext.applicationId();
            replaceAllDynamicPropertyFromJobcontext(jobContext, fields);
            SparkRunnerUtils.setJobDescription(jobContext, this.processorName);

            this.dataFrame = super.executeAndGetResultDataframe(jobContext);
            logger.debug("[{}] inside execute", this.processorName);

            this.reAssignInitialValueOfSubClassFields(fields);
            this.passDatasetToNextProcessorAndExecute(jobContext, this.dataFrame);
        } catch (Exception e) {
            logger.error("ERROR while passing dataframe to next processor STACKTRACE");
            ProcessorAudit.exceptionLog("MAIN PROCESSOR", this.processorName, applicationId, "ERROR_DETAIL", e.getMessage());
            boolean ignore = Boolean.parseBoolean(this.ignoreNullDataset);
            logger.error("[{}] ignoreNullDataset : [{}], error: {}", this.processorName, ignore, ExceptionUtils.getMessage(e));
            if (Utils.hasValidValue(this.ignoreNullDataset) && !ignore) {
                throw e;
            }
        }
    }

    private void replaceAllDynamicPropertyFromJobcontext(JobContext jobContext, Field[] fields) throws IllegalAccessException {
        for (Field field : fields) {
            Dynamic dynamic = field.getAnnotation(Dynamic.class);
            if (dynamic != null) {
                field.setAccessible(true);
                String value = (String) field.get(this);
                value = Utils.replaceDollarKeyFromContext(value, jobContext, this.processorName);
                field.set(this, value);
            }
        }
    }
}
