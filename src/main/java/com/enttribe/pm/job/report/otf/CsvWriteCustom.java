package com.enttribe.pm.job.report.otf;

import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;

import com.enttribe.sparkrunner.processor.audit.ProcessorAudit;
import com.enttribe.sparkrunner.util.AWSS3CustomClient;
import com.enttribe.sparkrunner.util.AWSS3Util;
import com.enttribe.sparkrunner.util.HDFSUtil;
import com.enttribe.sparkrunner.util.Utils;
import com.enttribe.sparkrunner.utils.exceptions.EmptyDataSetException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvWriteCustom extends Processor {
   private static Map<String, String> contextMap = new HashMap<>();

   private static Logger logger = LoggerFactory.getLogger(CsvWriteCustom.class);

   @Dynamic
   public String directoryPath;

   @Dynamic
   public String inferSchema;

   @Dynamic
   public String header;

   @Dynamic
   public String saveMode;

   @Dynamic
   public String bucket;

   @Dynamic
   public String delimiter;

   @Dynamic
   public String fileType;

   public CsvWriteCustom() {

      super();
      logger.info("CsvWriteCustom No-Argument ConstructorInitialized!");
   }

   public CsvWriteCustom(int processorId, String processorName, String directoryPath, String inferSchema,
         String header) {
      super(processorId, processorName);
      this.inferSchema = inferSchema;
      this.header = header;
      this.directoryPath = directoryPath;

      logger.info(
            "Initialized CsvWriteCustom with processorId={}, processorName={}, directoryPath={}, inferSchema={}, header={}",
            processorId, processorName, directoryPath, inferSchema, header);
   }

   public CsvWriteCustom(int processorId, String processorName, String directoryPath, String bucket, String inferSchema,
         String header, String saveMode, String delimiter, String fileType) {
      super(processorId, processorName);
      this.inferSchema = inferSchema;
      this.header = header;
      this.directoryPath = directoryPath;
      this.saveMode = saveMode;
      this.bucket = bucket;
      this.delimiter = delimiter;
      this.fileType = fileType;

      logger.info(
            "Initialized CsvWriteCustom with processorId={}, processorName={}, directoryPath={}, bucket={}, inferSchema={}, header={}, saveMode={}, delimiter={}, fileType={}",
            processorId, processorName, directoryPath, bucket, inferSchema, header, saveMode, delimiter, fileType);
   }

   public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
      contextMap = jobContext.getParameters();

      logger.info("Processor [{}]: Executing method 'executeAndGetResultDataframe' in WriteCSVFile class",
            this.processorName);

      if (this.dataFrame == null) {
         String exception = "Exception in CSVWrite processor named " + this.processorName
               + " Dataset from previous processor is NULL ";
         ProcessorAudit.exceptionLog("CSVWrite", this.processorName, applicationId, "Dataset Null Exception",
               exception);
         throw new EmptyDataSetException(exception);
      } else {
         ProcessorAudit.fileExistLogging("CSVWrite", this.directoryPath, this.processorName, applicationId, ".csv");
         Map<String, String> options = new HashMap<>();
         options.put("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ");
         if (this.inferSchema != null && this.header != null) {

            logger.info("[{}] Setting CSV options: inferSchema={}, header={}", this.processorName, this.inferSchema,
                  this.header);

            options.put("inferSchema", this.inferSchema);
            options.put("header", this.header);
            if (Utils.hasValidValue(this.delimiter)) {
               logger.info("[{}] Setting delimiter to '{}'", this.processorName, this.delimiter);
               options.put("delimiter", this.delimiter);
            }

            if (Utils.hasValidValue(this.fileType)) {
               logger.info("[{}] Setting compression codec to '{}'", this.processorName, this.fileType);
               options.put("codec", this.fileType);
            }
         }

         if (Utils.hasValidValue(this.directoryPath) && this.directoryPath.contains(".csv")
               && Utils.hasValidValue(this.saveMode)) {

            logger.info("Recieved Directory Path : {}", this.directoryPath);

            String csvFolder = this.directoryPath.replaceAll(".csv", "").replaceAll(".gz", "");

            logger.info("[{}] Writing DataFrame to CSV folder: '{}', saveMode: '{}'", this.processorName, csvFolder,
                  this.saveMode);

            this.dataFrame.coalesce(1).write().mode(this.saveMode).options(options).csv(csvFolder);

            if (!this.directoryPath.contains("s3a://") && !this.directoryPath.contains("s3://")) {
               this.convertCSVFolderInCSVFile(csvFolder);
            } else {

               logger.info("[{}] Detected S3 path. Converting S3 CSV folder to single file object: '{}'",
                     this.processorName, csvFolder);

               try {
                  this.convertCSVDirObjectInCSVFileObjectInAWS(csvFolder);
               } catch (Exception var5) {
                  logger.error("Processor [{}]: Error during S3 CSV folder conversion - {}", this.processorName,
                        ExceptionUtils.getStackTrace(var5));
               }
            }
         } else if (Utils.hasValidValue(this.directoryPath) && Utils.hasValidValue(this.saveMode)) {
            logger.info("Processor [{}]: Writing DataFrame directly to '{}', saveMode='{}'", this.processorName,
                  this.directoryPath, this.saveMode);
            this.dataFrame.write().mode(this.saveMode).options(options).csv(this.directoryPath);
         } else if (Utils.hasValidValue(this.directoryPath)) {
            logger.info("Processor [{}]: Writing DataFrame directly to '{}' with default save mode", this.processorName,
                  this.directoryPath);
            this.dataFrame.write().options(options).csv(this.directoryPath);
         }

         logger.info("Processor [{}]: CSV write process completed. Logging output file.", this.processorName);
         ProcessorAudit.fileLogging("CSVWrite", this.directoryPath, this.processorName, applicationId, ".csv");

         logger.info("Processor [{}]: Displaying sample data from DataFrame output.", this.processorName);
         this.dataFrame.show(5, false); // Show first 5 rows, do not truncate columns

         long rowCount = this.dataFrame.count();
         logger.info("Processor [{}]: Total number of records written: {}", this.processorName, rowCount);

         return this.dataFrame;
      }
   }

   private void convertCSVFolderInCSVFile(String csvFolder) throws Exception, FileNotFoundException, IOException {
      FileSystem fileSystem = HDFSUtil.getFileSystem();

      try {
         FileStatus[] listStatus = fileSystem.listStatus(new Path(csvFolder));

         for (int i = 0; i < listStatus.length; ++i) {
            Path path = listStatus[i].getPath();
            if (path.getName().contains("part")) {
               fileSystem.rename(path, new Path(this.directoryPath));
               fileSystem.delete(new Path(csvFolder), true);
            }
         }

      } catch (Exception var6) {
         fileSystem.close();
         String var10002 = this.processorName;
         throw new Exception("Error in renaming file in CSVwrite processor named " + var10002 + " :: "
               + ExceptionUtils.getStackTrace(var6));
      }
   }

   public static AWSS3CustomClient getS3Client() {
      AWSS3CustomClient client = new AWSS3CustomClient(contextMap.get("SPARK_MINIO_ENDPOINT_URL"),
            contextMap.get("SPARK_MINIO_ACCESS_KEY"), contextMap.get("SPARK_MINIO_SECRET_KEY"));
      return client;
   }

   private void convertCSVDirObjectInCSVFileObjectInAWS(String csvFolder) throws Exception {
      try {
         AWSS3CustomClient minioClient = getS3Client();
         List<String> listStatus = AWSS3Util.listObjects(minioClient, this.bucket, AWSS3Util.getDirPrefix(csvFolder),
               true);
         Iterator var3 = listStatus.iterator();

         while (var3.hasNext()) {
            String path = (String) var3.next();
            if (path.contains("part")) {
               AWSS3Util.renameObject(this.bucket, path, this.directoryPath, contextMap.get("SPARK_MINIO_ENDPOINT_URL"),
                     contextMap.get("SPARK_MINIO_ACCESS_KEY"), contextMap.get("SPARK_MINIO_SECRET_KEY"));
            }
         }

         AWSS3Util.deleteObjectList(this.bucket, listStatus, contextMap.get("SPARK_MINIO_ENDPOINT_URL"),
               contextMap.get("SPARK_MINIO_ACCESS_KEY"), contextMap.get("SPARK_MINIO_SECRET_KEY"));
      } catch (Exception var5) {
         String var10002 = this.processorName;
         throw new Exception("Error in renaming file in CSVwrite processor named " + var10002 + " :: "
               + ExceptionUtils.getStackTrace(var5));
      }
   }

   public List<String> validProcessorConfigurations() {
      List<String> list = new ArrayList<>();
      if (!Utils.hasValidValue(this.directoryPath)) {
         list.add("Exception inside CSVWrite processor named " + this.processorName + " : path is " + this.directoryPath
               + " that must not be " + this.directoryPath);
      }

      return list;
   }

}
