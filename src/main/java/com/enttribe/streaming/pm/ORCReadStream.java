// Source code is decompiled from a .class file using FernFlower decompiler.
package com.enttribe.streaming.pm;

import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processor.audit.ProcessorAudit;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.HDFSUtil;
import com.enttribe.sparkrunner.util.Utils;
import com.enttribe.sparkrunner.workflowengine.CustomSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ORCReadStream extends Processor {
   private static final long serialVersionUID = 1L;
   @Dynamic
   public String directoryPath;
   @Dynamic
   public String inputSchema;
   @Dynamic
   public String basePath;
   public String tempTable;

   public ORCReadStream() {
   }

   public ORCReadStream(int processorId, String processorName, String inputPath, String tempTable, String basePath) {
      super(processorId, processorName);
      this.directoryPath = inputPath;
      this.tempTable = tempTable;
      this.basePath = basePath;
   }

   public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
      ProcessorAudit.fileExistLogging("ORCRead", this.directoryPath, this.processorName, applicationId, "");
      ProcessorAudit.fileLogging("ORCRead", this.directoryPath, this.processorName, applicationId, "");
      if (Utils.hasValidValue(this.directoryPath) && !this.directoryPath.contains(Symbol.ASTERISK_STRING)) {
      }

      jobContext.sqlctx().sparkSession().conf().set("spark.sql.streaming.schemaInference", "true");
      jobContext.sqlctx().sparkSession().conf().set("spark.sql.streaming.schemaInference.enabled", "true");

      Map<String, String> optionsMap = this.getOptionsMap();
      if (Utils.hasValidValue(this.inputSchema)) {
         this.dataFrame = jobContext.sqlctx().readStream().options(optionsMap).schema(this.getSchema(this.inputSchema)).orc(this.directoryPath);
      } else {
         this.dataFrame = jobContext.sqlctx().readStream().options(optionsMap).orc(this.directoryPath);
      }

      Utils.createOrReplaceTempView(this.tempTable, this.dataFrame, this.processorName);
      return this.dataFrame;
   }

   private Map<String, String> getOptionsMap() {
      Map<String, String> options = new HashMap();
      if (Utils.hasValidValue(this.basePath)) {
         options.put("basePath", this.basePath);
      }

      return options;
   }

   public StructType getSchema(String inputSchema) {
      String[] schema = inputSchema.split(Symbol.COMMA_STRING);
      List<StructField> fields = new ArrayList();
      String[] var4 = schema;
      int var5 = schema.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String col = var4[var6];
         String[] colNamesAndDataTypes = col.split(Symbol.COLON_STRING);
         fields.add(DataTypes.createStructField(colNamesAndDataTypes[0], CustomSchema.getSparkSQLType(CustomSchema.getType(colNamesAndDataTypes[1])), true));
      }

      StructType structType = DataTypes.createStructType(fields);
      return structType;
   }

   public static String getExistingFiles(String filePaths) {
      String existedFilePath = "";
      String[] var2 = filePaths.split(Symbol.COMMA_STRING);
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String hdfsPath = var2[var4];
         if (HDFSUtil.isHdfsFolderExist(hdfsPath)) {
            if (!existedFilePath.contains(hdfsPath)) {
               existedFilePath = existedFilePath + hdfsPath + Symbol.COMMA_STRING;
            }
         } else {
            logger.error(" Path [{}] Doesn't Exist in HDFS", hdfsPath);
         }
      }

      if (existedFilePath.length() >= 1) {
         logger.error(" Paths [{}] exists in HDFS", existedFilePath);
         return existedFilePath.substring(0, existedFilePath.length() - 1);
      } else {
         return null;
      }
   }

   public List<String> validProcessorConfigurations() {
      List<String> list = new ArrayList();
      if (!Utils.hasValidValue(this.directoryPath)) {
         list.add("Exception inside ORCRead processor named " + this.processorName + " : path is " + this.directoryPath + " that must not be " + this.directoryPath);
      }

      return list;
   }
}
