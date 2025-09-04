package com.enttribe.pm.job.trino;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.constants.SparkRunnerConstants;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Utils;

public class MetaColumnParquetSave extends Processor {
  private static final long serialVersionUID = 1L;
  private static String JDBC_DRIVER = null;
  private static String DB_URL = null;
  private static String USER = null;
  private static String PASS = null;

  private static Logger logger = LoggerFactory.getLogger(MetaColumnParquetSave.class);
  public String compressionType;
  public String partitionBy;
  @Dynamic
  public String path;
  public String saveMode;
  public String numPartition;

   private Map<String, String> getMagnetColumns(String domain,String technology) throws Exception {
    logger.error("Going to fetch KPI detail");
    Connection conn = null;
    PreparedStatement stmt = null;
    String magnetConfig = null;
    ResultSet rs = null;
    Map<String, Map<String, String>> magnetParamMap = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    try {
      Class.forName(JDBC_DRIVER);
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
      String sql = "select CONFIG_VALUE from BASE_CONFIGURATION where APPLICATION_NAME='PRODUCT_MANAGEMENT' and CONFIG_TYPE='PM' and CONFIG_KEY='MAGNET_PARAM_CONFIG'";
      logger.info("The sql query for getting MagnetParameterConfig - {}", sql);
      stmt = conn.prepareStatement(sql);
      rs = stmt.executeQuery();
      while (rs.next()) {
        magnetConfig = rs.getString(1);
      }
      magnetParamMap = mapper.readValue(magnetConfig, new TypeReference<Map<String, Map<String, String>>>() {});
      logger.info("magnetParamMap size in class @CreateMetaData - {}", magnetParamMap.size());
    } catch (Exception e) {
      logger.error("Error while fetching kpi map- {}", Utils.getStackTrace(e));
    } finally {
      close(conn, stmt, rs);
    }
    
    if(domain.equalsIgnoreCase("RAN")) {
		return magnetParamMap.entrySet().stream().filter(e -> e.getKey().startsWith(domain))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())).values().stream()
				.flatMap(e -> e.entrySet().stream())
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (s, a) -> s));
    }
    else {
		return magnetParamMap.get(domain + "_ALL") != null ? magnetParamMap.get(domain + "_ALL"): magnetParamMap.get(domain + "_" + technology);
    }
  }

  public String getdate() {
      LocalDateTime now = LocalDateTime.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
      return now.format(formatter);
  }

  private void initializeDBProperties(JobContext jobContext) {
    // JDBC_DRIVER = jobContext.getParameter("pt_driver");
    // DB_URL = jobContext.getParameter("pt_url");
    // USER = jobContext.getParameter("pt_username");
    // PASS = jobContext.getParameter("pt_pwd");
    JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    DB_URL = "jdbc:mysql://mysql-platform-cluster.platformdb.svc.cluster.local:6446/PLATFORM?autoReconnect=true";
    USER = "PLATFORM";
    PASS = "ptfrm#121sp";
    logger.info("DB_URL: {}, USER: {}, PASS: {}, JDBC_DRIVER: {}", DB_URL, USER, PASS, JDBC_DRIVER);
  }

  private void close(Connection conn, PreparedStatement stmt, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        logger.error("Error While  Closing ResultSet {}", ExceptionUtils.getStackTrace(e));
      }
    }
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        logger.error("Error While  Closing Statement {}", ExceptionUtils.getStackTrace(e));
      }
    }
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        logger.error("Error While  connection {}", ExceptionUtils.getStackTrace(e));
      }
    }
  }

  @Override
  public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
    initializeDBProperties(jobContext);
    String date = getdate();
    String domain = "TRANSPORT";//jobContext.getParameter("domain");
    String technology = "COMMON";//jobContext.getParameter("technology");
    String emstype = "NA";
	logger.info("domain:{},technology{}", domain,technology);
    Map<String, String> config = getMagnetColumns(domain,technology);
    logger.info("CustomParquetWrite time start {} ", new Date());
    setCompressType(jobContext);
    StringBuilder geoQuery = new StringBuilder();
    List<String> metaColsList = Arrays.asList("NEID","NET","NAM","H1","L1","L2","L3","L4","ENB","DNAM", "CEL", "NS", "BND", "ENB_NEID", "H2", "H2_NEID", "H1_NEID", "DL4", "DL3", "DL2", "DL1", "NEL" ,"pmemsid");
    for (String col : metaColsList) {
      geoQuery = geoQuery.append("metaData['" + col + "'] as `" + col + "`, ");
    }
    geoQuery.append("metaData['CEL'] as `CELL`, ");
    for (Map.Entry<String, String> entry : config.entrySet()) {
      geoQuery = geoQuery.append("metaData['" + entry.getKey() + "'] as `" + entry.getValue() + "`, ");
    }

    String geoSelect = StringUtils.substringBeforeLast(geoQuery.toString(), ",");
    
    String query = "select " + geoSelect + ",domain as d ,  vendor as v , '"+emstype+"' AS emsType ,technology as t,  " + date +" as date from MetaColumnsData";

   logger.info("final select query {}", query);


    Dataset<Row> finalDataFrame = this.dataFrame.sqlContext().sql(query);
    if (Utils.hasValidValue(partitionBy) && Utils.hasValidValue(this.saveMode)) {
      String[] colName = partitionBy.split(Symbol.SPACE_STRING);
      Column col[] = new Column[colName.length];
      for (int i = SparkRunnerConstants.ZERO_INT; i < colName.length; i++) {
        col[i] = new Column(colName[i]);
      }
      logger.info("All Counter Map expansion done");
      logger.info("File Path Is :"+path);
      finalDataFrame = finalDataFrame.repartition(1);
      finalDataFrame.write().mode(this.saveMode).partitionBy(colName).orc(this.path);
      finalDataFrame.show(3);
      logger.info("Print DATAFRAME  :");
    
    } else if (Utils.hasValidValue(this.saveMode)) {
      finalDataFrame.write().mode(this.saveMode).orc(this.path);
      finalDataFrame.show(2);
    } else {
      finalDataFrame.write().mode(SaveMode.ErrorIfExists).orc(this.path);
    }
    logger.info("CustomParquetWrite time end {} ", new Date());
    Utils.createOrReplaceTempView("MetaParquet", finalDataFrame, processorName);
    return this.dataFrame;

  }

  private void setCompressType(JobContext jobContext) {
    Properties prop = new Properties();
    if (Utils.hasValidValue(compressionType)) {
      prop.put("spark.sql.parquet.compression.codec", compressionType);
    } else {
      prop.put("spark.sql.parquet.compression.codec", "snappy");
    }
    jobContext.sqlctx().setConf(prop);
  }

  @Override
  public List<String> validProcessorConfigurations() {
    List<String> list = new ArrayList<>();
    if (!Utils.hasValidValue(this.path)) {
      list.add("Exception in ParquetWrite processor named " + this.processorName + " : path is " + this.path
          + " that must not be " + this.path);
    }
    return list;
  }
}