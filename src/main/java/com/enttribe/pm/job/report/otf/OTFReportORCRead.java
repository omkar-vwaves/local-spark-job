package com.enttribe.pm.job.report.otf;
// package com.enttribe.sparkrunner.custom.pm.udf;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.constants.SparkRunnerConstants;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processor.audit.ProcessorAudit;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.HDFSUtil;
import com.enttribe.sparkrunner.util.Utils;
import com.enttribe.sparkrunner.workflowengine.CustomSchema;

import static org.apache.spark.sql.functions.*;


import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 */
public class OTFReportORCRead extends Processor {

	private static final long serialVersionUID = 1L;

	public String inputSchema;

	private static Logger logger = LoggerFactory.getLogger(OTFReportORCRead.class);

	private String tempTable = "CounterData";

	public OTFReportORCRead() {
		super();
	}

	@Override
	public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {

		try {

			logger.info("====== OTFReportORCRead Execution Started! ======");
			long startTime = System.currentTimeMillis();

			HashMap<String, String> parameters = jobContext.getParameters();

			String quarterlyDateList = parameters.get("quarterlyDateList");
			String categoryList = parameters.get("categoryString");
			String timeShiftWiseDate = parameters.get("timeShiftWiseDateMap");

			logger.info("Quarterly Date List={}, Category List={}, Time Shift Wise Date={}", quarterlyDateList,
					categoryList, timeShiftWiseDate);

			java.lang.reflect.Type type = new TypeToken<Map<String, List<String>>>() {
			}.getType();

			Map<String, List<String>> timeShiftWiseDateMap = new Gson().fromJson(timeShiftWiseDate, type);
			logger.info("Time Shift Wise Date Map={}", timeShiftWiseDateMap);

			String basePath = parameters.get("basePath");
			logger.info("Base Path={}", basePath);

			this.dataFrame = readCustomParquet(categoryList, quarterlyDateList, basePath, jobContext,
					timeShiftWiseDateMap);

			Utils.createOrReplaceTempView(tempTable, this.dataFrame, processorName);
			setPathsInJobContext(jobContext);

			long endTime = System.currentTimeMillis();
			long durationMillis = endTime - startTime;
			long minutes = durationMillis / (1000 * 60);
			long seconds = (durationMillis / 1000) % 60;

			this.dataFrame.show(5, false);

			logger.info("====== OTFReportORCRead Execution Completed! Time taken: {} minutes, {} seconds ======",
					minutes, seconds);

		} catch (Exception e) {
			logger.error("Exception While Reading Parquet For All Category, Message : {}, Error : {}", e.getMessage(),
					e);
		}

		return this.dataFrame;
	}

	private void setPathsInJobContext(JobContext jobContext) {

		// String HDFSFileWritePath = jobContext.getParameter("HDFSFileWritePath");
		String HDFSFileWritePath = jobContext.getParameter("SWF_FILE_WRITE_PATH");
		String frequency = jobContext.getParameter("frequency");
		String reportName = jobContext.getParameter("reportName");

		logger.info("SWF_FILE_WRITE_PATH: {}, FREQUENCY={}, REPORT_NAME={}", HDFSFileWritePath, frequency, reportName);

		String cleanedHDFSPath = HDFSFileWritePath.endsWith("/")
				? StringUtils.substringBeforeLast(HDFSFileWritePath, "/")
				: HDFSFileWritePath;
		String finalHDFSFileWritePath = "s3a://" + cleanedHDFSPath + "/" + frequency + "/" + getDate() + "/" + "gzip"
				+ "/" + reportName + "_" + getTime() + ".csv.gz";
		String sanitizedPath = finalHDFSFileWritePath.replaceAll(" ", "-");
		logger.info("HDFS_FILE_WRITE_PATH :{} ", sanitizedPath);
		jobContext.setParameters("HDFS_FILE_WRITE_PATH", sanitizedPath);

		String readPath = sanitizedPath.replace("s3a://", jobContext.getParameter("SPARK_MINIO_BUCKET_NAME_PM") + ":/");
		logger.info("HDFS_FILE_READ_PATH :{} ", readPath);
		jobContext.setParameters("HDFS_FILE_READ_PATH", readPath);

		String finalFileWritePath = StringUtils.substringBeforeLast(sanitizedPath, "gzip");
		logger.info("HDFS_FINAL_FILE_WRITE_PATH :{} ", finalFileWritePath);
		jobContext.setParameters("HDFS_FINAL_FILE_WRITE_PATH", finalFileWritePath);
	}

	private String getDate() {
		SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");
		String date = outputFormat.format(new Date());
		return date;
	}

	private String getTime() {
		return String.valueOf(System.currentTimeMillis());
	}

	private Dataset<Row> getMetaColumsData(JobContext jobContext, String categoryList,
			Map<String, List<String>> timeShiftWiseDateMap) throws SQLException, Exception {

		logger.info("====== getMetaColumsData Method Started! ======");

		String META_PARQUET = jobContext.getParameter("META_PARQUET");
		logger.info("Required META_PARQUET={} ", META_PARQUET);

		StringBuilder metaParquetReadPath = new StringBuilder();
		Map<String, String> optionsMap = getParquetReadOptions(META_PARQUET);
		logger.info("Options Map={}", optionsMap);

		Dataset<Row> metaColumnParquet = getEmptyDataFrameForMetaColumn(jobContext);
		// metaColumnParquet.show();
		// logger.info("Empty Meta Column Parquet Created Successfully!");

		String finalMetaParquetPath = "";
		String metaColumnPath = jobContext.getParameter("metaColumnPath");
		logger.info("Final Meta Column Path==>{} ", metaColumnPath);

		String categoryWiseMetaParquetPath = jobContext.getParameter("categoryWiseMetaParquetPath");
		Map<String, String> categoryWiseMetaParquetPaths = new Gson().fromJson(categoryWiseMetaParquetPath, Map.class);

		logger.info("Category Wise Meta Parquet Paths={}", categoryWiseMetaParquetPaths);

		SimpleDateFormat inputFormat = new SimpleDateFormat("yyMMdd");
		SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");

		logger.info("Time Shift Wise Date Map={}", timeShiftWiseDateMap); // {0=[25012*]}, {0=[25012*]}

		try {
			for (Map.Entry<String, List<String>> timeShiftWiseDates : timeShiftWiseDateMap.entrySet()) {
				for (String dates : timeShiftWiseDates.getValue()) {
					String date = dates.trim().substring(0, 6);
					String newDate = outputFormat.format(inputFormat.parse(date));
					if (jobContext.getParameter("vendor").equalsIgnoreCase("ALL")) {
						for (String category : categoryList.split(Symbol.COMMA_STRING)) {
							metaColumnPath = categoryWiseMetaParquetPaths.get(category.split("@")[0]);
							metaParquetReadPath = metaParquetReadPath.append(metaColumnPath + "date=" + newDate + "/,");
						}
					} else {
						metaParquetReadPath = metaParquetReadPath.append(metaColumnPath + "date=" + newDate + "/,");
					}
				}
			}
			finalMetaParquetPath = metaParquetReadPath.toString();

			// if (Utils.hasValidValue(finalMetaParquetPath)) {
			// finalMetaParquetPath = getExistingFilePaths(finalMetaParquetPath);
			// }

			/*
			 * temp change
			 */

			// finalMetaParquetPath =
			// "s3a://bntvpm/TRINO_ALLCOUNTER/NE_META/d=TRANSPORT/v=JUNIPER/emstype=NA/t=COMMON/date=20250119/";

			// logger.info("Final Meta Parquet Path={}", finalMetaParquetPath);
			logger.info("Options Map={}", optionsMap);

			metaColumnParquet = jobContext.getFileReader().options(optionsMap)
					.orc(finalMetaParquetPath.split(Symbol.COMMA_STRING));

			if ("true".equalsIgnoreCase(jobContext.getParameter("isNodeLevel"))) {
				metaColumnParquet = metaColumnParquet.withColumn("NAM", col("H1"));
				logger.info("NAM column replaced with H1 for node-level data.");
			} else {
				logger.info("No change in H1 column since isNodeLevel is not true.");
			}

			metaColumnParquet.show(5, false);
			logger.info("Meta Column Parquet Displayed Successfully!");

		} catch (Exception e) {
			logger.error("Exception While Reading MetaColumn Parquet, Message={}, Error={}", e.getMessage(), e);
			metaColumnParquet = getEmptyDataFrameForMetaColumn(jobContext);
			metaColumnParquet.show(5, false);
			logger.info("Empty Meta Column Parquet Created Successfully!");
		}

		String[] columns = metaColumnParquet.columns();
		StringBuilder mapQuery = new StringBuilder();
		StringBuilder joinQuery = new StringBuilder();

		logger.info("Columns={}", Arrays.toString(columns));

		StringBuilder finalMapQuery = new StringBuilder(" map(");
		for (String col : columns) {
			mapQuery = mapQuery.append("`" + col + "`,");
			if (!col.equalsIgnoreCase("pmemsid")) {
				joinQuery = joinQuery.append("m.`" + col + "`,");
				finalMapQuery = finalMapQuery.append("'" + col + "' ,m.`" + col + "` ,");
			}
		}

		logger.info("Map Query={}", mapQuery);
		logger.info("Join Query={}", joinQuery);
		logger.info("Final Map Query={}", finalMapQuery);

		String joinFinalQuery = StringUtils.substringBeforeLast(joinQuery.toString(), ",");
		String metaInfo = getMetaInfo(jobContext);
		logger.info("Meta Info={}", metaInfo);
		String finalQueryMap = StringUtils.substringBeforeLast(finalMapQuery.toString(), ",") + ") as metaData";

		logger.info("Final Query Map={}", finalQueryMap);
		logger.info("Join Final Query={}", joinFinalQuery);

		jobContext.setParameters("finalQueryMap", finalQueryMap);
		jobContext.setParameters("joinFinalQuery", joinFinalQuery);

		String mapSelect = StringUtils.substringBeforeLast(mapQuery.toString(), ",");

		logger.info("Map Select={}", mapSelect);
		logger.info("Join Final Query={}", joinFinalQuery);

		String finalQuery = "SELECT " + mapSelect + " FROM metaColumn";
		if (!StringUtils.isEmpty(metaInfo)) {
			finalQuery = finalQuery + " WHERE " + metaInfo;
		}
		logger.info("Executing Query={} ", finalQuery);
		Utils.createOrReplaceTempView("metaColumn", metaColumnParquet, processorName);
		metaColumnParquet = jobContext.sqlctx().sql(finalQuery);
		metaColumnParquet.show(5, false);
		logger.info("==> Meta Column Parquet Displayed Successfully! <==");

		String query = jobContext.getParameter("dynamicQuery");
		logger.info("Dynamic Query={}", query);

		if ((StringUtils.isNotEmpty(jobContext.getParameter("geography_l1"))
				&& jobContext.getParameter("geography_l1").contains("Custom"))
				&& jobContext.getParameter("node").contains("CELL") && !StringUtils.isEmpty(query)) {
			Dataset<Row> PolygonCellsDataset;
			List<Row> rowList = new ArrayList<>();
			StructType returnType = getReturnTypeForMetaInfo();
			ResultSet rs = createConnection(query, jobContext);
			while (rs.next()) {
				rowList.add(RowFactory.create(rs.getString(1)));
			}
			PolygonCellsDataset = jobContext.createDataFrame(rowList, returnType);
			Utils.createOrReplaceTempView("PolygonCellsDataset", PolygonCellsDataset, processorName);
			metaColumnParquet = jobContext.sqlctx()
					.sql("SELECT m.* FROM metaColumn m INNER JOIN PolygonCellsDataset pc ON m.NEID =pc.neid");
			Utils.createOrReplaceTempView("metaColumn", metaColumnParquet, processorName);
		}
		Utils.createOrReplaceTempView("metaColumn", metaColumnParquet, processorName);
		return metaColumnParquet;
	}

	private ResultSet createConnection(String sqlQuery, JobContext jobContext) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		final String JDBC_DRIVER = jobContext.getParameter("driver");
		final String DB_URL = jobContext.getParameter("url");
		final String USER = jobContext.getParameter("username");
		final String PASS = jobContext.getParameter("pwd");
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.prepareStatement(sqlQuery);
			rs = stmt.executeQuery();
		} catch (SQLException | ClassNotFoundException e) {
			logger.error("Error while fetching data from DB - {}", Utils.getStackTrace(e));
		} finally {
			doFinally(conn, stmt);
		}
		return rs;
	}

	private void doFinally(Connection conn, PreparedStatement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				logger.error("Error in Closing Statement in parse - {}", Utils.getStackTrace(e));
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Error in Closing connection in parse- {}", Utils.getStackTrace(e));
			}
		}
	}

	private String getSubCategory(String subCategoryHeader, String subCategoryValue, String subcategory) {
		if (!subCategoryValue.contains("Individual")) {
			String subCat1Condition = "`" + subCategoryHeader + "`" + " in ('" + subCategoryValue.replace(",", "','")
					+ "')";
			subcategory = checkAndAppend(subcategory, subCat1Condition);
		}
		return subcategory;
	}

	private String checkAndAppend(String subcategory, String subCat1Condition) {
		if (subcategory != null && !subcategory.equalsIgnoreCase("null")) {
			subcategory = subcategory + " and " + subCat1Condition;
		} else {
			subcategory = subCat1Condition;
		}
		return subcategory;
	}

	private Dataset<Row> readCustomParquet(String categoryList, String dateList, String basePath, JobContext jobContext,
			Map<String, List<String>> timeShiftWiseDateMap) {

		logger.info("====== readCustomParquet Method Started! ======");

		String frequency = jobContext.getParameter("frequency");
		String counterParquet = jobContext.getParameter("COUNTER_PARQUET");

		logger.info("Frequency={}, Counter Parquet={}", frequency, counterParquet);
		// s3a://bntvpm/PERFORMANCE/ALLCOUNTER/

		String[] sequenceCols = null;
		String selectString = Symbol.EMPTY_STRING;
		String moColumnsString = Symbol.EMPTY_STRING;
		String counterIds = jobContext.getParameter("finalCounterInfo");
		StringBuilder counterReadPath = new StringBuilder();
		String rawCounters = Symbol.EMPTY_STRING;
		String date = jobContext.getParameter("finalDateStringKey");
		String finalFrequencyKey = jobContext.getParameter("finalFrequencyKey");

		logger.info("Final Frequency Key={}, Counter IDs={}", finalFrequencyKey, counterIds);

		Dataset<Row> finalDataFrame = getFinalEmptyDataFrame(jobContext, counterIds);

		Utils.createOrReplaceTempView("CounterData", finalDataFrame, processorName);
		try {
			Set<String> sequenceNoList = new HashSet<>();
			sequenceNoList.add("quarterKey");
			sequenceNoList.add("dateKey");
			sequenceNoList.add("hourKey");
			sequenceNoList.add("pmemsid");
			sequenceNoList.add("categoryname");

			for (String counterID : counterIds.split(",")) {
				String sequenceno = counterID.split("#")[0];
				sequenceNoList.add(sequenceno);
			}

			logger.info("Sequence No List={}", sequenceNoList);

			String selectBuilder = " SELECT quarterKey, dateKey, hourKey," + finalFrequencyKey
					+ " AS finalKey, pmemsid, categoryname, ";

			String catgoryInfoMapString = jobContext.getParameter("catgoryInfoMap");
			String categoryWiseBasePath = jobContext.getParameter("categoryWiseBasePath");
			Map<String, String> categoryWiseBasePaths = new Gson().fromJson(categoryWiseBasePath, Map.class);
			Map<String, List<Map<String, String>>> catgoryInfoMap = new Gson().fromJson(catgoryInfoMapString,
					Map.class);

			logger.info("Select Builder={}", selectBuilder);
			logger.info("Category List={}", categoryList);

			logger.info("Category Wise Base Paths={}", categoryWiseBasePaths);
			logger.info("Catgory Info Map={}", catgoryInfoMap);

			Map<String, String> optionsMap = getBasicParquetOptions(counterParquet);
			logger.info("Options Map={}", optionsMap);

			logger.info("Time Shift Wise Date Map={}", timeShiftWiseDateMap);

			String rawCounterList = "";
			for (Map.Entry<String, List<String>> timeShiftWiseDates : timeShiftWiseDateMap.entrySet()) {
				String timeShift = timeShiftWiseDates.getKey();
				timeShift = timeShift.equals("0") ? "" : "ts" + timeShift;
				// logger.info("timeShift Value :{} ", timeShift);
				for (String dates : timeShiftWiseDates.getValue()) {
					for (String category : categoryList.split(Symbol.COMMA_STRING)) {
						// logger.info("Getting category is :{} ", category);
						String parquetBasePath = !jobContext.getParameter("vendor").isEmpty()
								&& jobContext.getParameter("vendor").equalsIgnoreCase("ALL")
										? categoryWiseBasePaths.get(category.split("@")[0])
										: basePath;
						// s3a://performance/JOB/ORC/QUARTERLY/domain=TRANSPORT/vendor=JUNIPER/emstype=NA/technology=COMMON

						if (MapUtils.isNotEmpty(catgoryInfoMap) && catgoryInfoMap.containsKey(category)) {
							List<Map<String, String>> catInfoList = catgoryInfoMap.get(category);
							StringBuilder moColumns = new StringBuilder();
							catInfoList.forEach(e -> {
								if (e.get("isKPICounter") != null && e.get("isKPICounter").equalsIgnoreCase("0")) {
									moColumns.append("'" + e.get("counterHeaderName") + "'" + Symbol.COMMA_STRING
											+ "'=$'," + "COALESCE(`C" + e.get("sequenceNo") + "`,'null')"
											+ Symbol.COMMA_STRING + "'@',");
									sequenceNoList.add("C" + e.get("sequenceNo")); // + ":" + DataTypes.StringType);
								}
							});
							moColumnsString = "Concat(" + StringUtils.substringBeforeLast(moColumns.toString(), ",'@'")
									+ ") as moHierachy ";
							// logger.info("Getting moColumnsString is: {}", moColumnsString);
							String subCatSequenceCols = Symbol.EMPTY_STRING;
							Set<String> variableIdList = new HashSet<String>();
							for (Map<String, String> counterInfoMap : catInfoList) {
								String subcategory = getSubCategoryCriteria(counterInfoMap);
								subCatSequenceCols = subcategory;
								String variableId = counterInfoMap.get("pmcountervariableid_pk");
								if (!variableIdList.contains(counterInfoMap.get("pmcountervariableid_pk"))) {
									String counter = "C" + counterInfoMap.get("sequenceNo") + "#"
											+ counterInfoMap.get("pmcountervariableid_pk") + timeShift;
									// logger.info("selectBuilder : {} ", selectBuilder);
									if (!selectBuilder.contains(counter)) {
										if (subcategory != null) {
											selectBuilder = selectBuilder + " (CASE WHEN " + subcategory + " THEN (`C"
													+ counterInfoMap.get("sequenceNo") + "`) ELSE NULL END) as `C"
													+ counterInfoMap.get("sequenceNo") + "#"
													+ counterInfoMap.get("pmcountervariableid_pk") + timeShift + "`,";
											variableIdList.add(variableId);
										} else {
											selectBuilder = selectBuilder + " (`C" + counterInfoMap.get("sequenceNo")
													+ "`)  as `C" + counterInfoMap.get("sequenceNo") + "#"
													+ counterInfoMap.get("pmcountervariableid_pk") + timeShift + "` ,";
											variableIdList.add(variableId);
										}
										rawCounters = rawCounters + "`C" + counterInfoMap.get("sequenceNo") + "#"
												+ counterInfoMap.get("pmcountervariableid_pk") + timeShift + "`,";
										rawCounterList = rawCounterList + "c.`C" + counterInfoMap.get("sequenceNo")
												+ "#" + counterInfoMap.get("pmcountervariableid_pk") + timeShift + "`,";
										variableIdList.add(variableId);
									}
								}

							}
							// logger.info("selectString is :{} and subCatSequenceCols : {} ", selectString,
							// subCatSequenceCols);
							if (StringUtils.isNotEmpty(subCatSequenceCols)) {
								sequenceNoList.addAll(Arrays.asList(subCatSequenceCols.split("and")).stream()
										.map(e -> StringUtils.substringBetween(e, "`", "`"))// + ":" +
																							// DataTypes.StringType)
										.collect(Collectors.toList()));
							}
							// logger.info("sequenceNoList : {} ", sequenceNoList);

							// s3a://performance/JOB/ORC/QUARTERLY/domain=TRANSPORT/vendor=JUNIPER/emstype=NA/technology=COMMON
							sequenceCols = sequenceNoList.toArray(new String[sequenceNoList.size()]);

							String datePart = dates.trim().substring(0, 6);
							String timePart = dates.trim().substring(dates.trim().length() - 4);
							String cleanCategoryName = category.split("@")[0].toUpperCase().trim();

							// counterReadPath = counterReadPath.append(parquetBasePath + "/date="
							// + dates.trim().substring(0, 6) + "/time=*/ptime=*/categoryname="
							// + category.split("@")[0].toUpperCase().trim() + "/,");

							counterReadPath = counterReadPath.append(parquetBasePath + "/date="
									+ datePart + "/time=" + timePart + "/ptime=*/categoryname="
									+ cleanCategoryName + "/,");
						}
					}
				}
			}

			String finalCounterPath = counterReadPath.toString();

			// Distinct
			String[] paths = finalCounterPath.split(",");
			Set<String> distinctPaths = new LinkedHashSet<>(Arrays.asList(paths));
			String distinctFinalCounterPath = distinctPaths.stream().collect(Collectors.joining(","));
			finalCounterPath = distinctFinalCounterPath.trim();

			// String finalCounterPath = counterParquet;
			logger.info("Generated finalCounterPath is ={} ", finalCounterPath);
			// logger.info("finalCounterPath :{} and sequenceNoList :{} and date :{} ",
			// finalCounterPath, sequenceNoList,
			// date);

			List<Column> columns = new ArrayList<>();
			for (String s : sequenceCols) {
				columns.add(new Column(s));
			}

			logger.info("Columns={}", columns);

			// Seq<Column> seqColumns =
			// JavaConverters.asScalaBufferConverter(columns).asScala();

			// Column[] seqColumns = (Column[]) columns.toArray();

			Column[] seqColumns = columns.toArray(new Column[0]);

			logger.info("Seq Columns={}", seqColumns);

			// if (Utils.hasValidValue(finalCounterPath)) {
			// finalCounterPath = getExistingFilePaths(finalCounterPath);
			// logger.info(" inside if finalCounterPath :{} ", finalCounterPath);
			// }

			/*
			 * temp change
			 */

			// bntvpm/PERFORMANCE/ALLCOUNTER/domain=TRANSPORT/vendor=JUNIPER/emstype=NA/technology=COMMON/date=250119/time=0700
			// finalCounterPath =
			// "s3a://bntvpm/PERFORMANCE/ALLCOUNTER/domain=TRANSPORT/vendor=JUNIPER/emstype=NA/technology=COMMON/date=250119/time=0700/ptime=*/categoryname=INTERFACE/";

			logger.info("Final Counter Path={}", finalCounterPath);

			selectString = StringUtils.substringBeforeLast(selectBuilder.toString(), ",");
			logger.info("Select String={}", selectString);

			Dataset<Row> metaColumsData = getMetaColumsData(jobContext, categoryList, timeShiftWiseDateMap);
			metaColumsData.show(5, false);
			logger.info("Meta Columns Dataframe Displayed Successfully!");

			Utils.createOrReplaceTempView("metaColumn", metaColumsData, processorName);
			logger.info("Meta Column Temp Table Created Successfully!");

			try {
				Dataset<Row> singleCategoryData = null;

				logger.info("Final Counter Path={}", finalCounterPath);
				logger.info("Frequency={}", frequency);
				// logger.info("Date={}", date);
				logger.info("Options Map={}", optionsMap);
				logger.info("Sequence Columns={}", Arrays.toString(seqColumns));
				logger.info("Select String={}", selectString);
				logger.info("Mo Columns String={}", moColumnsString);
				logger.info("Raw Counters={}", rawCounters);
				logger.info("Raw Counter List={}", rawCounterList);
				logger.info("Final Query Map={}", jobContext.getParameter("finalQueryMap"));
				logger.info("Join Final Query={}", jobContext.getParameter("joinFinalQuery"));

				if (StringUtils.isNotEmpty(finalCounterPath) && !finalCounterPath.equalsIgnoreCase("null")) {
					if (!StringUtils.isEmpty(frequency) && (frequency.equalsIgnoreCase("DAILY")
							|| frequency.equalsIgnoreCase("PERDAY") || frequency.equalsIgnoreCase("WEEKLY")
							|| frequency.equalsIgnoreCase("PERWEEK") || frequency.equalsIgnoreCase("MONTHLY")
							|| frequency.equalsIgnoreCase("PERMONTH"))) {
						singleCategoryData = jobContext.getFileReader().options(optionsMap)
								.orc(finalCounterPath.split(Symbol.COMMA_STRING)).select(seqColumns)
								.where("dateKey in ('" + date.replace(",", "','") + "')");
					} else if (!StringUtils.isEmpty(frequency)
							&& (frequency.equalsIgnoreCase("PERHOUR") || frequency.equalsIgnoreCase("HOURLY"))) {
						singleCategoryData = jobContext.getFileReader().options(optionsMap)
								.orc(finalCounterPath.split(Symbol.COMMA_STRING)).select(seqColumns)
								.where("hourKey in ('" + date.replace(",", "','") + "')");
					} else if (!StringUtils.isEmpty(frequency)
							&& (frequency.equalsIgnoreCase("15 Min") || frequency.equalsIgnoreCase("QUARTERLY"))) {

						logger.info("Reading Per Quarter Data For Frequency={}!", frequency);

						String[] dateArray = date.split(",");
						Set<String> distinctDates = Arrays.stream(dateArray)
								.filter(s -> s != null && !s.trim().isEmpty())
								.collect(Collectors.toCollection(LinkedHashSet::new));

						String distinctDateString = String.join(",", distinctDates);

						String quarterKey = distinctDateString.replace(",", "','");
						logger.info("Quarter Key={}", quarterKey);

						String quarterKeyQuery = "quarterKey IN ('" + quarterKey + "')";
						logger.info("Quarter Key Query={}", quarterKeyQuery);

						// singleCategoryData = jobContext.getFileReader().options(optionsMap)
						// .orc(finalCounterPath.split(",")).select(seqColumns)
						// .where(quarterKeyQuery);

						Configuration hadoopConf = jobContext.sqlctx().sparkContext().hadoopConfiguration();
						FileSystem fs = FileSystem.get(new java.net.URI("s3a://performance"), hadoopConf);

						String[] paths1 = finalCounterPath.split(",");
						for (String rawPath : paths1) {
							Path globPath = new Path(rawPath);
							try {
								// Match wildcards (ptime=*)
								FileStatus[] matched = fs.globStatus(globPath);
								if (matched != null && matched.length > 0) {
									Dataset<Row> df = jobContext.getFileReader()
											.options(optionsMap)
											.orc(rawPath)
											.select(seqColumns)
											.where(quarterKeyQuery);

									singleCategoryData = (singleCategoryData == null)
											? df
											: singleCategoryData.unionByName(df, true);
								} else {
									logger.info("Skipping non-existent glob path: " + rawPath);
								}
							} catch (Exception e) {
								logger.info("Error while checking or reading path: " + rawPath, e);
							}
						}

						if (singleCategoryData == null) {
							singleCategoryData = jobContext.sqlctx().emptyDataFrame();
						}

					}

					singleCategoryData.show(5, false);
					logger.info("==> Single Category Data Displayed Successfully! <==");

					Utils.createOrReplaceTempView("singleCategoryData", singleCategoryData, processorName);

					String singleCategoryQuery = selectString + Symbol.COMMA_STRING + moColumnsString
							+ " FROM singleCategoryData";

					logger.info("Generated Single Category Query : {} ", singleCategoryQuery);
					singleCategoryData = jobContext.sqlctx().sql(singleCategoryQuery);

					singleCategoryData.show(5, false);
					logger.info("==> Single Category Data Displayed Successfully! <==");

					Utils.createOrReplaceTempView("CounterData", singleCategoryData, processorName);

					String finalQueryMap = jobContext.getParameter("finalQueryMap");
					logger.info("finalQueryMap : {} ", finalQueryMap);

					String finalQuery = "SELECT c.quarterKey, c.dateKey, c.hourKey, c.finalKey, c.pmemsid, c.moHierachy, c.categoryname, m.NAM, "
							+ rawCounterList + finalQueryMap
							+ " FROM CounterData c JOIN metaColumn m ON UPPER(c.pmemsid) =UPPER(m.pmemsid) AND c.dateKey= m.date";

					// String finalQuery = "SELECT
					// c.quarterKey,c.dateKey,c.hourKey,c.finalKey,c.pmemsid,c.moHierachy,c.categoryname,m.NAM,"
					// + rawCounterList + finalQueryMap
					// + " FROM CounterData c JOIN metaColumn m ON UPPER('111.11.11.0000019')
					// =UPPER(m.NEID) AND c.dateKey= m.date";
					logger.info("Generated Final Join Query =========> {} ", finalQuery);

					// SELECT c.quarterKey, c.dateKey, c.hourKey, c.finalKey, c.pmemsid,
					// c.moHierachy, c.categoryname, m.NAM,
					// c.`C6#29380`,c.`C11#29385`,c.`C2#29376`,c.`C23#29396`, map('L1' ,m.`L1` ,'L2'
					// ,m.`L2` ,'L3' ,m.`L3` ,'L4' ,m.`L4` ,'H1' ,m.`H1` ,'H2' ,m.`H2` ,'ENB'
					// ,m.`ENB` ,'NAM' ,m.`NAM` ,'M33' ,m.`M33` ,'NET' ,m.`NET` ,'metaData'
					// ,m.`metaData` ) as metaData FROM CounterData c JOIN metaColumn m ON
					// UPPER(c.pmemsid) =UPPER(m.NEID) AND c.dateKey= m.date

					/*
					 *
					 * removed temp UPPER(c.pmemsid) =UPPER(m.pmemsid) have to add pmemsid in trino
					 * 
					 * 
					 * column can be change as
					 * 'moHierachy' moHierachy ,m.nename NAM
					 *
					 * 
					 * NEID
					 */
					finalDataFrame = jobContext.sqlctx().sql(finalQuery);
					finalDataFrame.show(5, false);
					logger.info("==> Final Query DataFrame Displayed Successfully! <==");
					Utils.createOrReplaceTempView("CounterData", finalDataFrame, processorName);
				}
			} catch (Exception e1) {
				logger.error("Exception while reading Parquet,{}", Utils.getStackTrace(e1));
				finalDataFrame = getFinalEmptyDataFrame(jobContext, counterIds);
			}
		} catch (Exception e) {
			ProcessorAudit.exceptionLog(SparkRunnerConstants.ORC_READ, this.processorName, this.applicationId,
					"IgnoreNull Dataset Exception in Parquet Read", e.getMessage());
			boolean ignore = Boolean.parseBoolean(this.ignoreNullDataset);
			logger.info("[{}] ignoreNullDataset :[{}] , error : {}", this.processorName, ignore,
					ExceptionUtils.getStackTrace(e));
		}
		// ==========================*update MetaData Query*============================
		rawCounters = StringUtils.substringBeforeLast(rawCounters, ",");
		String metaDataFinalQuery = "SELECT quarterKey, dateKey, hourKey, finalKey, categoryname, NAM, moHierachy, parseData.rawdata.metaData, "
				+ rawCounters
				+ " FROM (SELECT CreateMetaData(metaData, moHierachy) AS rawdata, quarterKey, finalKey, dateKey, NAM, hourKey, categoryname, moHierachy, "
				+ rawCounters + " FROM CounterData) AS parseData";

		logger.info("***** metaDataFinalQuery : {} *****", metaDataFinalQuery);
		finalDataFrame = jobContext.sqlctx().sql(metaDataFinalQuery);

		Utils.createOrReplaceTempView("CounterData", finalDataFrame, processorName);
		// logger.info("finalDataFrame is printed");
		// finalDataFrame.show();
		return finalDataFrame;
	}

	// public StructType getSchema(String[] schema) {
	// // String[] schema = inputSchema.split(Symbol.COMMA_STRING);
	// List<StructField> fields = new ArrayList<>();
	// for (String col : schema) {
	// String[] colNamesAndDataTypes = col.split(Symbol.COLON_STRING);
	// if
	// (colNamesAndDataTypes[SparkRunnerConstants.ONE_INT].equalsIgnoreCase("ARRAY")
	// && colNamesAndDataTypes.length == SparkRunnerConstants.INDEX_OF_ARRAY) {
	// fields.add(
	// DataTypes.createStructField(colNamesAndDataTypes[SparkRunnerConstants.ZERO_INT],
	// DataTypes.createArrayType(CustomSchema.getSparkSQLType(
	// CustomSchema.getType(colNamesAndDataTypes[SparkRunnerConstants.TWO_INT]))),
	// true));

	// } else if
	// (colNamesAndDataTypes[SparkRunnerConstants.ONE_INT].equalsIgnoreCase("MAP")
	// && colNamesAndDataTypes.length == SparkRunnerConstants.INDEX_OF_MAP) {
	// fields.add(DataTypes.createStructField(colNamesAndDataTypes[SparkRunnerConstants.ZERO_INT],
	// DataTypes.createMapType(
	// CustomSchema.getSparkSQLType(
	// CustomSchema.getType(colNamesAndDataTypes[SparkRunnerConstants.TWO_INT])),
	// CustomSchema.getSparkSQLType(
	// CustomSchema.getType(colNamesAndDataTypes[SparkRunnerConstants.THREE_INT]))),
	// true));
	// } else {
	// fields.add(
	// DataTypes.createStructField(colNamesAndDataTypes[SparkRunnerConstants.ZERO_INT],
	// CustomSchema.getSparkSQLType(
	// CustomSchema.getType(colNamesAndDataTypes[SparkRunnerConstants.ONE_INT])),
	// true));
	// }

	// }
	// StructType structType = DataTypes.createStructType(fields);
	// return structType;

	// }

	private Dataset<Row> getFinalEmptyDataFrame(JobContext jobContext, String replace) {

		if (jobContext == null) {
			throw new IllegalArgumentException("JobContext Cannot Be NULL.");
		}

		try {

			StructType schema = getReturnType(replace);
			if (schema == null) {
				throw new IllegalStateException("Return Type Schema is NULL For Replace Value: " + replace);
			}

			Dataset<Row> emptyDataset = jobContext.createDataFrame(Collections.emptyList(), schema);
			return emptyDataset;
		} catch (Exception e) {
			logger.error("Error Creating Empty DataFrame For Replace='{}': {}", replace, e.getMessage(), e);
			StructType fallbackSchema = new StructType();
			return jobContext.createDataFrame(Collections.emptyList(), fallbackSchema);
		}
	}

	// private Dataset<Row> getFinalEmptyDataFrame(JobContext jobContext, String
	// replace) {

	// logger.info("===> Starting getFinalEmptyDataFrame() with replace: {}",
	// replace);

	// Dataset<Row> returnDataset;
	// StructType returnType = getReturnType(replace);
	// List<Row> rowList = new ArrayList<>();
	// returnDataset = jobContext.createDataFrame(rowList, returnType);

	// returnDataset.show();
	// logger.info("Empty DataFrame Displayed Successfully!");

	// return returnDataset;
	// }

	private String getMetaInfo(JobContext jobContext)
			throws JSONException, JsonParseException, JsonMappingException, IOException {

		StringBuilder filterCondition = new StringBuilder();
		ObjectMapper objectMapper = new ObjectMapper();

		String nodeValue = jobContext.getParameter("nodeVal");
		String reportLevel = jobContext.getParameter("reportLevel");
		String cellList = jobContext.getParameter("cellList");
		String filterLevel = jobContext.getParameter("filterLevel");
		String analysisObjectJson = jobContext.getParameter("analysisObject");
		String geoListJson = jobContext.getParameter("listOfGeography");
		String cellOwner = jobContext.getParameter("cellOwner");
		String magnetFieldsJson = jobContext.getParameter("magnetFields");
		String jvidColumnJson = jobContext.getParameter("jvidColumn");
		String netype = jobContext.getParameter("netype");

		logger.info(
				"Initial Filter Level: {} With Parameters: netype={}, nodeValue={}, reportLevel={}",
				filterLevel, netype, nodeValue, reportLevel);

		// Add netype filter
		if (StringUtils.isNotBlank(netype)) {
			filterCondition.append(" UPPER(NET) IN ('")
					.append(netype.toUpperCase().replace(",", "','")).append("')");
		}

		// Add cell owner filter
		if (StringUtils.isNotBlank(cellOwner)) {
			filterCondition.append(" AND M33 IN ('")
					.append(cellOwner.replace(",", "','")).append("')");
		}

		// Process magnet fields
		if (StringUtils.isNotBlank(magnetFieldsJson)) {
			try {
				List<Map<String, Object>> magnetFields = objectMapper.readValue(
						magnetFieldsJson, new TypeReference<>() {
						});
				for (Map<String, Object> field : magnetFields) {
					String column = (String) field.get("columnName");
					String value = String.valueOf(field.get("value")).replaceAll("[\\[\\]]", "'").replace(", ", "','");
					filterCondition.append(" AND ").append(column).append(" IN (").append(value).append(")");

					logger.info("Magnet Field Filter Added: {} IN ({})", column, value);
				}
			} catch (Exception e) {
				logger.error("Failed to Parse magnetFields: {}", magnetFieldsJson, e);
			}
		}

		// Process JVId column filter
		if (StringUtils.isNotBlank(jvidColumnJson)) {
			try {
				List<Map<String, Object>> jvidFields = objectMapper.readValue(
						jvidColumnJson, new TypeReference<>() {
						});
				for (Map<String, Object> field : jvidFields) {
					String column = (String) field.get("columnName");
					String value = String.valueOf(field.get("value")).replaceAll("[\\[\\]]", "'").replace(", ", "','");
					filterCondition.append(" AND ").append(column).append(" LIKE '").append(value).append("'");

					logger.info("JVId Column Filter Added: {} LIKE '{}'", column, value);
				}
			} catch (Exception e) {
				logger.error("Failed to Parse jvidColumn: {}", jvidColumnJson, e);
			}
		}

		// Process geography list
		if (StringUtils.isNotBlank(geoListJson)) {
			try {
				Map<String, List<String>> geoListMap = new Gson().fromJson(geoListJson, Map.class);
				for (Map.Entry<String, List<String>> entry : geoListMap.entrySet()) {
					String levelKey = entry.getKey();
					List<String> geoValues = entry.getValue();
					if (!geoValues.isEmpty()) {
						String joinedValues = String.join("','", geoValues);

						if (joinedValues.toUpperCase().contains("CLUBBED")
								|| joinedValues.toUpperCase().contains("INDIVIDUAL")) {
							// Nothing to do
						} else {
							filterCondition.append(" AND UPPER(").append(levelKey)
									.append(") IN ('").append(joinedValues).append("')");
						}

						logger.info("Geography Filter Added: {} IN ({})", levelKey, joinedValues);
					}
				}
			} catch (Exception e) {
				logger.error("Failed to Parse listOfGeography: {}", geoListJson, e);
			}
		}

		// Cell list condition based on filter level
		if (StringUtils.isNotBlank(filterLevel) && StringUtils.isNotBlank(cellList)) {
			if (filterLevel.contains(Symbol.UNDERSCORE_STRING)) {
				String baseLevel = StringUtils.substringBefore(filterLevel, Symbol.UNDERSCORE_STRING);
				filterCondition.append(" AND ").append(baseLevel)
						.append(" IN ('").append(cellList.replace(",", "','")).append("')");
			} else if ("NEID".equalsIgnoreCase(filterLevel)) {
				filterCondition.append(" AND NAM IN ('").append(cellList.replace(",", "','")).append("')");
			}
		}

		// Analysis object filter
		if (StringUtils.isNotBlank(analysisObjectJson)) {
			try {
				Map<String, String> filterMap = new Gson().fromJson(analysisObjectJson, Map.class);
				if (!filterMap.isEmpty()) {
					String filterKey = String.join(",", filterMap.keySet());
					Set<String> filterValues = new HashSet<>(filterMap.values());
					String joinedValues = String.join("','", filterValues);
					filterCondition.append(" AND ").append(filterKey)
							.append(" IN ('").append(joinedValues).append("')");

					logger.info("Analysis Object Filter Added: {} IN ({})", filterKey, joinedValues);
				}
			} catch (Exception e) {
				logger.error("Failed to Parse analysisObject: {}", analysisObjectJson, e);
			}
		}

		// Custom report level override
		if ("CUSTOM".equalsIgnoreCase(reportLevel) && !"INDIVIDUAL".equalsIgnoreCase(nodeValue)) {
			jobContext.setParameters("filterLevel", "L0");
			logger.info("Custom reportLevel Detected. Updated filterLevel to L0.");
		}

		// Override for NODE_LEVEL parameter
		if ("NODE_LEVEL".equalsIgnoreCase(jobContext.getParameter("NODE_LEVEL"))) {
			filterLevel = "NODE";
		}

		// Final override for NODE level
		if ("NODE".equalsIgnoreCase(filterLevel)) {
			jobContext.setParameters("filterLevel", "NAM");
			logger.info("NODE level detected. Updated filterLevel to NAM.");
		}

		String filter = filterCondition.toString().trim();
		logger.info("Generated Filter : {}", filter);
		return filter;
	}

	// private String getMetaInfo(JobContext jobContext)
	// throws JSONException, JsonParseException, JsonMappingException, IOException {

	// String condition = Symbol.EMPTY_STRING;
	// String nodeVal = jobContext.getParameter("nodeVal");
	// String reportLevel = jobContext.getParameter("reportLevel");
	// String cellList = jobContext.getParameter("cellList");
	// String level = jobContext.getParameter("filterLevel");
	// logger.info("Checking the filterLevel for NODE case : {} ", level);
	// String analysisObject = jobContext.getParameter("analysisObject");
	// String listOfGeography = jobContext.getParameter("listOfGeography");
	// Map<String, List<String>> geoLists = new Gson().fromJson(listOfGeography,
	// Map.class);
	// String cellOwner = jobContext.getParameter("cellOwner");
	// String magnetFields = jobContext.getParameter("magnetFields");
	// String jvidColumn = jobContext.getParameter("jvidColumn");
	// if (StringUtils.isNotEmpty(jobContext.getParameter("netype"))) {
	// String netype = jobContext.getParameter("netype");
	// // netype = netype + ",INTERFACE";
	// condition = condition + " UPPER(NET) in ('" + netype.replace(",",
	// "','").toUpperCase() + "')";
	// }

	// if (!StringUtils.isEmpty(cellOwner)) {
	// condition = condition + " and M33 in ('" + cellOwner.replace(",", "','") +
	// "')";
	// }
	// if (!StringUtils.isEmpty(magnetFields)) {
	// magnetFields = Arrays.asList(magnetFields.split(",")).toString().trim();
	// logger.info("magnetFields: {}", magnetFields);
	// Map<String, Object> map = new HashMap<>();
	// ObjectMapper mapper = new ObjectMapper();
	// List<Map<String, Object>> readValue = mapper.readValue(magnetFields,
	// new TypeReference<List<Map<String, Object>>>() {
	// });
	// readValue.forEach(a -> {
	// map.put((String) a.get("columnName"), a.get("value"));
	// });
	// logger.info("map : {}", map.toString());
	// for (Entry<String, Object> entry : map.entrySet()) {
	// String column = entry.getKey();
	// String value = StringUtils.replaceIgnoreCase(
	// StringUtils.replaceIgnoreCase(
	// StringUtils.replaceIgnoreCase(entry.getValue().toString().trim(), "[", "'"),
	// "]", "'"),
	// ", ", "','").trim();
	// logger.info("column : {} ,value : {} ", column, value);
	// condition = condition + " AND " + column + " in (" + value + ")";
	// }
	// }
	// if (!StringUtils.isEmpty(jvidColumn)) {
	// jvidColumn = Arrays.asList(jvidColumn.split(",")).toString().trim();
	// Map<String, Object> map = new HashMap<>();
	// ObjectMapper mapper = new ObjectMapper();
	// List<Map<String, Object>> readValue = mapper.readValue(jvidColumn,
	// new TypeReference<List<Map<String, Object>>>() {
	// });
	// readValue.forEach(a -> {
	// map.put((String) a.get("columnName"), a.get("value"));
	// });
	// logger.info("map : {}", map.toString());
	// for (Entry<String, Object> entry : map.entrySet()) {
	// String column = entry.getKey();
	// String value = StringUtils.replaceIgnoreCase(
	// StringUtils.replaceIgnoreCase(
	// StringUtils.replaceIgnoreCase(entry.getValue().toString().trim(), "[", "'"),
	// "]", "'"),
	// ", ", "','").trim();
	// logger.info("column : {} ,value : {} ", column, value);
	// condition = condition + " AND " + column + " like '" + value + "'";
	// }
	// }
	// logger.info("geoLists is {}", geoLists);
	// for (Entry<String, List<String>> geoList : geoLists.entrySet()) {
	// String levelKey = geoList.getKey();
	// List<String> geoValues = geoList.getValue();
	// logger.info("geoValues size is {}", geoValues.size());
	// // if (geoValues.size() > 0) {
	// condition = condition + " and UPPER(" + levelKey + ") in ('"
	// + String.join(",", geoValues).replace(",", "','")
	// + "')";
	// // } else {
	// // logger.info("geoValues size is 0");
	// // }

	// }

	// if (level != null && level.contains(Symbol.UNDERSCORE_STRING) &&
	// StringUtils.isNotEmpty(cellList)) {
	// level = StringUtils.substringBefore(level, Symbol.UNDERSCORE_STRING);
	// condition = condition + " and " + level + " in ('" + cellList.replace(",",
	// "','") + "')";
	// } else if (level != null && level.equals("NEID") &&
	// StringUtils.isNotEmpty(cellList)) {
	// condition = condition + " and NAM in ('" + cellList.replace(",", "','") +
	// "')";
	// }
	// if (StringUtils.isNotEmpty(analysisObject)) {
	// Map<String, String> filterValue = new Gson().fromJson(analysisObject,
	// Map.class);
	// String filterKey =
	// filterValue.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.joining(","));
	// Set<String> collect =
	// filterValue.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toSet());
	// String join = String.join(",", collect.toString());
	// String value = StringUtils.replace(join.replace("[[", ""), "]]", "");
	// condition = condition + " and " + filterKey + " in ('" + value + "')";
	// }
	// if (reportLevel.equalsIgnoreCase("CUSTOM") &&
	// !nodeVal.equalsIgnoreCase("INDIVIDUAL")) {
	// jobContext.setParameters("filterLevel", "L0");
	// logger.info("Checking the filterLevel for Custom case : {} ",
	// jobContext.getParameter("filterLevel"));
	// }

	// if ("NODE_LEVEL".equalsIgnoreCase(jobContext.getParameter("NODE_LEVEL"))) {
	// level = "NODE";
	// }
	// if (level != null && level.equalsIgnoreCase("NODE")) {
	// jobContext.setParameters("filterLevel", "NAM");
	// logger.info("Checking the filterLevel for NODE case : {} ",
	// jobContext.getParameter("filterLevel"));
	// }
	// return condition;
	// }

	private String getSubCategoryCriteria(Map<String, String> counterInfoMap) {
		String subcategory = null;

		try {
			for (int i = 1; i <= 4; i++) {
				String headerKey = "subcategoryHeader" + i;
				String valueKey = "subcategoryValue" + i;

				String subcategoryHeader = counterInfoMap.get(headerKey);
				String subcategoryValue = counterInfoMap.get(valueKey);

				if (subcategoryHeader != null && subcategoryValue != null) {
					subcategory = getSubCategory(subcategoryHeader, subcategoryValue, subcategory);
				}
			}
		} catch (Exception e) {
			logger.error("Error While Building Subcategory Criteria From counterInfoMap, Message: {}, Error: {}",
					e.getMessage(), e);
		}

		return subcategory;
	}

	// private String getSubCategoryCriteria(Map<String, String> counterInfoMap) {
	// String subcategory = null;
	// if (counterInfoMap.get("subcategoryHeader1") != null &&
	// counterInfoMap.get("subcategoryValue1") != null) {
	// subcategory = getSubCategory(counterInfoMap.get("subcategoryHeader1"),
	// counterInfoMap.get("subcategoryValue1"), subcategory);
	// }
	// if (counterInfoMap.get("subcategoryHeader2") != null &&
	// counterInfoMap.get("subcategoryValue2") != null) {
	// subcategory = getSubCategory(counterInfoMap.get("subcategoryHeader2"),
	// counterInfoMap.get("subcategoryValue2"), subcategory);
	// }
	// if (counterInfoMap.get("subcategoryHeader3") != null &&
	// counterInfoMap.get("subcategoryValue3") != null) {
	// subcategory = getSubCategory(counterInfoMap.get("subcategoryHeader3"),
	// counterInfoMap.get("subcategoryValue3"), subcategory);
	// }
	// if (counterInfoMap.get("subcategoryHeader4") != null &&
	// counterInfoMap.get("subcategoryValue4") != null) {
	// subcategory = getSubCategory(counterInfoMap.get("subcategoryHeader4"),
	// counterInfoMap.get("subcategoryValue4"), subcategory);
	// }
	// return subcategory;
	// }

	public static String getRawCounter(
			String[] columnsList,
			Map<String, Map<String, String>> counterInfoMap,
			String rawCounterList,
			JobContext jobContext) {

		StringBuilder finalQueryBuilder = new StringBuilder();
		StringBuilder rawCounterBuilder = new StringBuilder();

		try {
			if (columnsList == null || columnsList.length == 0) {
				logger.warn("Column List is Empty/NULL. Returning Empty Query.");
				return "";
			}

			for (String column : columnsList) {
				String trimmedColumn = column.trim();
				if (!trimmedColumn.equalsIgnoreCase("finalKey") &&
						!trimmedColumn.equalsIgnoreCase("pmEmsId")) {

					Map<String, String> counterInfo = counterInfoMap.get(trimmedColumn.toUpperCase());
					if (counterInfo != null) {
						String sequenceNo = counterInfo.get("sequenceNo");
						String counterId = counterInfo.get("pmcountervariableid_pk");

						if (sequenceNo != null && counterId != null) {
							String rawColumn = "`C" + sequenceNo + "#" + counterId + "`";
							finalQueryBuilder.append(rawColumn).append(", ");
							rawCounterBuilder.append("c.").append(rawColumn).append(", ");
						} else {
							logger.warn("Missing sequenceNo or pmcountervariableid_pk for column: {}", trimmedColumn);
						}
					} else {
						logger.warn("No Counter Info Found For Column: {}", trimmedColumn);
					}
				}
			}

			// Remove trailing commas if necessary
			String finalQuery = StringUtils.stripEnd(finalQueryBuilder.toString(), ", ");
			String rawCounterQuery = StringUtils.stripEnd(rawCounterBuilder.toString(), ", ");

			jobContext.setParameters("rawCounterListQuery", rawCounterQuery);

			return finalQuery;

		} catch (Exception e) {
			logger.error("Exception While Generating Raw Counter Query. Columns: {}, Message: {}, Error: {}",
					Arrays.toString(columnsList), e.getMessage(), e);
			return "";
		}
	}

	// public static String getRawCounter(String[] columnsList, Map<String,
	// Map<String, String>> counterInfoMap,
	// String rawCounterList, JobContext jobContext) {

	// String finalQuery = "";
	// try {
	// for (String columnString : columnsList) {
	// if (!columnString.trim().equalsIgnoreCase("finalKey")
	// && !columnString.trim().equalsIgnoreCase("pmEmsId")
	// && counterInfoMap.get(columnString.toUpperCase().trim()) != null) {
	// Map<String, String> counterInfo =
	// counterInfoMap.get(columnString.toUpperCase().trim());

	// finalQuery = finalQuery + "`C" + counterInfo.get("sequenceNo") + "#"
	// + counterInfo.get("pmcountervariableid_pk") + "` ,";
	// rawCounterList = rawCounterList + "c.`C" + counterInfo.get("sequenceNo") +
	// "#"
	// + counterInfo.get("pmcountervariableid_pk") + "` ,";
	// }
	// }
	// rawCounterList = StringUtils.substringBeforeLast(rawCounterList.toString(),
	// ",");
	// jobContext.setParameters("rawCounterListQuery", rawCounterList);
	// } catch (Exception e) {
	// logger.error("Exception in getting rawCouter Map String : {},{}",
	// e.getStackTrace(), counterInfoMap);
	// }

	// return StringUtils.substringBeforeLast(finalQuery.toString(), ",");// + ") as
	// rawCounters";
	// }

	private Dataset<Row> getEmptyDataFrameForMetaColumn(JobContext jobContext) {
		try {
			StructType schema = getReturnTypeForMetaColumn();
			List<Row> emptyRows = Collections.emptyList();

			Dataset<Row> emptyDataFrame = jobContext.createDataFrame(emptyRows, schema);
			logger.info("Empty DataFrame For Meta Column Created With Schema: {}", schema.simpleString());

			return emptyDataFrame;
		} catch (Exception e) {
			logger.error("Failed to Create Empty DataFrame For Meta Column, Message : {}, Error : {}", e.getMessage(),
					e);
			return jobContext.createDataFrame(Collections.emptyList(), new StructType());
		}
	}

	// private Dataset<Row> getEmptyDataFrameForMetaColumn(JobContext jobContext) {
	// Dataset<Row> returnDataset;
	// StructType returnType = getReturnTypeForMetaColumn();
	// List<Row> rowList = new ArrayList<>();
	// returnDataset = jobContext.createDataFrame(rowList, returnType);
	// return returnDataset;
	// }

	public StructType getReturnType(String countersCommaSeparated) {

		List<StructField> fields = new ArrayList<StructField>();

		try {

			if (countersCommaSeparated == null || countersCommaSeparated.trim().isEmpty()) {
				logger.error("Input String For Counters is NULL/Empty. Proceeding With Static Fields Only.");
			}

			// Static fields
			fields.add(DataTypes.createStructField("quarterKey", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("dateKey", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("hourKey", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("moHierachy", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("pmemsid", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("NAM", DataTypes.StringType, true));
			fields.add(DataTypes.createStructField("categoryName", DataTypes.StringType, true));

			// Dynamic counter fields
			if (countersCommaSeparated != null && !countersCommaSeparated.trim().isEmpty()) {
				String[] counters = countersCommaSeparated.split(",");
				for (String counter : counters) {
					fields.add(DataTypes.createStructField(counter.trim(), DataTypes.StringType, true));
				}
			}

			// MetaData field
			fields.add(DataTypes.createStructField(
					"metaData",
					DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true),
					true));

			logger.info("Successfully Constructed return StructType With {} Fields.", fields.size());

		} catch (Exception e) {
			logger.error("Exception While Building Return Type Schema. Input={}, Message={}, Error={}",
					countersCommaSeparated, e.getMessage(), e);
		}

		return DataTypes.createStructType(fields);

	}

	// fields.add(DataTypes.createStructField("quarterKey", DataTypes.StringType,
	// true));
	// fields.add(DataTypes.createStructField("dateKey", DataTypes.StringType,
	// true));
	// fields.add(DataTypes.createStructField("hourKey", DataTypes.StringType,
	// true));
	// fields.add(DataTypes.createStructField("moHierachy", DataTypes.StringType,
	// true));
	// fields.add(DataTypes.createStructField("pmemsid", DataTypes.StringType,
	// true));
	// fields.add(DataTypes.createStructField("NAM", DataTypes.StringType, true));
	// fields.add(DataTypes.createStructField("categoryName", DataTypes.StringType,
	// true));
	// for (int i = 0; i < counterList.size(); i++) {
	// fields.add(DataTypes.createStructField(counterList.get(i),
	// DataTypes.StringType, true));
	// }
	// fields.add(DataTypes.createStructField("metaData",
	// DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true),
	// true));
	// return DataTypes.createStructType(fields);

	public StructType getReturnTypeForMetaInfo() {
		return DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("neid", DataTypes.StringType, true), });
	}

	public StructType getReturnTypeForMetaColumn() {
		return DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("pmemsid", DataTypes.StringType, true),
				DataTypes.createStructField("L1", DataTypes.StringType, true),
				DataTypes.createStructField("L2", DataTypes.StringType, true),
				DataTypes.createStructField("L3", DataTypes.StringType, true),
				DataTypes.createStructField("L4", DataTypes.StringType, true),
				DataTypes.createStructField("H1", DataTypes.StringType, true),
				DataTypes.createStructField("H2", DataTypes.StringType, true),
				DataTypes.createStructField("ENB", DataTypes.StringType, true),
				DataTypes.createStructField("NAM", DataTypes.StringType, true),
				DataTypes.createStructField("M33", DataTypes.StringType, true),
				DataTypes.createStructField("NET", DataTypes.StringType, true),
				DataTypes.createStructField("metaData",
						DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true), });
	}

	private Map<String, String> getBasicParquetOptions(String baseParquetPath) {
		Map<String, String> options = new HashMap<>();

		try {

			if (baseParquetPath == null || baseParquetPath.trim().isEmpty()) {
				logger.error("Base Parquet Path is NULL/Empty.");
				return options;
			}

			options.put("basePath", baseParquetPath);
			options.put("mergeSchema", "true");

			logger.info("Basic Parquet Options Set: {}", options);

		} catch (Exception e) {
			logger.error("Error While Preparing Basic Parquet Options For Path : {}, Message : {}, Error : {}",
					baseParquetPath, e.getMessage(), e);
		}

		return options;
	}

	private Map<String, String> getParquetReadOptions(String baseParquetPath) {

		Map<String, String> options = new HashMap<>();

		try {
			if (baseParquetPath == null || baseParquetPath.trim().isEmpty()) {
				logger.error("Provided Parquet Base Path is NULL/Empty.");
				return options;
			}

			options.put("basePath", baseParquetPath);
			logger.info("Parquet Read Options Prepared With basePath: {}", baseParquetPath);
		} catch (Exception e) {
			logger.error("Failed to Prepare Parquet Read Options For Path: {}, Message: {}, Error: {}", baseParquetPath,
					e.getMessage(), e);
		}
		return options;
	}

	// private Map<String, String> getParquetReadOptions(String counterParquet) {
	// Map<String, String> options = new HashMap<>();
	// options.put("basePath", counterParquet);
	// return options;
	// }

	public static boolean isHdfsFolderExist(String folderPath) {
		try {
			FileSystem fileSystem = HDFSUtil.getFileSystem();
			Path path = new Path(folderPath);
			FileStatus[] statuses = fileSystem.globStatus(path);

			if (statuses != null) {
				for (FileStatus status : statuses) {
					if (status.isDirectory()) {
						logger.info("HDFS Directory Exists: {}", folderPath);
						return true;
					}
				}
			}

			logger.info("No Directory Found At Path: {}", folderPath);
		} catch (Exception e) {
			logger.error("Error While Checking HDFS Directory Existence For Path : {}, Message : {}, Error : {}",
					folderPath, e.getMessage(), e);
		}

		return false;
	}

	// public static boolean isHdfsFolderExist(String folder) {
	// try {
	// FileSystem fileSystem = HDFSUtil.getFileSystem();
	// Path path = new Path(folder);
	// FileStatus[] globStatus = fileSystem.globStatus(path);
	// if (Arrays.asList(globStatus).toString().contains("isDirectory=true")) {
	// return true;
	// }

	// } catch (Exception e) {
	// logger.error("Error in reading from the directory : {}", e.getMessage());

	// }
	// return false;
	// }

	public static String getExistingFilePaths(String commaSeparatedPaths) {

		StringBuilder existingPaths = new StringBuilder();

		try {
			String[] pathArray = commaSeparatedPaths.split(Symbol.COMMA_STRING);

			for (String path : pathArray) {
				if (isHdfsFolderExist(path)) {
					if (existingPaths.indexOf(path) == -1) {
						existingPaths.append(path).append(Symbol.COMMA_STRING);
						logger.info("Path Exists: {}", path);
					}
				} else {
					logger.info("Path Does Not Exist: {}", path);
				}
			}

			if (existingPaths.length() > 0) {
				// Remove Trailing Comma
				existingPaths.setLength(existingPaths.length() - 1);
				return existingPaths.toString();
			}
		} catch (Exception e) {
			logger.error("Error While Checking Existing HDFS Path: {}, Message : {}, Error : {}", commaSeparatedPaths,
					e.getMessage(), e);
		}

		return existingPaths.toString();
	}

	// public static String getExistingFilePaths(String filePaths) {
	// String existedFilePath = Symbol.EMPTY_STRING;
	// for (String hdfsPath : filePaths.split(Symbol.COMMA_STRING)) {
	// if (isHdfsFolderExist(hdfsPath)) {
	// if (existedFilePath.contains(hdfsPath)) {
	// continue;
	// }
	// existedFilePath += hdfsPath + Symbol.COMMA_STRING;
	// } else {
	// }
	// }
	// if (existedFilePath.length() >= 1) {
	// return existedFilePath.substring(0, existedFilePath.length() - 1);
	// }
	// return null;
	// }

}