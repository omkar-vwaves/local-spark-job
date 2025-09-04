package com.enttribe.pm.job.report.otf;

// import java.awt.Color;
// import java.io.BufferedReader;
// import java.io.IOException;
// import java.io.InputStreamReader;
// import java.math.BigDecimal;
// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.PreparedStatement;
// import java.sql.SQLException;
// import java.text.DateFormat;
// import java.text.ParseException;
// import java.text.SimpleDateFormat;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Calendar;
// import java.util.Date;
// import java.util.GregorianCalendar;
// import java.util.HashMap;
// import java.util.Iterator;
// import java.util.LinkedHashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Map.Entry;
// import java.util.Set;
// import java.util.stream.Collectors;
// import java.util.zip.GZIPInputStream;
// import java.util.zip.GZIPOutputStream;

// import org.apache.commons.lang.NullArgumentException;
// import org.apache.commons.lang.StringEscapeUtils;
// import org.apache.commons.lang3.StringUtils;
// import org.apache.commons.lang3.exception.ExceptionUtils;
// import org.apache.commons.lang3.time.DateUtils;
// import org.apache.hadoop.fs.FSDataInputStream;
// import org.apache.hadoop.fs.FSDataOutputStream;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined;
// import org.apache.poi.ss.usermodel.BorderStyle;
// import org.apache.poi.ss.usermodel.Cell;
// import org.apache.poi.ss.usermodel.CellStyle;
// import org.apache.poi.ss.usermodel.CreationHelper;
// import org.apache.poi.ss.usermodel.FillPatternType;
// import org.apache.poi.ss.usermodel.Font;
// import org.apache.poi.ss.usermodel.HorizontalAlignment;
// import org.apache.poi.ss.usermodel.IndexedColors;
// import org.apache.poi.ss.usermodel.Row;
// import org.apache.poi.ss.usermodel.Sheet;
// import org.apache.poi.ss.usermodel.VerticalAlignment;
// import org.apache.poi.ss.usermodel.Workbook;
// import org.apache.poi.ss.util.CellRangeAddress;
// import org.apache.poi.ss.util.NumberToTextConverter;
// import org.apache.poi.xssf.streaming.SXSSFWorkbook;
// import org.apache.poi.xssf.usermodel.XSSFCellStyle;
// import org.apache.poi.xssf.usermodel.XSSFColor;
// import org.apache.poi.xssf.usermodel.XSSFFont;
// import org.apache.poi.xssf.usermodel.XSSFWorkbook;
// import org.apache.spark.SparkConf;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.SparkSession;
// import org.joda.time.DateTime;
// import org.joda.time.LocalDateTime;
// import org.json.JSONArray;
// import org.json.JSONException;
// import org.json.JSONObject;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import com.google.common.base.Joiner;
// import com.enttribe.commons.Symbol;
// import com.enttribe.sparkrunner.context.JobContext;
// import com.enttribe.sparkrunner.context.JobContextImpl;
// import com.enttribe.sparkrunner.processors.Processor;
// import com.enttribe.sparkrunner.util.Expression;
// import com.enttribe.sparkrunner.util.HDFSUtil;
// import com.enttribe.sparkrunner.util.Utils;

// import joptsimple.internal.Strings;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
// import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
// import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
// import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.enttribe.commons.Symbol;
import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.context.JobContextImpl;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.Expression;
// import com.enttribe.sparkrunner.util.HDFSUtil;
import com.enttribe.sparkrunner.util.Utils;

import joptsimple.internal.Strings;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.enttribe.commons.storage.s3.S3Client;

public class CreateCustomPMExcelReport extends Processor {
	private static Logger logger = LoggerFactory.getLogger(CreateCustomPMExcelReport.class);

	private static final long serialVersionUID = 1L;

	@Dynamic
	public String outputPath;

	private static final Integer PM_SHEET_NUMBER_OF_ROWS = 800000;
	private static final Integer CELL_HEIGHT_HEADER_NORMAL = 22;
	private static final Integer FONT_SIZE_VALUE_NORMAL = 11;
	private static final Integer CELL_WIDTH_LARGE = 6500;
	public static final String KPI_JSONKEY = "kpi";
	public static final String METACOLUMN = "metaColumn";
	public static final String DOMAIN_JSONKEY = "domain";
	public static final String NODE_JSONKEY = "node";
	public static final int INDEX_ZERO = 0;
	public static final int INDEX_EIGHTEEN = 18;
	public static final String TIME = "TIME";
	public static final String DATE = "DATE";
	public static final String DD_MM_YYYY = "dd/MM/yyyy";
	public static final String HH_MM = "HH:mm";
	public static final String BBH = "BBH";
	public static final String NBH = "NBH";
	public static final String RK = "RK";
	public static final String ENB = "ENB";
	public static final String CEL = "CEL";
	public static final String BND = "BND";
	public static String DUPLEX = "DUPLEX";
	public static final String ALL_ENODEB_INDIVIDUAL = "ALL ENODEB - INDIVIDUAL";
	public static final String RAN_DOMAIN = "RAN";
	public static final String BAND2300 = "2300";
	public static final String BAND850 = "850";
	public static String DUPLEX_TDD = "TDD";
	public static String DUPLEX_FDD = "FDD";
	public static final String FROMDATE_JSONKEY = "fromDate";
	public static final String TODATE_JSONKEY = "toDate";
	public static final String DURATION_JSONKEY = "duration";
	public static final String DAYNO = "1";
	public static final String PERHOURLIST_JSONKEY = "perHourList";
	public static final String YYYYWW = "yyyyww";
	public static final String YYYYMM = "yyyyMM";
	public static final String ZERO = "0";
	public static final String DURATION_WEEKLY_BBH = "WEEKLY_BBH";
	public static final String BLANK = " ";
	public static final String FREQUENCY_QUARTERLY = "15 Min";
	public static final String FREQUENCY_PERDAY = "PERDAY";
	public static final String FREQUENCY_PERWEEK = "PERWEEK";
	public static final String FREQUENCY_PERHOUR = "PERHOUR";
	public static final String FREQUENCY_PERMONTH = "PERMONTH";
	public static final String FREQUENCY_BUSIESTDAY = "BUSIESTDAY";
	public static final String FREQUENCY_5MIN = "5 Min";
	public static final String DURATION_DAILY = "DAILY";
	public static final String DURATION_HOURLY = "HOURLY";
	public static final String DURATION_WEEKLY = "WEEKLY";
	public static final String DURATION_MONTHLY = "MONTHLY";
	public static final String YYYYMMDD = "yyyyMMdd";
	public static final String REPORTFORMATTYPE_JSONKEY = "reportFormatType";
	private static final String SPARK_MINIO_ENDPOINT_URL = "SPARK_MINIO_ENDPOINT_URL";
	private static final String SPARK_MINIO_ACCESS_KEY = "SPARK_MINIO_ACCESS_KEY";
	private static final String SPARK_MINIO_SECRET_KEY = "SPARK_MINIO_SECRET_KEY";
	private static final String SPARK_MINIO_BUCKET_NAME_PM = "SPARK_MINIO_BUCKET_NAME_PM";

	private static String sparkMinioEndpointUrl;
	private static String sparkMinioAccessKey;
	private static String sparkMinioSecretKey;
	private static String sparkMinioBucketNamePm;

	private static final String PM_TIMEGAP_IN_MINUTES_FOR_QUARTER_FREQUENCY = "15";

	public CreateCustomPMExcelReport() {
		super();
		logger.info("CreateCustomPMExcelReport No-Argument ConstructorInitialized!");
	}

	public static void main(String[] args) throws Exception {

		String kpi = "[{\"subCatInfo\":\"\",\"headerName\":\"1066-Outbound traffic (MB)\",\"timeAggregation\":\"\",\"count\":\"0\",\"kpiName\":\"1066-Outbound traffic (MB)\",\"nodeAggregation\":\"\",\"cellPercent\":false,\"colorInputRangeConfig\":[],\"type\":\"KPI\",\"categoryName\":\"\",\"sequenceNo\":\"\",\"id\":25302,\"categoryId\":null,\"kpigroup\":\"OTHER\",\"kpicode\":\"1066\",\"Threshold\":false},{\"subCatInfo\":\"\",\"headerName\":\"1057-ifOutOctets (bytes)\",\"timeAggregation\":\"\",\"count\":\"1\",\"kpiName\":\"1057-ifOutOctets (bytes)\",\"nodeAggregation\":\"\",\"cellPercent\":false,\"colorInputRangeConfig\":[],\"type\":\"KPI\",\"categoryName\":\"\",\"sequenceNo\":\"\",\"id\":25293,\"categoryId\":null,\"kpigroup\":\"OTHER\",\"kpicode\":\"1057\",\"Threshold\":false},{\"subCatInfo\":\"\",\"headerName\":\"13977-Download Throughput\",\"timeAggregation\":\"\",\"count\":\"2\",\"kpiName\":\"13977-Download Throughput\",\"nodeAggregation\":\"\",\"cellPercent\":false,\"colorInputRangeConfig\":[],\"type\":\"KPI\",\"categoryName\":\"\",\"sequenceNo\":\"\",\"id\":25432,\"categoryId\":null,\"kpigroup\":\"OTHER\",\"kpicode\":\"13977\",\"Threshold\":false},{\"subCatInfo\":\"\",\"headerName\":\"1046-ifInErrors\",\"timeAggregation\":\"\",\"count\":\"3\",\"kpiName\":\"1046-ifInErrors\",\"nodeAggregation\":\"\",\"cellPercent\":false,\"colorInputRangeConfig\":[],\"type\":\"KPI\",\"categoryName\":\"\",\"sequenceNo\":\"\",\"id\":25282,\"categoryId\":null,\"kpigroup\":\"OTHER\",\"kpicode\":\"1046\",\"Threshold\":false},{\"subCatInfo\":\"\",\"headerName\":\"1061-ifSpeed\",\"timeAggregation\":\"\",\"count\":\"4\",\"kpiName\":\"1061-ifSpeed\",\"nodeAggregation\":\"\",\"cellPercent\":false,\"colorInputRangeConfig\":[],\"type\":\"KPI\",\"categoryName\":\"\",\"sequenceNo\":\"\",\"id\":25297,\"categoryId\":null,\"kpigroup\":\"OTHER\",\"kpicode\":\"1061\",\"Threshold\":false}]";
		SparkConf conf = new SparkConf()
				.setAppName("Spark Application")
				.setMaster("local[*]")
				.set("spark.driver.memory", "4g")
				.set("spark.driver.cores", "2")
				.set("spark.sql.shuffle.partitions", "200")
				.set("spark.default.parallelism", "100")
				.set("spark.local.dir", "/Users/ent-00356/Documents/spark-local-dir")
				.set("spark.network.timeout", "600s")
				.set("spark.sql.files.maxPartitionBytes", "134217728")

				// S3A configuration
				.set("spark.hadoop.fs.s3a.access.key", "bootadmin")
				.set("spark.hadoop.fs.s3a.secret.key", "bootadmin")
				.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
				.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
				.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
				.set("spark.hadoop.fs.s3a.path.style.access", "true")
				.set("spark.hadoop.fs.s3a.aws.credentials.provider",
						"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

		SparkSession session = SparkSession.builder()
				.config(conf)
				.getOrCreate();
		JobContext jobContext = new JobContextImpl(session);
		jobContext.setParameters(DOMAIN_JSONKEY, "TRANSPORT");
		jobContext.setParameters(NODE_JSONKEY, null);
		jobContext.setParameters(FREQUENCY_JSONKEY, FREQUENCY_QUARTERLY);
		jobContext.setParameters(DURATION_JSONKEY, "dates");
		jobContext.setParameters(FROMDATE_JSONKEY, "Jan 19,2025 0:00");
		jobContext.setParameters(TODATE_JSONKEY, "Jan 19,2025 23:00");
		jobContext.setParameters(VENDOR_JSONKEY, "JUNIPER");
		jobContext.setParameters(NETYPE_JSONKEY, null);
		jobContext.setParameters(REPORTTYPE, "OTF Report");
		jobContext.setParameters(REPORTNAME, "otf_report");
		jobContext.setParameters(REPORTLEVEL, "GeographyL0");
		jobContext.setParameters(REPORTGEOGRAPHY, "India");
		jobContext.setParameters("node", "Router");
		jobContext.setParameters(KPI_JSONKEY, kpi);
		jobContext.setParameters(REPORTFORMATTYPE_JSONKEY, "csv");
		jobContext.setParameters("metaColumn",
				"latitude,longitude,nefrequency,nename,nesource,nestatus,netype,uuid,technology,vendor,geographyl4id_fk,creationtime,modificationtime,emsserverid_fk,domain,swversion,mcc,mnc,neid,pmemsid,cmemsid,fmemsid,ipv4,ipv6,model,macaddress,floorid_fk,nestatuscriteria,duplex,enbid,ecgi,cellnum,friendlyName,parentneid_fk,dayonestatus,identifier,propertyid,radius,serialnumber,geographyl3id_fk,geographyl2id_fk,geographyl1id_fk,nestage,operationalstate,swbuildnumber,inservicedate,category,isvirtual,secured,hostname,fqdn,oldneid,physicalserialnumber,creatorid_fk,lastmodifierid_fk,oldnename#Latitude,Longitude,NE frequency,NE name,NE source,NE status,NE type,UUID,Technology,Vendor,Cluster,Creation time,Modification time,EMS server ID,Domain,Software version,MCC,MNC,NE ID,PM EMS ID,CM EMS ID,FM EMS ID,IPV4,IPV6,Model,Mac address,Floor ID,NE status criteria,Duplex,ENB ID,ECGI,CELL NUM,Friendly name,Parent NE ID,Day 1 status,Identifier,Property ID,Radius,Serial number,City,State,Region,NE stage,Operational state,SW build number,In service date,Category,Is virtual,Secured,Hostname,FQDN,OLD NE ID,Physical serial number,Creator,Last modifier,Old NE name");

		List<org.apache.spark.sql.Row> list = new ArrayList<>();
		StructType schema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("hdfsFilePath", DataTypes.StringType, false)
		});
		list.add(RowFactory.create("s3a://bntvpm/protected/PM/OnDemandReport/OTF_REPORT.csv.gz"));

		CreateCustomPMExcelReport processor = new CreateCustomPMExcelReport();
		processor.dataFrame = session.createDataFrame(list, schema);
		processor.outputPath = "s3a://bntvpm/protected/PM/OnDemandReport/";
		processor.executeAndGetResultDataframe(jobContext);
	}

	public CreateCustomPMExcelReport(int processorId, String processorName) {
		super(processorId, processorName);
	}

	public CreateCustomPMExcelReport(int processorId, String processorName, String outputPath) {
		super(processorId, processorName);
		this.outputPath = outputPath;
	}

	@Override
	public Dataset<org.apache.spark.sql.Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
		HashMap<String, String> parameterMap = jobContext.getParameters();

		this.sparkMinioEndpointUrl = parameterMap.get(SPARK_MINIO_ENDPOINT_URL);
		this.sparkMinioAccessKey = parameterMap.get(SPARK_MINIO_ACCESS_KEY);
		this.sparkMinioSecretKey = parameterMap.get(SPARK_MINIO_SECRET_KEY);
		this.sparkMinioBucketNamePm = parameterMap.get(SPARK_MINIO_BUCKET_NAME_PM);
		if (this.sparkMinioBucketNamePm == null) {
			this.sparkMinioBucketNamePm = "bntvpm";
		}

		this.outputPath = parameterMap.get("HDFS_FINAL_FILE_WRITE_PATH");
		logger.info("HDFS_FINAL_FILE_WRITE_PATH : {}", this.outputPath);

		String workbookName = getExcelReportName(parameterMap);
		jobContext.setParameters("workbookName", workbookName);
		String genaratedFilePath = this.outputPath + workbookName;
		String kpiJson = parameterMap.get(KPI_JSONKEY);
		String threshold = parameterMap.get("Threshold");
		String metaColumn = parameterMap.get(METACOLUMN);
		String domain = parameterMap.get(DOMAIN_JSONKEY);
		String node = parameterMap.get(NODE_JSONKEY);
		String moColumns = parameterMap.get("moColumns"); // "Category"
		String trendOTFReport = parameterMap.get("trendOTFReport");
		String csvReportName = workbookName.replaceAll(".xlsx", ".csv.gz");
		jobContext.setParameters("csvReportName", csvReportName);
		String extension = parameterMap.get(REPORTFORMATTYPE_JSONKEY);
		String duration = parameterMap.get(DURATION_JSONKEY);
		String startTime = parameterMap.get(FROMDATE_JSONKEY);
		String endTime = parameterMap.get(TODATE_JSONKEY);
		String dayNo = parameterMap.get("dayNo");
		String specificKey = parameterMap.get("specificKey");
		String dayno = "";
		if (dayNo != null && !dayNo.equalsIgnoreCase("")) {
			dayno = dayNo;
		} else if (duration.equalsIgnoreCase(DURATION_HOURLY) && (dayNo == null || dayNo.equalsIgnoreCase(""))) {
			dayno = "";
		} else {
			dayno = "1";
		}
		try {
			Workbook wb = createWorkBook(jobContext, workbookName);
			if (this.dataFrame != null) {
				List<org.apache.spark.sql.Row> dataList = this.dataFrame.collectAsList();
				for (org.apache.spark.sql.Row row : dataList) {
					String specTimeString = "";
					String hdfsFilePath = row.getString(0);
					logger.info("hdfsFilePath: {}", hdfsFilePath);
					// hdfsFilePath = hdfsFilePath.replaceAll("s3a://", "file:///");
					String frequency = parameterMap.get(FREQUENCY_JSONKEY);
					String sheetName = getSheetNameFromCsvFilePath(hdfsFilePath);
					if (!sheetName.contains("GeneratedReportId=") && !sheetName.contains("ReportwidgetId=")) {
						if (trendOTFReport != null && trendOTFReport.equalsIgnoreCase("true")) {
							String metaColumns = Symbol.EMPTY_STRING;
							Date stdDate = null;
							Date endDate = null;
							List<String> timekeyList = new ArrayList();
							List<String> csvHeader = new ArrayList(Arrays.asList(metaColumn.split("#")[0].split(",")));
							List<String> ExcelHeader = new ArrayList(
									Arrays.asList(metaColumn.split("#")[1].split(",")));

							logger.info(
									"csv Header is : {} and Excel Header is : {}  and index to remove date and time in header are : {} {} ",
									csvHeader, ExcelHeader, csvHeader.indexOf("DATE"), csvHeader.indexOf("TIME"));
							if (metaColumn.contains("DATE")) {
								ExcelHeader.remove(csvHeader.indexOf("DATE"));
								csvHeader.remove("DATE");
							}
							if (metaColumn.contains("TIME")) {
								ExcelHeader.remove(csvHeader.indexOf("TIME"));
								csvHeader.remove("TIME");
							}
							logger.info(
									"After removing Date and Time metaColumns is : {} and csvHeader is : {} and ExcelHeader is : {} and needRemove index is : {}",
									metaColumn, csvHeader, ExcelHeader);

							if (StringUtils.isNotEmpty(moColumns)) {
								metaColumns = Strings.join(csvHeader, ",") + "#" + Strings.join(ExcelHeader, ",")
										+ Symbol.COMMA_STRING + moColumns + ",KPI";
							} else {
								metaColumns = Strings.join(csvHeader, ",") + "#" + Strings.join(ExcelHeader, ",")
										+ ",KPI";
							}
							if (StringUtils.substringAfter(metaColumns, Symbol.HASH_STRING).equalsIgnoreCase(",KPI")) {
								metaColumns = "#" + StringUtils.substringAfter(metaColumns, Symbol.HASH_STRING)
										.replaceFirst(Symbol.COMMA_STRING, Symbol.EMPTY_STRING);
							}
							logger.info("final metaColumns is : {} ", metaColumns);

							if (specificKey.equalsIgnoreCase("true")) {
								timekeyList = getSpecificDateListFromContext(parameterMap.get("specificDateList"),
										frequency);
								timekeyList = timekeyList.stream().sorted().collect(Collectors.toList());
							} else {
								List<Date> dateList = getDateListFromConetext(parameterMap.get("specificDate"));
								List<String> hourList = getListFromtimeString(parameterMap.get(PERHOURLIST_JSONKEY),
										Symbol.COMMA_STRING, dateList);
								List<Map<String, Date>> dateRangeMap = getDateRangeNew(dayno, duration, startTime,
										endTime, dateList, hourList);
								logger.info("dateRangeMap in Excel : {} ", dateRangeMap);
								for (Map<String, Date> dateRange : dateRangeMap) {
									stdDate = dateRange.get(MINTIME);
									endDate = dateRange.get(MAXTIME);
									if (sheetName.equalsIgnoreCase(FREQUENCY_QUARTERLY)) {
										timekeyList = getQuarterly(hourList, stdDate, endDate, duration,
												PM_TIMEGAP_IN_MINUTES_FOR_QUARTER_FREQUENCY);
									} else if (sheetName.equalsIgnoreCase(FREQUENCY_PERHOUR)) {
										timekeyList = getPerhourList(hourList, stdDate, endDate, sheetName, duration);
									} else if (sheetName.equalsIgnoreCase(FREQUENCY_PERWEEK)
											|| sheetName.equalsIgnoreCase(DURATION_WEEKLY_BBH)) {
										timekeyList = parseWeekWithDate(stdDate, "ww-yyyy");

									} else if (frequency.equalsIgnoreCase(FREQUENCY_PERMONTH)) {
										// timekeyList = parseDateForKeyAndFormatMonth(stdDate, endDate, DD_MM_YYYY);
										Calendar calendar = new GregorianCalendar();
										String dateKey = "";
										calendar.setTime(stdDate);
										calendar.set(Calendar.DATE, 01);
										DateTime dateTime = new DateTime(calendar.getTime());
										dateKey = dateTime.toString("MMM-yyyy");
										timekeyList.add(dateKey);

									} else {
										timekeyList = parseDateFortimeKey(stdDate, endDate, DD_MM_YYYY, sheetName);
									}
								}
							}
							specTimeString += Joiner.on(",").join(timekeyList);
							logger.info("stdDate {} ,endDate {} ,timekeyList {} ,specTimeString {}  ", stdDate, endDate,
									timekeyList, specTimeString);
							String timeString = "";
							String[] splitString = specTimeString.split(",");
							int size = splitString.length;
							for (int i = 0; i < size; i++) {
								if (i != size - 1) {
									timeString = timeString + splitString[i] + ",";
								} else {
									timeString = timeString + splitString[i];
								}
							}
							logger.info("timeString --> {}", timeString);
							wb = createExcelWorkBook(wb, sheetName, hdfsFilePath, kpiJson, metaColumns, node, domain,
									moColumns, timeString, threshold);
						} else {
							wb = createKPIExcelWorkBook(wb, sheetName, hdfsFilePath, kpiJson, metaColumn, node, domain,
									moColumns, extension, parameterMap);
						}
					}
				}

				long fileSizeInBytes = getFileSizeInBytes(wb);
				String pushedDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

				// String fileSize = exportExcel(genaratedFilePath, wb, parameterMap,
				// getS3Client(jobContext));

				String generatedExcelFilePath = pushExcelFileToMinioRemote(wb, workbookName, pushedDate,
						jobContext.getParameters());

				logger.info("Report Format Type Is : {}", extension);

				if (!extension.equalsIgnoreCase("csv")) {

					logger.error("Excel File Push to SeaweedFS Successfully to Path: {}", generatedExcelFilePath);

					updateReportDetails(parameterMap, workbookName, generatedExcelFilePath,
							String.valueOf(fileSizeInBytes));

				} else {
					// String csvFilePath = this.outputPath + csvReportName;
					// s3a://bntvpm/protected/PM/OnDemandReport/GeneratedReportId=15780/15-Min/20250527/
					// logger.info("Starting CSV Conversion. Source: '{}', Target: '{}'",
					// genaratedFilePath, csvFilePath);

					// String outputFile = csvFilePath;

					// workbookName = workbookName.replace("xlsx", "csv");
					// String generatedCsvFilePath = pushCSVFileToMinioRemote(wb, workbookName,
					// pushedDate,
					// jobContext.getParameters());

					logger.info(
							"Provided Report Format Is 'CSV' Type, Getting Generated 'EXCEL' to Convert it to 'CSV'!");

					String generatedCsvFilePath = pushCSVFileToMinioRemoteFromExcelPath(generatedExcelFilePath,
							workbookName.replace("xlsx", "csv"), pushedDate, jobContext.getParameters());

					logger.error("CSV File Push to SeaweedFS Successfully to Path: {}", generatedCsvFilePath);

					// convertExceltoCSV(genaratedFilePath, outputFile);

					// Clean up .gz extension from the name, if present
					// csvReportName = csvReportName.replaceAll(".csv.gz", ".csv");
					// logger.info("CSV Report Name After Conversion: {}", csvReportName);

					updateReportDetails(parameterMap, workbookName, generatedCsvFilePath,
							String.valueOf(fileSizeInBytes));
				}
			}
			logger.error("OTF Report Pushed to SWF Successfully, Finished Time is : {}", new Date());

		} catch (Exception e) {
			logger.error("Exception While Creating Workbook, Message: {}, Error: {}", e.getMessage(), e);
			throw new Exception();
		}
		return this.dataFrame;
	}

	private static long getFileSizeInBytes(Workbook workbook) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			workbook.write(bos);
			System.out.println("File Size In Bytes = " + bos.toByteArray().length + ", In KB = "
					+ bos.toByteArray().length / 1024);
			return bos.toByteArray().length;
		} catch (IOException e) {
			System.out.println("Error in Getting File Size In Bytes, Message: " + e.getMessage() + ", Error: " + e);
			return 0;
		}
	}

	@SuppressWarnings("resource")
	private static void convertExceltoCSV(String generatedFilePath, String outputFile) throws Exception {
		FSDataOutputStream fout = null;
		FSDataInputStream fsin = null;
		FileSystem fileSystem = null;
		GZIPOutputStream gzipOutputStream = null;

		Workbook workbook = null;

		try {
			logger.info("Starting Excel to CSV conversion.");
			logger.info("Source Excel path: {}", generatedFilePath);
			generatedFilePath = generatedFilePath.replaceAll(" ", "-");

			fileSystem = getFileSystem(generatedFilePath);
			fsin = fileSystem.open(new Path(generatedFilePath));
			fout = fileSystem.create(new Path(outputFile), true);
			gzipOutputStream = new GZIPOutputStream(fout);

			workbook = new XSSFWorkbook(fsin);
			int numberOfSheets = workbook.getNumberOfSheets();
			StringBuilder data = new StringBuilder();

			for (int i = 0; i < numberOfSheets; i++) {
				Sheet sheet = workbook.getSheetAt(i);
				logger.info("Processing sheet: {}", sheet.getSheetName());
				for (Row row : sheet) {
					for (Cell cell : row) {
						switch (cell.getCellType()) {
							case BOOLEAN:
								data.append(cell.getBooleanCellValue()).append(",");
								break;
							case NUMERIC:
								if (DateUtil.isCellDateFormatted(cell)) {
									String formattedDate = new SimpleDateFormat("dd/MM/yyyy")
											.format(cell.getDateCellValue());
									data.append(formattedDate).append(",");
								} else {
									data.append(cell.getNumericCellValue()).append(",");
								}
								break;
							case STRING:
								data.append(cell.getStringCellValue()).append(",");
								break;
							case BLANK:
								data.append(",");
								break;
							default:
								data.append(cell.toString()).append(",");
						}
					}
					data.append('\n');
				}
			}

			gzipOutputStream.write(data.toString().getBytes(StandardCharsets.UTF_8));
			gzipOutputStream.finish();

			logger.info("Excel to CSV conversion completed successfully.");
			logger.info("Deleting original Excel file: {}", generatedFilePath);
			fileSystem.delete(new Path(generatedFilePath), true);

		} catch (Exception e) {
			logger.error("Error during Excel to CSV conversion: {}", ExceptionUtils.getStackTrace(e));
			throw e; // Rethrow so calling method knows it failed
		} finally {
			IOUtils.closeQuietly(gzipOutputStream);
			IOUtils.closeQuietly(fout);
			IOUtils.closeQuietly(fsin);
			IOUtils.closeQuietly(workbook);

			if (fileSystem != null) {
				try {
					long fileSizeBytes = fileSystem.getFileStatus(new Path(outputFile)).getLen();
					logger.info("CSV GZIP file size: {} bytes ({} KB)", fileSizeBytes, fileSizeBytes / 1024);
				} catch (IOException ioException) {
					logger.error("Unable to determine file size for output: {}, reason: {}", outputFile,
							ioException.getMessage());
				}

				IOUtils.closeQuietly(fileSystem);
			}
		}
	}

	public static void convertWorkbookToCSV(Workbook workbook, OutputStream outputStream) throws IOException {

		System.out.println("Starting to Convert Workbook to CSV");

		DataFormatter formatter = new DataFormatter();
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

		Sheet sheet = workbook.getSheetAt(0);
		System.out.println("Processing Sheet Name = " + sheet.getSheetName() + " & Total Rows to Process = "
				+ sheet.getLastRowNum());

		for (Row row : sheet) {
			List<String> cells = new ArrayList<>();
			for (Cell cell : row) {
				String text = formatter.formatCellValue(cell);
				if (text.contains(",") || text.contains("\"")) {
					text = "\"" + text.replace("\"", "\"\"") + "\"";
				}
				cells.add(text);
			}
			writer.println(String.join(",", cells));
		}
		writer.flush();
		System.out.println("Successfully Converted Workbook to CSV For Sheet Name = " + sheet.getSheetName());
	}

	private static String pushCSVFileToMinioRemote(Workbook workbook, String reportFileName, String pushedDate,
			Map<String, String> jobContextMap) {

		String bucketName = jobContextMap.get("MINIO_BUCKET_NAME");
		String genaratedFilePath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;
		String minioEndpointUrl = jobContextMap.get("MINIO_ENDPOINT_URL");
		String minioAccessKey = jobContextMap.get("MINIO_ACCESS_KEY");
		String minioSecretKey = jobContextMap.get("MINIO_SECRET_KEY");
		String minioRegion = "us-east-1";

		logger.info("MinIO Credentials: bucketName={}, minioEndpointUrl={}, minioAccessKey={}, minioSecretKey={}",
				bucketName, minioEndpointUrl, minioAccessKey, minioSecretKey);

		try {

			System.out.println("Pushing CSV File to Minio Remote");
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			convertWorkbookToCSV(workbook, bos);
			byte[] byteArray = bos.toByteArray();
			InputStream inputStream = new ByteArrayInputStream(byteArray);

			AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey, minioSecretKey);
			ClientConfiguration clientConfiguration = new ClientConfiguration();
			clientConfiguration.setSignerOverride("AWSS3V4SignerType");
			AmazonS3 s3client = (AmazonS3) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) AmazonS3ClientBuilder
					.standard().withEndpointConfiguration(new EndpointConfiguration(minioEndpointUrl, minioRegion)))
					.withPathStyleAccessEnabled(true)).withClientConfiguration(clientConfiguration))
					.withCredentials(new AWSStaticCredentialsProvider(credentials))).build();

			S3Client client = new S3Client(s3client);

			client.putObject(bucketName, genaratedFilePath, inputStream);
			System.out.println("CSV File Pushed to Minio Remote Successfully, File Path: " + genaratedFilePath);

			return genaratedFilePath;

		} catch (Exception e) {
			System.out.println("Error in Pushing Excel File to Minio, Message: " + e.getMessage() + ", Error: " + e);
			return genaratedFilePath;
		}

	}

	// @SuppressWarnings("resource")
	// private static void convertExceltoCSV(String genaratedFilePath, String
	// outputFile) throws Exception {
	// StringBuffer data = new StringBuffer();
	// FSDataOutputStream fout = null;
	// FileSystem fileSystem = null;
	// FSDataInputStream fsin = null;
	// // fileSystem = HDFSUtil.getFileSystem();
	// logger.info("genaratedFilePath : {}", genaratedFilePath);
	// genaratedFilePath = genaratedFilePath.replaceAll(" ", "-");
	// fileSystem = getFileSystem(genaratedFilePath);
	// fsin = fileSystem.open(new Path(genaratedFilePath));
	// fout = fileSystem.create(new Path(outputFile), true);
	// GZIPOutputStream gzipOuputStream = new GZIPOutputStream(fout);
	// Workbook workbook = new XSSFWorkbook(fsin);
	// try {
	// int numberOfSheets = workbook.getNumberOfSheets();
	// Row row;
	// Cell cell;
	// for (int i = 0; i < numberOfSheets; i++) {
	// Sheet sheet = workbook.getSheetAt(0);
	// Iterator<Row> rowIterator = sheet.iterator();
	// while (rowIterator.hasNext()) {
	// row = rowIterator.next();
	// Iterator<Cell> cellIterator = row.cellIterator();
	// while (cellIterator.hasNext()) {
	// cell = cellIterator.next();
	// switch (cell.getCellType()) {
	// case BOOLEAN:
	// data.append(cell.getBooleanCellValue() + ",");
	// break;
	// case NUMERIC:
	// String cellVal = cell.toString();
	// String Date = "-";
	// String[] splitval = cellVal.split(Symbol.HYPHEN_STRING);
	// if (splitval.length >= 3) {
	// DateFormat formatter = new SimpleDateFormat("dd-MMM-yy");
	// Date = new SimpleDateFormat("dd/MM/yyyy")
	// .format(new Date(formatter.parse(cellVal).getTime()));
	// data.append(Date + ",");
	// } else {
	// data.append(cell.getNumericCellValue() + ",");
	// }
	// break;
	// case STRING:
	// data.append(cell.getStringCellValue() + ",");
	// break;
	// case BLANK:
	// data.append("" + ",");
	// break;
	// default:
	// data.append(cell + ",");

	// }
	// }
	// data.append('\n');
	// }

	// }
	// // logger.info("Headers info in csv : {} ", data.toString());
	// gzipOuputStream.write(data.toString().getBytes());
	// fileSystem.delete(new Path(genaratedFilePath), true);
	// gzipOuputStream.finish();
	// gzipOuputStream.close();

	// } catch (Exception e) {
	// logger.error("error in writing file in Processor CreateWorkbook, error = {}",
	// ExceptionUtils.getStackTrace(e));
	// } finally {
	// if (fout != null) {
	// fout.close();
	// }
	// if (fileSystem != null) {
	// fileSystem.close();
	// }
	// }

	// }

	public static List<String> getQuarterly(List<String> hourList, Date startDate, Date endDate, String duration,
			String timedivision) {
		List<String> finallist = new ArrayList<>();
		List<String> qauterlyList = getPerhourList(hourList, startDate, endDate, FREQUENCY_PERHOUR, duration);

		for (String list : qauterlyList) {
			list = list.contains("00") ? list.replaceAll("00", "") : list;
			Integer minute = 0;
			while (minute < 60) {
				finallist.add(list + (String.valueOf(minute).length() == 1 ? ZERO : "") + String.valueOf(minute));
				minute += Integer.valueOf(timedivision);
			}
		}
		return finallist;

	}

	public static List<String> parseWeekForDateAndFormat(Date startDate, Date endDate, String format) {
		List<String> dateKey = new ArrayList<>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		while (calendar.getTime().before(endDate) || calendar.getTime().equals(endDate)) {
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			DateTime dateTime = new DateTime(calendar.getTime());
			String strDateOnly = dateTime.toString(format);
			dateKey.add(strDateOnly);
			calendar.add(Calendar.WEEK_OF_YEAR, 1);
		}
		return dateKey;
	}

	public static List<String> parseWeekWithDate(Date startDate, String format) {
		List<String> finaldateKey = new ArrayList<>();
		String dateKey = "";
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		DateTime dateTime = new DateTime(calendar.getTime());
		dateKey = "W" + dateTime.toString(format);
		calendar.add(Calendar.WEEK_OF_YEAR, 1);
		finaldateKey.add(dateKey);
		return finaldateKey;
	}

	public static List<String> parseDateForKeyAndFormatMonth(Date startDate, Date endDate, String format) {
		List<String> dateKey = new ArrayList<>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		calendar.set(Calendar.DATE, 01);

		while (calendar.getTime().before(endDate) || calendar.getTime().equals(endDate)) {
			String date = new SimpleDateFormat(format).format(calendar.getTime());
			dateKey.add(date);
			calendar.add(Calendar.MONTH, 1);
		}
		return dateKey;
	}

	private static List<String> getListFromtimeString(String metaColumns, String separator, List<Date> dateList) {
		if (dateList != null && !dateList.isEmpty()) {
			return new ArrayList<>(Arrays.asList("23:00", "22:00", "21:00", "20:00", "19:00", "18:00", "17:00", "16:00",
					"15:00", "14:00", "13:00", "12:00", "11:00", "10:00", "09:00", "08:00", "07:00", "06:00", "05:00",
					"04:00", "03:00", "02:00", "01:00", "00:00"));
		} else {
			if (metaColumns != null) {
				return new ArrayList<>(Arrays.asList(metaColumns.split(separator)));
			}
		}
		return null;
	}

	private static List<Date> getDateListFromConetext(String dateListString) {
		List<Date> list = new ArrayList<>();
		if (dateListString != null) {
			String[] dateListArray = dateListString.split(",");
			for (int i = 0; i < dateListArray.length; i++) {
				Date date = new Date(Long.parseLong(dateListArray[i]));
				list.add(date);
			}
		}
		// logger.info("getDateListFromConetext {}", list);
		return list;
	}

	private static List<String> getSpecificDateListFromContext(String dateListString, String frequency) {
		List<String> list = new ArrayList<>();
		String dateKey = null;
		Calendar calendar = new GregorianCalendar();
		if (dateListString != null) {
			String[] dateListArray = dateListString.split(",");
			for (int i = 0; i < dateListArray.length; i++) {
				Date date = new Date(Long.parseLong(dateListArray[i]));
				calendar.setTime(date);
				if (frequency.equalsIgnoreCase("15 MIN") || frequency.equalsIgnoreCase("PERHOUR")) {
					dateKey = new SimpleDateFormat(DD_MM_YYYY + " " + HH_MM).format(calendar.getTime());
				} else {
					dateKey = new SimpleDateFormat(DD_MM_YYYY).format(calendar.getTime());
				}
				list.add(dateKey);
			}
		}
		// logger.info("getSpecificDateListFromContext {}", list);
		return list;
	}

	public static LocalDateTime setTimeOfDay(Date date, Integer hour, Integer minute, Integer second) {
		LocalDateTime local = new LocalDateTime(date);
		if (hour != null && minute != null && second != null) {
			local = local.withHourOfDay(hour).withMinuteOfHour(minute).withSecondOfMinute(second);
		} else if (hour != null && minute != null) {
			local = local.withHourOfDay(hour).withMinuteOfHour(minute);
		} else if (hour != null && second != null) {
			local = local.withHourOfDay(hour).withSecondOfMinute(second);
		} else if (hour != null) {
			local = local.withHourOfDay(hour);
		} else if (minute != null && second != null) {
			local = local.withMinuteOfHour(minute).withSecondOfMinute(second);
		} else if (minute != null) {
			local = local.withMinuteOfHour(minute);
		} else if (second != null) {
			local = local.withSecondOfMinute(second);
		}
		return local;
	}

	public static List<String> parseDateForQuarterlytimeKey(Date startDate, Date endDate, String format,
			String frequency) {
		startDate = setTimeOfDay(startDate, startDate.getHours(), 0, 0).toDate();
		endDate = setTimeOfDay(endDate, endDate.getHours(), 0, 0).toDate();

		// endDate=DateUtils.addHours(endDate, 1);
		List<String> dateKey = new ArrayList<>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		while (calendar.getTime().before(endDate)) {
			if (format.equalsIgnoreCase(DD_MM_YYYY + " " + HH_MM) && frequency.equalsIgnoreCase("15 MIN")) {
				String date = new SimpleDateFormat(format).format(calendar.getTime());
				dateKey.add(date);
				calendar.add(Calendar.MINUTE, 15);
			} else {
				if (format.equalsIgnoreCase(DD_MM_YYYY + " " + HH_MM) && frequency.equalsIgnoreCase("PERHOUR")) {
					String date = new SimpleDateFormat(format).format(calendar.getTime());
					dateKey.add(date);
					calendar.add(Calendar.HOUR, 1);

				} else {
					String date = new SimpleDateFormat(format).format(calendar.getTime());
					dateKey.add(date);
					calendar.add(Calendar.DATE, 1);
				}
			}

		}
		return dateKey;
	}

	public static List<String> parseDateFortimeKey(Date startDate, Date endDate, String format, String frequency) {
		startDate = setTimeOfDay(startDate, startDate.getHours(), 0, 0).toDate();
		endDate = setTimeOfDay(endDate, endDate.getHours(), 0, 0).toDate();

		// endDate=DateUtils.addHours(endDate, 1);
		List<String> dateKey = new ArrayList<>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		while (calendar.getTime().before(endDate) || calendar.getTime().equals(endDate)) {
			if (format.equalsIgnoreCase(DD_MM_YYYY + " " + HH_MM) && frequency.equalsIgnoreCase("15 MIN")) {
				String date = new SimpleDateFormat(format).format(calendar.getTime());
				dateKey.add(date);
				calendar.add(Calendar.MINUTE, 15);
			} else {
				if (format.equalsIgnoreCase(DD_MM_YYYY + " " + HH_MM) && frequency.equalsIgnoreCase("PERHOUR")) {
					String date = new SimpleDateFormat(format).format(calendar.getTime());
					dateKey.add(date);
					calendar.add(Calendar.HOUR, 1);

				} else {
					String date = new SimpleDateFormat(format).format(calendar.getTime());
					dateKey.add(date);
					calendar.add(Calendar.DATE, 1);
				}
			}

		}
		return dateKey;
	}

	// private void updateReportDetails(HashMap<String, String> parameterMap, String
	// workbookName,
	// String fullFileString, String fileSize) {
	// Connection conn1 = null;
	// Connection conn2 = null;
	// PreparedStatement stmt1 = null;
	// PreparedStatement stmt2 = null;
	// String REPORT_WIDGET_ID_PK = parameterMap.get("REPORT_WIDGET_ID_PK");

	// String updateReportWidget = "UPDATE REPORT_WIDGET SET STATUS = 'CREATED',
	// FILE_PATH ='" + workbookName
	// + "' WHERE REPORT_WIDGET_ID_PK ='"
	// + REPORT_WIDGET_ID_PK + "'";

	// logger.info("REPORT_WIDGET Update Query: {}", updateReportWidget);

	// // String fileSize = parameterMap.get("FILE_SIZE");

	// String prefixToRemove = "s3a://bntvpm/";
	// String fullFilePath = fullFileString.startsWith(prefixToRemove)
	// ? fullFileString.substring(prefixToRemove.length())
	// : fullFileString;

	// String updateGeneratedReport = "UPDATE GENERATED_REPORT SET PROGRESS_STATE =
	// 'GENERATED', FILE_SIZE = "
	// + fileSize + ", FILE_PATH = '" + fullFilePath
	// + "', GENERATED_DATE = NOW() WHERE REPORT_WIDGET_ID = "
	// + REPORT_WIDGET_ID_PK;

	// logger.info("GENERATED_REPORT Update Query: {}", updateGeneratedReport);

	// try {
	// final String JDBC_DRIVER = parameterMap.get("SPARK_PM_JDBC_DRIVER");
	// final String DB_URL = parameterMap.get("SPARK_PM_JDBC_URL");
	// final String USER = parameterMap.get("SPARK_PM_JDBC_USERNAME");
	// final String PASS = parameterMap.get("SPARK_PM_JDBC_PASSWORD");
	// logger.info("JDBC Driver={}, Url={}, Username={}, Password={}", JDBC_DRIVER,
	// DB_URL, USER, PASS);

	// Class.forName(JDBC_DRIVER);
	// conn1 = DriverManager.getConnection(DB_URL, USER, PASS);
	// conn2 = DriverManager.getConnection(DB_URL, USER, PASS);
	// stmt1 = conn1.prepareStatement(updateReportWidget);
	// stmt2 = conn2.prepareStatement(updateGeneratedReport);
	// stmt1.executeUpdate();
	// stmt2.executeUpdate();
	// // logger.error("Successfully update Reportwidget File path :{} and
	// // updateReportWidget
	// // :{}", workbookName,
	// // updateReportWidget);
	// } catch (Exception e) {
	// logger.error("Exception while update Filepath in Reportwidget :{}",
	// e.getMessage());
	// } finally {
	// if (stmt1 != null && stmt2 != null) {
	// try {
	// stmt1.close();
	// stmt2.close();
	// } catch (SQLException e) {
	// logger.error("Error in Closing Statement in parse - {}",
	// Utils.getStackTrace(e));
	// }
	// }
	// if (conn1 != null && conn2 != null) {
	// try {
	// conn1.close();
	// conn2.close();
	// } catch (SQLException e) {
	// logger.error("Error in Closing connection in parse- {}",
	// Utils.getStackTrace(e));
	// }
	// }

	// }
	// }

	private void updateReportDetails(Map<String, String> parameterMap, String workbookName,
			String fullFileString, String fileSize) {

		String reportWidgetIdPk = parameterMap.get("REPORT_WIDGET_ID_PK");
		String jdbcDriver = parameterMap.get("SPARK_PM_JDBC_DRIVER");
		String dbUrl = parameterMap.get("SPARK_PM_JDBC_URL");
		String username = parameterMap.get("SPARK_PM_JDBC_USERNAME");
		String password = parameterMap.get("SPARK_PM_JDBC_PASSWORD");

		// Remove prefix from file path
		final String prefixToRemove = "s3a://bntvpm/";
		String fullFilePath = fullFileString.startsWith(prefixToRemove)
				? fullFileString.substring(prefixToRemove.length())
				: fullFileString;

		String updateReportWidgetSql = "UPDATE REPORT_WIDGET SET STATUS = ?, FILE_PATH = ? WHERE REPORT_WIDGET_ID_PK = ?";
		String updateGeneratedReportSql = "UPDATE GENERATED_REPORT SET PROGRESS_STATE = ?, FILE_SIZE = ?, FILE_PATH = ?, GENERATED_DATE = NOW() WHERE REPORT_WIDGET_ID = ?";

		logger.info("Updating REPORT_WIDGET And GENERATED_REPORT For REPORT_WIDGET_ID_PK={}", reportWidgetIdPk);

		try {
			Class.forName(jdbcDriver);
			try (Connection conn = DriverManager.getConnection(dbUrl, username, password);
					PreparedStatement stmt1 = conn.prepareStatement(updateReportWidgetSql);
					PreparedStatement stmt2 = conn.prepareStatement(updateGeneratedReportSql)) {

				// Prepare and execute REPORT_WIDGET update
				stmt1.setString(1, "CREATED");
				stmt1.setString(2, workbookName);
				stmt1.setString(3, reportWidgetIdPk);
				int rows1 = stmt1.executeUpdate();
				logger.info("===== [ Updated REPORT_WIDGET: {} Row(s) Affected ] =====", rows1);

				// Prepare and execute GENERATED_REPORT update
				stmt2.setString(1, "GENERATED");
				stmt2.setLong(2, Long.parseLong(fileSize));
				stmt2.setString(3, fullFilePath);
				stmt2.setString(4, reportWidgetIdPk);
				int rows2 = stmt2.executeUpdate();
				logger.info("===== [ Updated GENERATED_REPORT: {} Row(s) Affected ] =====", rows2);

			}
		} catch (ClassNotFoundException e) {
			logger.error("JDBC Driver Class Not Found: {}", jdbcDriver, e);
		} catch (SQLException e) {
			logger.error("SQL Error Occurred While Updating Report Details: {}", e.getMessage(), e);
		} catch (Exception e) {
			logger.error("Unexpected Error Occurred In @updateReportDetails: {}", e.getMessage(), e);
		}
	}

	// private static AmazonS3 getS3Client(JobContext jobContext) {
	// try {
	// Map<String, String> parameterMap = jobContext.getParameters();

	// String minioAccessKey = parameterMap.get(SPARK_MINIO_ACCESS_KEY);
	// String minioSecretKey = parameterMap.get(SPARK_MINIO_SECRET_KEY);
	// String minioEndpointUrl = parameterMap.get(SPARK_MINIO_ENDPOINT_URL);
	// String minioRegion = "us-east-1"; // hardcoded region, or get from
	// parameterMap if needed

	// if (minioAccessKey == null || minioSecretKey == null || minioEndpointUrl ==
	// null) {
	// String missingParams = "";
	// if (minioAccessKey == null)
	// missingParams += "minioAccessKey ";
	// if (minioSecretKey == null)
	// missingParams += "minioSecretKey ";
	// if (minioEndpointUrl == null)
	// missingParams += "minioEndpointUrl ";
	// logger.error("MinIO Configuration Missing Parameters: {}",
	// missingParams.trim());
	// throw new IllegalStateException("MinIO Configuration Parameters are Missing:
	// " + missingParams);
	// }

	// AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey,
	// minioSecretKey);

	// ClientConfiguration clientConfiguration = new ClientConfiguration();
	// clientConfiguration.setSignerOverride("AWSS3V4SignerType");
	// clientConfiguration.setConnectionTimeout(30000);
	// clientConfiguration.setSocketTimeout(30000);

	// AmazonS3 s3client = AmazonS3ClientBuilder.standard()
	// .withEndpointConfiguration(new EndpointConfiguration(minioEndpointUrl,
	// minioRegion))
	// .withPathStyleAccessEnabled(true)
	// .withClientConfiguration(clientConfiguration)
	// .withCredentials(new AWSStaticCredentialsProvider(credentials))
	// .build();

	// logger.info("Successfully Created AmazonS3 Client For Endpoint: {}",
	// minioEndpointUrl);
	// return s3client;

	// } catch (Exception e) {
	// logger.error("Error Creating AmazonS3 Client: {}", e.getMessage(), e);
	// throw new RuntimeException("Failed to Create AmazonS3 Client", e);
	// }
	// }

	private static String pushExcelFileToMinioRemote(Workbook workbook, String reportFileName, String pushedDate,
			Map<String, String> jobContextMap) throws Exception {

		String genaratedFilePath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;
		String minioRegion = "us-east-1";
		String bucketName = jobContextMap.get("SPARK_MINIO_BUCKET_NAME_PM");
		String minioEndpointUrl = jobContextMap.get("SPARK_MINIO_ENDPOINT_URL");
		String minioAccessKey = jobContextMap.get("SPARK_MINIO_ACCESS_KEY");
		String minioSecretKey = jobContextMap.get("SPARK_MINIO_SECRET_KEY");

		logger.info("MinIO Credentials: bucketName={}, minioEndpointUrl={}, minioAccessKey={}, minioSecretKey={}",
				bucketName, minioEndpointUrl, minioAccessKey, minioSecretKey);

		try {

			System.out.println("Pushing Excel File to Minio Remote");

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			workbook.write(bos);
			byte[] byteArray = bos.toByteArray();
			InputStream inputStream = new ByteArrayInputStream(byteArray);

			AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey, minioSecretKey);
			ClientConfiguration clientConfiguration = new ClientConfiguration();
			clientConfiguration.setSignerOverride("AWSS3V4SignerType");
			AmazonS3 s3client = (AmazonS3) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) ((AmazonS3ClientBuilder) AmazonS3ClientBuilder
					.standard().withEndpointConfiguration(new EndpointConfiguration(minioEndpointUrl, minioRegion)))
					.withPathStyleAccessEnabled(true)).withClientConfiguration(clientConfiguration))
					.withCredentials(new AWSStaticCredentialsProvider(credentials))).build();

			S3Client client = new S3Client(s3client);

			client.putObject(bucketName, genaratedFilePath, inputStream);
			System.out.println("Excel File Pushed to Minio Remote Successfully, File Path: " + genaratedFilePath);

		} catch (Exception e) {
			System.out.println("Error in Pushing Excel File to Minio, Message: " + e.getMessage() + ", Error: " + e);
			return genaratedFilePath;
		}
		return genaratedFilePath;
	}

	private static String exportExcel(String excelFileName, Workbook workbook,
			Map<String, String> jobContextMap,
			AmazonS3 s3client) throws IOException {
		FSDataOutputStream fout = null;
		FileSystem fileSystem = null;
		long fileSizeBytes = -1;

		try {
			if (excelFileName == null || excelFileName.trim().isEmpty()) {
				logger.error("Provided Excel file name is null or empty. Skipping export.");
				return String.valueOf(fileSizeBytes);
			}

			excelFileName = excelFileName.trim().replaceAll(" ", "-");
			logger.info("Starting Excel export. Target file: {}", excelFileName);

			fileSystem = getFileSystem(excelFileName);
			Path filePath = new Path(excelFileName);

			fout = fileSystem.create(filePath, true);
			workbook.write(fout);
			fout.hflush(); // Ensure data is flushed to file system

			// Extract bucket and key from s3a:// URL
			if (!excelFileName.startsWith("s3a://")) {
				logger.warn("File path does not start with s3a://, skipping S3 metadata retrieval");
				return String.valueOf(fileSizeBytes);
			}

			String pathWithoutScheme = excelFileName.substring("s3a://".length());
			int slashIndex = pathWithoutScheme.indexOf('/');
			if (slashIndex <= 0 || slashIndex == pathWithoutScheme.length() - 1) {
				logger.error("Invalid S3 path format: {}", excelFileName);
				return String.valueOf(fileSizeBytes);
			}

			String bucket = pathWithoutScheme.substring(0, slashIndex);
			String key = pathWithoutScheme.substring(slashIndex + 1);

			boolean sizeRetrieved = false;
			int retries = 5;
			int delay = 2000; // ms

			while (retries-- > 0 && !sizeRetrieved) {
				try {
					ObjectMetadata metadata = s3client.getObjectMetadata(bucket, key);
					fileSizeBytes = metadata.getContentLength();
					sizeRetrieved = true;
				} catch (AmazonS3Exception s3e) {
					if (s3e.getStatusCode() == 404) {
						logger.warn("Object not yet visible in S3 API: {}/{}. Checking via listObjectsV2...", bucket,
								key);
						ListObjectsV2Result listResult = s3client.listObjectsV2(new ListObjectsV2Request()
								.withBucketName(bucket)
								.withPrefix(key)
								.withMaxKeys(1));

						if (!listResult.getObjectSummaries().isEmpty()) {
							fileSizeBytes = listResult.getObjectSummaries().get(0).getSize();
							sizeRetrieved = true;
							break;
						} else {
							logger.warn("Still not found via listObjectsV2. Retrying in {} ms ({} retries left)...",
									delay, retries);
						}
					} else {
						logger.error(
								"AmazonS3Exception retrieving metadata for {}/{}. Retrying in {} ms ({} retries left)...",
								bucket, key, delay, retries, s3e);
					}
					Thread.sleep(delay);
					delay *= 2; // exponential backoff
				} catch (SdkClientException sce) {
					logger.error(
							"SdkClientException retrieving metadata for {}/{}. Retrying in {} ms ({} retries left)...",
							bucket, key, delay, retries, sce);
					Thread.sleep(delay);
					delay *= 2;
				}
			}

			if (sizeRetrieved) {
				logger.info("Excel file successfully written. Size: {} bytes ({} KB)", fileSizeBytes,
						fileSizeBytes / 1024);
				jobContextMap.put("FILE_SIZE", String.valueOf(fileSizeBytes));
			} else {
				logger.error("Failed to retrieve file size after retries for: {}", excelFileName);
			}

		} catch (Exception e) {
			logger.error("An error occurred while exporting Excel file: {}", excelFileName, e);
		} finally {
			try {
				if (fout != null)
					fout.close();
			} catch (IOException e) {
				logger.error("Failed to close FSDataOutputStream for: {}", excelFileName, e);
			}

			try {
				if (fileSystem != null)
					fileSystem.close();
			} catch (IOException e) {
				logger.error("Failed to close FileSystem while exporting Excel file: {}", excelFileName, e);
			}
		}

		return String.valueOf(fileSizeBytes);
	}

	private static String pushCSVFileToMinioRemoteFromExcelPath(String excelFilePath, String reportFileName,
			String pushedDate, Map<String, String> jobContextMap) {

		String bucketName = jobContextMap.get("SPARK_MINIO_BUCKET_NAME_PM");
		if (bucketName == null) {
			bucketName = "bntvpm";
		}
		String minioEndpointUrl = jobContextMap.get("SPARK_MINIO_ENDPOINT_URL");
		String minioAccessKey = jobContextMap.get("SPARK_MINIO_ACCESS_KEY");
		String minioSecretKey = jobContextMap.get("SPARK_MINIO_SECRET_KEY");

		logger.info("MinIO Credentials: bucketName={}, minioEndpointUrl={}, minioAccessKey={}, minioSecretKey={}",
				bucketName, minioEndpointUrl, minioAccessKey, minioSecretKey);

		String minioRegion = "us-east-1";
		String generatedCsvFilePath = "protected/PM/FlowReport/" + pushedDate + "/" + reportFileName;

		try {
			logger.info("Fetching Excel file from MinIO bucket: {}, key: {}", bucketName, excelFilePath);

			// Setup MinIO S3 client
			AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey, minioSecretKey);
			ClientConfiguration clientConfiguration = new ClientConfiguration();
			clientConfiguration.setSignerOverride("AWSS3V4SignerType");

			AmazonS3 s3client = AmazonS3ClientBuilder
					.standard()
					.withEndpointConfiguration(new EndpointConfiguration(minioEndpointUrl, minioRegion))
					.withPathStyleAccessEnabled(true)
					.withClientConfiguration(clientConfiguration)
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.build();

			S3Client s3 = new S3Client(s3client);

			InputStream excelInputStream = s3.getObject(bucketName, excelFilePath);
			logger.info("Excel file fetched successfully.");

			// Read Excel workbook
			Workbook workbook = new XSSFWorkbook(excelInputStream);
			logger.info("Workbook loaded. Converting to CSV.");

			// Convert to CSV
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			convertWorkbookToCSV(workbook, bos);
			logger.info("Excel to CSV conversion completed.");

			// Prepare CSV stream for upload
			InputStream csvInputStream = new ByteArrayInputStream(bos.toByteArray());
			logger.info("Uploading CSV file to MinIO path: {}", generatedCsvFilePath);
			s3.putObject(bucketName, generatedCsvFilePath, csvInputStream);

			logger.info("CSV File pushed to MinIO successfully. Path: {}", generatedCsvFilePath);
			return generatedCsvFilePath;

		} catch (Exception e) {
			logger.error("Error in Excel-to-CSV upload: {}", e.getMessage(), e);
			return generatedCsvFilePath;
		}
	}

	// private static void exportExcel(String excelFileName, Workbook workbook,
	// Map<String, String> jobContextMap)
	// throws IOException {
	// FSDataOutputStream fout = null;
	// FileSystem fileSystem = null;

	// try {
	// if (excelFileName == null || excelFileName.trim().isEmpty()) {
	// logger.error("Provided Excel file name is null or empty. Skipping export.");
	// return;
	// }

	// excelFileName = excelFileName.replaceAll(" ", "-");
	// logger.info("Starting Excel export. Target file: {}", excelFileName);

	// fileSystem = getFileSystem(excelFileName);
	// Path filePath = new Path(excelFileName);

	// fout = fileSystem.create(filePath, true);
	// workbook.write(fout);
	// fout.hflush(); // Ensure all data is flushed before reading file status

	// // Retry logic for S3A consistency
	// int retries = 5;
	// long fileSizeBytes = -1;
	// boolean sizeRetrieved = false;

	// while (retries-- > 0) {
	// try {
	// // fileSizeBytes = fileSystem.getFileStatus(filePath).getLen();
	// FileStatus[] statuses = fileSystem.listStatus(filePath.getParent());
	// for (FileStatus status : statuses) {
	// if (status.getPath().toString().equals(filePath.toString())) {
	// fileSizeBytes = status.getLen();
	// sizeRetrieved = true;
	// break;
	// }
	// }
	// } catch (FileNotFoundException fnfe) {
	// logger.error(
	// "Excel file not yet visible in file system. Retrying in 2 seconds... ({}
	// retries left)",
	// retries);
	// Thread.sleep(2000); // Wait before retry
	// } catch (IOException ioe) {
	// logger.error("IOException while retrieving file size for: {}", excelFileName,
	// ioe);
	// break;
	// }
	// }

	// if (sizeRetrieved) {
	// logger.info("Successfully wrote Excel file. Size: {} bytes ({} KB)",
	// fileSizeBytes,
	// fileSizeBytes / 1024);
	// jobContextMap.put("FILE_SIZE", String.valueOf(fileSizeBytes));
	// } else {
	// logger.error("Unable to retrieve file size after retries for: {}",
	// excelFileName);
	// }

	// } catch (Exception e) {
	// logger.error("An error occurred while exporting Excel file: {}",
	// excelFileName, e);
	// } finally {
	// try {
	// if (fout != null) {
	// fout.close();
	// }
	// } catch (IOException e) {
	// logger.error("Failed to close FSDataOutputStream for: {}", excelFileName, e);
	// }

	// try {
	// if (fileSystem != null) {
	// fileSystem.close();
	// }
	// } catch (IOException e) {
	// logger.error("Failed to close FileSystem while exporting Excel file: {}",
	// excelFileName, e);
	// }
	// }
	// }

	// private static void exportExcel(String excelFileName, Workbook workbook,
	// Map<String, String> jobContextMap)
	// throws IOException {
	// FSDataOutputStream fout = null;
	// FileSystem fileSystem = null;
	// try {
	// // fileSystem = HDFSUtil.getFileSystem();
	// logger.info("excelFileName : {}", excelFileName);
	// excelFileName = excelFileName.replaceAll(" ", "-");
	// fileSystem = getFileSystem(excelFileName);

	// fout = fileSystem.create(new Path(excelFileName), true);
	// workbook.write(fout);
	// fout.hflush(); // Ensure all data is flushed before checking size

	// Path filePath = new Path(excelFileName);
	// long fileSizeBytes = fileSystem.getFileStatus(filePath).getLen();
	// logger.info("Excel file size: {} bytes ({} KB)", fileSizeBytes, fileSizeBytes
	// / 1024);

	// jobContextMap.put("FILE_SIZE", String.valueOf(fileSizeBytes));

	// } catch (Exception e) {
	// logger.error("error in writing file in Processor CreateWorkbook, error = {}",
	// ExceptionUtils.getStackTrace(e));
	// } finally {
	// if (fout != null) {
	// fout.close();
	// }
	// if (fileSystem != null) {
	// fileSystem.close();
	// }
	// }

	// }

	private static String getSheetNameFromCsvFilePath(String csvFilePath) {
		return StringUtils.substringAfterLast(StringUtils.substringBeforeLast(csvFilePath, Symbol.SLASH_FORWARD_STRING),
				Symbol.SLASH_FORWARD_STRING);
	}

	/**
	 * Create WorkBook
	 *
	 * @param jobContext
	 * @return
	 */
	private Workbook createWorkBook(JobContext jobContext, String workbookName) {
		Map<String, SXSSFWorkbook> workbookMap = jobContext.mapOfWBAndWBName;
		SXSSFWorkbook workbook = getWorkbook(workbookMap, jobContext, workbookName);
		return workbook;
	}

	/**
	 * Create New WorkBook if Not Available
	 *
	 * @param workbookMap
	 * @param jobContext
	 * @return
	 */
	private SXSSFWorkbook getWorkbook(Map<String, SXSSFWorkbook> workbookMap, JobContext jobContext,
			String workbookName) {
		SXSSFWorkbook workbook = null;
		try {
			if (workbookMap.isEmpty()) {
				workbookMap = new HashMap<String, SXSSFWorkbook>();
				workbook = getNewWorkbook(workbook, workbookMap, jobContext, workbookName);
			} else {
				boolean isAvailable = false;
				if (workbookMap.containsKey(workbookName)) {
					workbook = workbookMap.get(workbookName);
					isAvailable = true;
				}

				if (!isAvailable) {
					workbook = getNewWorkbook(workbook, workbookMap, jobContext, workbookName);
				}
			}
		} catch (Exception e) {
			logger.error("[{}] Error while creating workbook : {}", this.processorName,
					ExceptionUtils.getStackTrace(e));
		}
		return workbook;
	}

	/**
	 * Create New WorkBook
	 *
	 * @param workbook
	 * @param workbookList
	 * @param jobContext
	 * @return
	 */
	public SXSSFWorkbook getNewWorkbook(SXSSFWorkbook workbook, Map<String, SXSSFWorkbook> workbookList,
			JobContext jobContext, String workbookName) {

		if (com.enttribe.sparkrunner.util.Utils.hasValidValue(workbookName)) {
			workbook = new SXSSFWorkbook(100);
			workbookList.put(workbookName, workbook);
			jobContext.mapOfWBAndWBName = workbookList;
			return workbook;
		} else {
			throw new NullArgumentException(
					"Exception Inside processor CreateExcelSheet workbookName : " + workbookName);
		}
	}

	private Workbook createKPIExcelWorkBook(Workbook wb, String sheetName, String hdfsFilePath, String kpiJson,
			String metaColumns, String node, String domain, String moColumns, String extension,
			Map<String, String> jobContextMap) {
		logger.info(
				"Starting createKPIExcelWorkBook with sheetName={}, hdfsFilePath={}, node={}, domain={}, extension={}",
				sheetName, hdfsFilePath, node, domain, extension);

		Integer rowIndex = INDEX_ZERO;
		Integer cellHieght = INDEX_EIGHTEEN;
		Integer nextCellIndex = INDEX_ZERO;
		Integer sheetIndex = INDEX_ZERO;
		List<String> kpiIdList = new ArrayList<>();

		logger.info("Creating new sheet: {}", sheetName);
		Sheet sheet = wb.createSheet(sheetName);

		if (StringUtils.isNotEmpty(moColumns)) {
			logger.info("Adding moColumns to metaColumns: {}", moColumns);
			metaColumns = metaColumns + Symbol.COMMA_STRING + moColumns;
		}

		logger.info("Getting headers for PM KPI report");
		rowIndex = getHeadersForPMKPIReport(sheet, wb, rowIndex, kpiJson, metaColumns, kpiIdList, extension,
				jobContextMap);

		logger.info("Creating helper and cell styles");
		CreationHelper createHelper = wb.getCreationHelper();
		logger.info("KPI ID List: {}", kpiIdList);

		CellStyle valueCellStyle = getCellStyle(wb, null, null, null, true, true, true, true);
		CellStyle valueCellStyleDateTime = getCellStyle(wb, null, null, null, true, true, true, true);

		logger.info("Processing data columns from metaColumns");
		List<String> dataColumns = getListFromString(StringUtils.substringBefore(metaColumns, Symbol.HASH_STRING),
				Symbol.COMMA_STRING);
		dataColumns = dataColumns.stream().filter(s -> !s.equalsIgnoreCase("")).collect(Collectors.toList());
		logger.info("Filtered data columns: {}", dataColumns);

		Integer noOfRows = PM_SHEET_NUMBER_OF_ROWS;
		logger.info("Sheet configuration - dataColumns={}, noOfRows={}", dataColumns, noOfRows);

		Map<String, String> neDuplexMap = new LinkedHashMap<>();
		FileSystem fileSystem = null;
		FSDataInputStream fsin = null;
		BufferedReader br = null;

		try {
			logger.info("Opening HDFS file: {}", hdfsFilePath);
			hdfsFilePath = hdfsFilePath.replaceAll(" ", "-");
			// fileSystem = HDFSUtil.getFileSystem();
			fileSystem = getFileSystem(hdfsFilePath);
			fsin = fileSystem.open(new Path(hdfsFilePath));
			GZIPInputStream gzip = new GZIPInputStream(fsin);
			br = new BufferedReader(new InputStreamReader(gzip));

			String sCurrentLine = br.readLine();
			Map<String, Integer> headerIndexMap = getCounterIndexMap(sCurrentLine);
			logger.info("Header index map: {}", headerIndexMap);

			int lineCount = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				lineCount++;
				if (lineCount % 1000 == 0) {
					logger.info("Processing line {}", lineCount);
				}

				String[] elements = sCurrentLine.split(Symbol.COMMA_STRING);
				Map<String, String> resultMap = getResultMapFromCurrentRow(headerIndexMap, elements);
				nextCellIndex = 0;

				Row row = getNewRow(sheet, rowIndex++, cellHieght);

				logger.debug("Writing data columns for row {}", rowIndex);
				for (String header : dataColumns) {
					nextCellIndex = writeWrapperValueInFileSMF(node, nextCellIndex, sheet, valueCellStyle, row,
							resultMap, header, neDuplexMap, createHelper, valueCellStyleDateTime, domain);
				}

				if (StringUtils.isNotEmpty(moColumns)) {
					logger.debug("Processing MO columns for row {}", rowIndex);
					Map<String, String> moValueMapFromMOHeirachy = getMOValueMapFromMOHeirachy(resultMap, "moHeirachy");
					for (String mocolumn : moColumns.split(Symbol.COMMA_STRING)) {
						writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
								getValueFromResult(moValueMapFromMOHeirachy, mocolumn));
					}
				}

				logger.debug("Writing KPI values for row {}", rowIndex);
				for (String kpi : kpiIdList) {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
							getValueAsDoubleFromResult(resultMap, kpi));
				}

				if (sheet.getPhysicalNumberOfRows() > noOfRows) {
					++sheetIndex;
					nextCellIndex = 0;
					rowIndex = 0;
					String newSheetName = sheetName + Symbol.UNDERSCORE_STRING + sheetIndex;
					logger.info("Creating new sheet: {} due to row limit exceeded", newSheetName);
					sheet = wb.createSheet(newSheetName);
					rowIndex = getHeadersForPMKPIReport(sheet, wb, rowIndex, kpiJson, metaColumns, kpiIdList,
							extension, new HashMap<>());
				}
			}
			logger.info("Finished processing {} lines", lineCount);

		} catch (Exception e) {
			logger.error("Exception in writing excel sheet: {}", ExceptionUtils.getStackTrace(e));
		} finally {
			logger.info("Closing resources");
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("Error closing BufferedReader: {}", e.getMessage());
				}
			}
			if (fsin != null) {
				try {
					fsin.close();
				} catch (IOException e) {
					logger.error("Error closing FSDataInputStream: {}", e.getMessage());
				}
			}
			if (fileSystem != null) {
				try {
					fileSystem.close();
				} catch (IOException e) {
					logger.error("Error closing FileSystem: {}", e.getMessage());
				}
			}
		}
		logger.info("Completed createKPIExcelWorkBook");
		return wb;
	}

	private Workbook createExcelWorkBook(Workbook wb, String sheetName, String hdfsFilePath, String kpiJson,
			String metaColumns, String node, String domain, String moColumns, String timeString, String threshold) {
		Integer rowIndex = INDEX_ZERO;
		Integer cellHieght = INDEX_EIGHTEEN;
		Integer nextCellIndex = INDEX_ZERO;
		Integer sheetIndex = INDEX_ZERO;
		List<String> kpiIdList = new ArrayList<>();
		Sheet sheet = wb.createSheet(sheetName);

		rowIndex = getHeadersForPMReport(sheet, wb, rowIndex, kpiJson, metaColumns, kpiIdList, timeString, threshold);
		Map<String, String> kpicodeMap = getKpicodeGoupWiseMap(kpiJson);// <1065,kpiname>
		logger.info("kpiIdList  : {} and kpiGroupMap : {} ", kpiIdList, kpicodeMap);
		CreationHelper createHelper = wb.getCreationHelper();
		CellStyle valueCellStyle = getCellStyle(wb, null, null, null, true, true, true, true);
		CellStyle valueCellStyleDateTime = getCellStyle(wb, null, null, null, true, true, true, true);
		Font greenfont = getFontForExcel(wb, true, FONT_SIZE_VALUE_NORMAL);
		Font redfont = getFontForExcel(wb, true, FONT_SIZE_VALUE_NORMAL);
		XSSFCellStyle redkpiCellStyle = getCellStyle(wb, Color.BLACK, null, BorderStyle.THIN, null, true, true, true,
				true, HorizontalAlignment.CENTER);
		XSSFCellStyle greenkpiCellStyle = getCellStyle(wb, Color.BLACK, null, BorderStyle.THIN, null, true, true, true,
				true, HorizontalAlignment.CENTER);

		redfont.setColor(HSSFColorPredefined.RED.getIndex());
		logger.info("HSSFColorPredefined.RED.getIndex() : {} and index2 : {} ", HSSFColorPredefined.RED.getIndex(),
				HSSFColorPredefined.RED.getIndex2());
		greenfont.setColor(HSSFColorPredefined.GREEN.getIndex());

		redkpiCellStyle.setFont(redfont);
		greenkpiCellStyle.setFont(greenfont);

		List<String> dataColumns = getListFromString(StringUtils.substringBefore(metaColumns, Symbol.HASH_STRING),
				Symbol.COMMA_STRING);

		Integer noOfRows = PM_SHEET_NUMBER_OF_ROWS;

		dataColumns = dataColumns.stream().filter(s -> !s.equalsIgnoreCase("")).collect(Collectors.toList());

		logger.info("noOfRows for new sheet file {}  and dataColumns : {}  and  checking list is empty or not : {} ",
				noOfRows, dataColumns, dataColumns.isEmpty());

		Map<String, String> neDuplexMap = new LinkedHashMap<>();
		FileSystem fileSystem = null;
		FSDataInputStream fsin = null;
		BufferedReader br = null;
		try {
			// fileSystem = HDFSUtil.getFileSystem();
			fileSystem = getFileSystem(hdfsFilePath);
			fsin = fileSystem.open(new Path(hdfsFilePath));
			logger.info("hdfsFilePath : {} ", hdfsFilePath);
			GZIPInputStream gzip = new GZIPInputStream(fsin);
			br = new BufferedReader(new InputStreamReader(gzip));
			String sCurrentLine = br.readLine();
			LinkedHashMap<String, Integer> headerIndexMap = getCounterIndexMap(sCurrentLine);// (D,0),(V,1),(NODE,3),(ENB,4),(L1,5)
			// ,(L2,6),(L3,7),(L4,8),(1065#0,9),(1073#1,10),(9123#2,11)
			logger.info("headerIndexMap : {} ", headerIndexMap);
			while ((sCurrentLine = br.readLine()) != null) {
				String[] elements = sCurrentLine.split(Symbol.COMMA_STRING);
				Map<String, Map<String, String>> resultMap = getResultMapFromCurrentRow(headerIndexMap, elements,
						metaColumns);// (D,RAN) (kpiid,(dt,val))
				Integer count = 0;
				for (Entry<String, Map<String, String>> kpiResultMap : resultMap.entrySet()) {
					nextCellIndex = 0;
					Row row = getNewRow(sheet, rowIndex++, cellHieght);
					if (!dataColumns.isEmpty() && !dataColumns.get(0).equalsIgnoreCase(Symbol.EMPTY_STRING)) {
						for (String header : dataColumns) {
							nextCellIndex = writeWrapperValueInFileSMF(node, nextCellIndex, sheet, valueCellStyle, row,
									kpiResultMap.getValue(), header, neDuplexMap, createHelper, valueCellStyleDateTime,
									domain);
						}
					}
					if (StringUtils.isNotEmpty(moColumns)) {
						Map<String, String> moValueMapFromMOHeirachy = getMOValueMapFromMOHeirachy(
								kpiResultMap.getValue(), "moHeirachy");
						for (String mocolumn : moColumns.split(Symbol.COMMA_STRING)) {
							writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
									getValueFromResult(moValueMapFromMOHeirachy, mocolumn));
						}
					}
					String threshold_conditions = StringUtils.substringAfter(kpicodeMap.get(kpiResultMap.getKey()),
							"##");
					String kpiname = StringUtils.substringBefore(kpicodeMap.get(kpiResultMap.getKey()), "##");
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++, kpiname);
					count = 0;
					for (String kpi : kpiIdList) {
						count = writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
								getValueAsDoubleFromResult(kpiResultMap.getValue(), kpi), count, greenkpiCellStyle,
								redkpiCellStyle, threshold_conditions);// kpi --> date, resultMap.get(key)
					}
					if (threshold.equalsIgnoreCase("false")) {
						writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++, count);
					}
					// for end
					if (sheet.getPhysicalNumberOfRows() > noOfRows) {
						++sheetIndex;
						nextCellIndex = 0;
						rowIndex = 0;
						sheet = wb.createSheet(sheetName + Symbol.UNDERSCORE_STRING + sheetIndex);
						rowIndex = getHeadersForPMReport(sheet, wb, rowIndex, kpiJson, metaColumns, kpiIdList,
								timeString, threshold);
						logger.info("adding new sheet : " + sheet.getSheetName());
					}
				}

			}
		} catch (Exception e) {
			logger.error("Exception in writing excel sheet :{}", ExceptionUtils.getStackTrace(e));
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (fsin != null) {
				try {
					fsin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (fileSystem != null) {
				try {
					fileSystem.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return wb;
	}

	private Map<String, String> getResultMapFromCurrentRow(Map<String, Integer> headerIndexMap, String[] elements) {
		Map<String, String> returnMap = new HashMap<>();
		for (Entry<String, Integer> headerIndex : headerIndexMap.entrySet()) {
			String kpiId = headerIndex.getKey();// StringUtils.substringBefore(headerIndex.getKey(),
												// Symbol.HASH_STRING);
			String kpiValue = elements[headerIndex.getValue()];
			returnMap.put(kpiId, kpiValue);
		}
		return returnMap;
	}

	private Map<String, Map<String, String>> getResultMapFromCurrentRow(Map<String, Integer> headerIndexMap,
			String[] elements, String metaColumns) {
		Map<String, String> returnMap = new LinkedHashMap<>();
		Map<String, Map<String, String>> kpiResultMap = new LinkedHashMap<>();
		String kpikey = "";
		for (Entry<String, Integer> headerIndex : headerIndexMap.entrySet()) {
			if (!metaColumns.contains(headerIndex.getKey()) && !headerIndex.getKey().contains("moHeirachy")) {
				String kpiId = StringUtils.substringBefore(headerIndex.getKey(), Symbol.HASH_STRING);
				if ((headerIndex.getKey()).contains("#c")) {
					kpikey = kpiId + "c";
				} else {
					kpikey = kpiId;
				}
				String value = elements[headerIndex.getValue()];
				String[] kpiDateValue = value.split("##");
				Map<String, String> returnMapKPI = new HashMap<>();
				for (String kpi : kpiDateValue) {
					String kpiDate = StringUtils.substringAfter(kpi, "@@");
					String kpiValue = StringUtils.substringBefore(kpi, "@@");
					returnMapKPI.put(kpiDate, kpiValue);
				}

				returnMapKPI.putAll(returnMap);
				kpiResultMap.put(kpikey, returnMapKPI);
			} else {
				String kpiId = StringUtils.substringBefore(headerIndex.getKey(), Symbol.HASH_STRING);
				String kpiValue = elements[headerIndex.getValue()];
				returnMap.put(kpiId, kpiValue);
			}
		}
		return kpiResultMap;
	}

	private LinkedHashMap<String, Integer> getCounterIndexMap(String sCurrentLine) {
		LinkedHashMap<String, Integer> counterIndexMap = new LinkedHashMap<String, Integer>();
		String[] splitValue = sCurrentLine.split(Symbol.COMMA_STRING);
		Integer index = 0;
		for (String value : splitValue) {
			counterIndexMap.put(value, index);
			index++;
		}
		return counterIndexMap;
	}

	private Integer writeWrapperValueInFileSMF(String node, Integer nextCellIndex, Sheet sheet,
			CellStyle valueCellStyle, Row row, Map<String, String> resultMap, String header,
			Map<String, String> neDuplexMap, CreationHelper createHelper, CellStyle valueCellStyleDateTime,
			String domain) throws ParseException {
		if (header.equals(DATE)) {
			SimpleDateFormat sdf = new SimpleDateFormat(DD_MM_YYYY);
			String dateString = getValueFromResult(resultMap, header);
			if (dateString != null) {
				try {
					Date date = sdf.parse(dateString);
					writeDateInCell(valueCellStyleDateTime, nextCellIndex++, createHelper, row, date, DD_MM_YYYY);
				} catch (Exception e) {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++, Symbol.HYPHEN_STRING);
				}

			} else {
				writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++, Symbol.HYPHEN_STRING);
			}
		} else if (!header.contains(BBH) && !header.contains(NBH) && !header.contains(RK)) {
			if (node.equalsIgnoreCase(ALL_ENODEB_INDIVIDUAL)) {
				if (header.equals(CEL)) {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++, Symbol.HYPHEN_STRING);
				} else if (header.equals(DUPLEX)) {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
							neDuplexMap.get(getValueFromResult(resultMap, ENB)));

				} else {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
							getValueFromResult(resultMap, header));
				}
			} else {
				if (header.equals(DUPLEX)) {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
							getDuplexByNeFrequency(getValueFromResult(resultMap, BND)));

				} else if (header.equals(CEL)) {
					if (domain != null && domain.equalsIgnoreCase(RAN_DOMAIN)) {
						Integer cellId = getCellIdAsInteger(resultMap, header);
						writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++, cellId);
					} else {
						writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
								getValueFromResult(resultMap, header));
					}

				} else {
					writeValueOnCell(row, valueCellStyle, false, sheet, nextCellIndex++,
							getValueFromResult(resultMap, header));
				}
			}
		}
		return nextCellIndex;
	}

	private Integer getCellIdAsInteger(Map<String, String> resultMap, String header) {
		try {
			String cellString = getValueFromResult(resultMap, header);
			return cellString != null && !cellString.equalsIgnoreCase(Symbol.HYPHEN_STRING)
					? Integer.parseInt(cellString)
					: null;
		} catch (Exception e) {
			return null;
		}
	}

	private String getValueFromResult(Map<String, String> resultMap, String header) {
		String value = Symbol.HYPHEN_STRING;
		if (resultMap != null && resultMap.get(header) != null) {
			value = resultMap.get(header);
		}
		return value;
	}

	private Double getValueAsDoubleFromResult(Map<String, String> resultMap, String header) {
		Double value = null;
		if (resultMap != null && resultMap.get(header) != null) {
			String kpiValueString = resultMap.get(header);
			if (kpiValueString != null) {
				try {
					value = Double.valueOf(kpiValueString);
				} catch (NumberFormatException nfe) {
				}
			}
		}
		return value;
	}

	private Map<String, String> getMOValueMapFromMOHeirachy(Map<String, String> resultMap, String header) {
		Map<String, String> moMap = new HashMap<String, String>();
		if (resultMap != null && resultMap.get(header) != null) {
			String moHierachy = resultMap.get(header);
			if (StringUtils.isNotEmpty(moHierachy)) {
				for (String moColumn : moHierachy.split("@")) {
					String moHeader = StringUtils.substringBefore(moColumn, "=$");
					String moValue = StringUtils.substringAfter(moColumn, "=$");
					moValue = (moValue.equalsIgnoreCase("null") || moValue == null) ? Symbol.HYPHEN_STRING : moValue;
					moMap.put(moHeader, moValue);
				}
			}
		}
		return moMap;
	}

	/**
	 * Checks if is numeric.
	 *
	 * @param str the str
	 * @return true, if is numeric
	 */
	public static boolean isNumeric(String str) {
		try {
			Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	public String getDuplexByNeFrequency(String neFrequency) {
		String duplex = neFrequency;
		if (BAND2300.equalsIgnoreCase(neFrequency)) {
			duplex = DUPLEX_TDD;
		} else if (BAND850.equalsIgnoreCase(neFrequency)) {
			duplex = DUPLEX_FDD;
		}
		return duplex;
	}

	private static void writeDateInCell(CellStyle cellStyle, Integer nextCellIndex, CreationHelper createHelper,
			Row row, Date date, String format) {
		short dateFormat = createHelper.createDataFormat().getFormat(format);
		cellStyle.setDataFormat(dateFormat);
		Cell cell = row.createCell(nextCellIndex);
		cell.setCellValue(date);
		cell.setCellStyle(cellStyle);

	}

	public static List<String> append24HourInCRCHours(List<String> hourList, Date startDate, Date endDate,
			String format) {
		List<String> dateKey = new ArrayList<>();
		String strtdateInString = new SimpleDateFormat(format).format(startDate);
		String enddateInString = new SimpleDateFormat(format).format(endDate);
		// logger.info("startDate {} and endDate {}", startDate, endDate);
		Integer endDatehour = Integer.parseInt(enddateInString.split(BLANK)[1].split(":")[0]);
		strtdateInString = strtdateInString.split(BLANK)[0];
		int strtHour = setTimeOfDay(startDate, null, null, null).getHourOfDay();
		logger.info("strtdateInString {} , endDatehour {} , strtHour {} , enddateInString {} ", strtdateInString,
				endDatehour, strtHour, enddateInString);
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startDate);
		int i = 0;
		startDate = setTimeOfDay(startDate, 0, 0, 0).toDate();
		calendar.set(Calendar.MILLISECOND, 000000);
		endDate = setTimeOfDay(endDate, 0, 0, 0).toDate();
		Calendar calendar1 = new GregorianCalendar();
		calendar1.setTime(endDate);
		calendar1.set(Calendar.MILLISECOND, 000000);
		logger.info("CALENDER :: {} endDate  :: {} startDate :: {} isutr : {}", calendar.getTime(), endDate, startDate,
				calendar.getTime().before(endDate));
		while (calendar.getTime().before(calendar1.getTime()) || calendar.getTime().equals(calendar1.getTime())) {
			for (int hour = hourList.size() - 1; hour >= 0; hour--) {
				int hr = Integer.parseInt(hourList.get(hour).split(":")[0]);
				if ((i == 0 && (calendar.get(Calendar.HOUR_OF_DAY)) <= (hr)) || i > 0) {
					strtdateInString = new SimpleDateFormat(format).format(calendar.getTime());
					strtdateInString = strtdateInString.split(BLANK)[0];
					if (hr < 10) {
						dateKey.add(strtdateInString + " " + ZERO + hr + ":00");
					} else {
						dateKey.add(strtdateInString + " " + hr + ":00");
					}

				}
			}
			calendar.add(Calendar.DATE, 1);
			i++;
		}
		if (DateUtils.isSameDay(startDate, endDate)) {
			Integer hour = setTimeOfDay(startDate, strtHour, null, null).getHourOfDay();
			getHourList(format, calendar.getTime(), hour, endDatehour, hourList, true, dateKey);
		} else if (DateUtils.isSameDay(calendar.getTime(), endDate)) {
			getHourList(format, calendar.getTime(), 0, endDatehour, hourList, true, dateKey);
		}
		System.out.println("dateKey : " + dateKey);
		return dateKey;
	}

	public static List<String> getHourList(String format, Date strDate, int startHour, int endDatehour,
			List<String> hourList, boolean isLast, List<String> dateKey) {
		logger.info("startHour :{}  endDatehour : {} and strDate {}", startHour, endDatehour, strDate);
		for (int hour = startHour; hour <= endDatehour; hour++) {
			String hourString = null;
			if (hour < 10) {
				hourString = ZERO + hour + ":00";
			} else {
				hourString = hour + ":00";
			}
			if (hourList.contains(hourString)) {
				String strtdateInString = new SimpleDateFormat(format).format(strDate);
				strtdateInString = strtdateInString.split(BLANK)[0];
				if (hour < 10) {
					dateKey.add(strtdateInString + " " + ZERO + hour + ":00");

				} else {
					dateKey.add(strtdateInString + " " + hour + ":00");
				}
			}
		}
		return dateKey;
	}

	public static List<String> parseDateHourForKeyAndFormat(Date startDate, Date endDate, String format) {
		List<String> dateKey = new ArrayList<>();
		if (DateUtils.isSameDay(startDate, endDate)) {
			append24HourInCRC(dateKey, startDate, format);
		} else {
			Calendar calendar = new GregorianCalendar();
			calendar.setTime(startDate);
			while (calendar.getTime().before(endDate)) {
				append24HourInCRC(dateKey, calendar.getTime(), format);
				calendar.add(Calendar.DATE, 1);
			}
		}
		return dateKey;
	}

	private static List<String> append24HourInCRC(List<String> dateKey, Date date, String format) {
		String dateInString = new SimpleDateFormat(format).format(date);
		for (int hour = 0; hour < 24; hour++) {
			if (hour < 10) {
				dateKey.add(dateInString + ZERO + hour);

			} else {
				dateKey.add(dateInString + hour);
			}
		}
		return dateKey;
	}

	private static List<String> getPerhourList(List<String> hourList, Date startDate, Date endDate, String frequency,
			String duration) {
		// endDate = DateUtils.addDays(endDate, -1);
		logger.info("hourList: {}, startDate: {}, endDate: {},", hourList, startDate, endDate);
		if (frequency.equalsIgnoreCase(FREQUENCY_PERHOUR)) {
			List<String> parseDateForKeyAndFormat = null;
			try {
				if ((duration.equalsIgnoreCase(DURATION_DAILY))) {
					endDate = setTimeOfDay(endDate, 23, 0, 0).toDate();
				} else {
					if ((duration.equalsIgnoreCase(DURATION_HOURLY))) {
						// endDate = DateUtils.addDays(endDate, 1);
						DateTime dt = new DateTime(endDate);
						int endHour = dt.getHourOfDay();
						endDate = setTimeOfDay(endDate, endHour, 0, 0).toDate();
						System.out.println("endDate in duration" + endDate);
					}
				}
				if (hourList != null && !hourList.isEmpty()) {

					parseDateForKeyAndFormat = append24HourInCRCHours(hourList, startDate, endDate,
							DD_MM_YYYY + "  " + HH_MM);
				} else {
					parseDateForKeyAndFormat = parseDateHourForKeyAndFormatHours(startDate, endDate,
							DD_MM_YYYY + "  " + HH_MM);
				}
			} catch (ParseException e) {
				logger.error("Error in getting .parseDateHourForKeyAndFormatHours {}", ExceptionUtils.getStackTrace(e));
			}
			parseDateForKeyAndFormat = parseDateForKeyAndFormat.stream().sorted().collect(Collectors.toList());
			logger.info("parseDateForKeyAndFormat hourly frequency: {} ", parseDateForKeyAndFormat);
			return parseDateForKeyAndFormat;
		}
		List<String> parseDateForKeyAndFormat = parseDateHourForKeyAndFormat(startDate, endDate, YYYYMMDD);
		parseDateForKeyAndFormat = parseDateForKeyAndFormat.stream().sorted().collect(Collectors.toList());
		logger.info("parseDateForKeyAndFormat : {} ", parseDateForKeyAndFormat);
		return parseDateForKeyAndFormat;
	}

	public static List<String> parseDateHourForKeyAndFormatHours(Date startDate, Date endDate, String format)
			throws ParseException {
		List<String> dateKey = new ArrayList<>();
		logger.info("startDate : {} endDate : {}", startDate, endDate);
		String strtdateInString = new SimpleDateFormat(format).format(startDate);
		String enddateInString = new SimpleDateFormat(format).format(endDate);
		Integer starthour = Integer.parseInt(strtdateInString.split(BLANK)[1].split(":")[0]);
		Integer endhour = Integer.parseInt(enddateInString.split(BLANK)[1].split(":")[0]);
		if (DateUtils.isSameDay(startDate, endDate)) {
			logger.info("SAME Starthour : {} and  endhour : {}", starthour, endhour);
			append24HourInCRCHours(dateKey, startDate, starthour, endhour, format);
		} else {
			Calendar calendar = new GregorianCalendar();
			calendar.setTime(startDate);
			int dateCount = 0;
			while (calendar.getTime().before(endDate) || DateUtils.isSameDay(calendar.getTime(), endDate)) {
				logger.info("FOR DATE {} end date : {}", calendar.getTime(), endDate);
				String enddate = new SimpleDateFormat(format).format(endDate).split(BLANK)[0];
				String currentDate = new SimpleDateFormat(format).format(calendar.getTime()).split(BLANK)[0];
				logger.info("enddate {} and currentDate :{}", enddate, currentDate);
				if (enddate.equals(currentDate)) {
					endhour = Integer.parseInt(enddateInString.split(BLANK)[1].split(":")[0]);
					append24HourInCRCHours(dateKey, calendar.getTime(), 0, endhour, format);
				} else {
					if (dateCount == 0) {
						starthour = Integer.parseInt(strtdateInString.split(BLANK)[1].split(":")[0]);
					} else {
						starthour = 0;
					}
					append24HourInCRCHours(dateKey, calendar.getTime(), starthour, 23, format);
				}
				dateCount++;
				calendar.add(Calendar.DATE, 1);
			}
		}
		return dateKey;
	}

	private static List<String> append24HourInCRCHours(List<String> dateKey, Date startDate, Integer startHour,
			Integer endhour, String format) {
		String strtdateInString = new SimpleDateFormat(format).format(startDate);
		strtdateInString = strtdateInString.split(BLANK)[0];
		for (int hour = startHour; hour <= endhour; hour++) {
			if (hour < 10) {
				dateKey.add(strtdateInString + " " + ZERO + hour + ":00");

			} else {
				dateKey.add(strtdateInString + " " + hour + ":00");
			}
		}
		return dateKey;
	}

	private Integer getHeadersForPMKPIReport(Sheet sheet, Workbook wb, Integer rowIndex, String kpiJson,
			String metaColumns, List<String> kpiIdList, String extension, Map<String, String> jobContextMap) {
		logger.info("Starting getHeadersForPMKPIReport with rowIndex={}, extension={}", rowIndex, extension);
		try {
			Integer headerColIndex = 0;
			Integer groupColIndex = 0;
			logger.info("Creating new row at index={} with height={}", rowIndex, CELL_HEIGHT_HEADER_NORMAL);
			Row row = getNewRow(sheet, rowIndex, CELL_HEIGHT_HEADER_NORMAL);

			logger.info("Creating font and cell styles");
			Font fontHeader = getFontForExcel(wb, true, FONT_SIZE_VALUE_NORMAL);
			XSSFCellStyle fixed2CellStyle = getCellStyle(wb, Color.BLACK, Color.decode("#E9DC6E"), BorderStyle.THIN,
					null, true, true, true, false, HorizontalAlignment.CENTER);
			XSSFCellStyle fixCellStyle = getCellStyle(wb, Color.BLACK, Color.decode("#E9DC6E"), BorderStyle.THIN, null,
					true, true, false, true, HorizontalAlignment.CENTER);
			XSSFCellStyle kpiCellStyle = getCellStyle(wb, Color.BLACK, Color.decode("#7BAC7B"), BorderStyle.THIN, null,
					true, true, true, true, HorizontalAlignment.CENTER);

			logger.info("Setting fonts for cell styles");
			fixCellStyle.setFont(fontHeader);
			kpiCellStyle.setFont(fontHeader);

			logger.info("Getting KPI group map from JSON");
			Map<String, Map<String, String>> kpiGroupMap = getKpiGroupWiseMap(kpiJson, extension);
			logger.info("KPI group map: {}", kpiGroupMap);

			logger.info("Processing metaColumns: {}", metaColumns);
			List<String> columnsHeader = getListFromString(StringUtils.substringAfter(metaColumns, Symbol.HASH_STRING),
					Symbol.COMMA_STRING);
			logger.info("Extracted columns header: {}", columnsHeader);

			Integer firstCol = columnsHeader.size();
			logger.info("First column index: {}", firstCol);

			Set<String> groupset = kpiGroupMap.keySet();
			logger.info("KPI groups: {}", groupset);

			if (groupset != null && groupset.contains(Symbol.EMPTY_STRING) && groupset.size() == 1) {
				logger.info("Using simple header layout (no groups)");
				setDynamicHeader(sheet, columnsHeader, headerColIndex, row, fixCellStyle, kpiCellStyle, kpiGroupMap,
						groupset, kpiIdList);
			} else {
				logger.info("Using grouped header layout");
				rowIndex = setKPIDynamicHeaderWithGroup(sheet, rowIndex, columnsHeader, headerColIndex, groupColIndex,
						row, fixCellStyle, fixed2CellStyle, kpiCellStyle, kpiGroupMap, firstCol, groupset, kpiIdList);
			}
			logger.info("Header creation completed successfully");

		} catch (Exception e) {
			logger.error("Error in creating headers: {}", ExceptionUtils.getStackTrace(e));
		}
		logger.info("Returning rowIndex: {}", rowIndex + 1);
		return ++rowIndex;
	}

	private Integer getHeadersForPMReport(Sheet sheet, Workbook wb, Integer rowIndex, String kpiJson,
			String metaColumns, List<String> kpiIdList, String timestring, String threshold) {
		logger.info("Starting getHeadersForPMReport with rowIndex={}, threshold={}", rowIndex, threshold);
		try {
			logger.info("Input parameters - metaColumns={}, timestring={}", metaColumns, timestring);

			Integer headerColIndex = 0;
			Integer groupColIndex = 0;

			logger.info("Creating new row at index={} with height={}", rowIndex, CELL_HEIGHT_HEADER_NORMAL);
			Row row = getNewRow(sheet, rowIndex, CELL_HEIGHT_HEADER_NORMAL);

			logger.info("Creating font and cell styles");
			Font fontHeader = getFontForExcel(wb, true, FONT_SIZE_VALUE_NORMAL);
			XSSFCellStyle fixed2CellStyle = getCellStyle(wb, Color.BLACK, Color.decode("#E9DC6E"), BorderStyle.THIN,
					null, true, true, true, false, HorizontalAlignment.CENTER);
			XSSFCellStyle fixCellStyle = getCellStyle(wb, Color.BLACK, Color.decode("#E9DC6E"), BorderStyle.THIN, null,
					true, true, false, true, HorizontalAlignment.CENTER);
			XSSFCellStyle kpiCellStyle = getCellStyle(wb, Color.BLACK, Color.decode("#7BAC7B"), BorderStyle.THIN, null,
					true, true, true, true, HorizontalAlignment.CENTER);

			logger.info("Setting fonts for cell styles");
			fixCellStyle.setFont(fontHeader);
			kpiCellStyle.setFont(fontHeader);

			logger.info("Getting KPI group map from JSON");
			Map<String, List<String>> kpiGroupMap = getKpiGoupWiseMap(kpiJson);
			logger.info("KPI group map: {}", kpiGroupMap);

			logger.info("Getting time group map for timestring={}", timestring);
			Map<String, List<String>> Timeupdatekey = getTimeGoupWiseMap(timestring, kpiIdList);
			logger.info("Time update key map: {}", Timeupdatekey);

			logger.info("Processing metaColumns for header extraction");
			List<String> columnsHeader = getListFromString(StringUtils.substringAfter(metaColumns, Symbol.HASH_STRING),
					Symbol.COMMA_STRING);
			logger.info("Extracted columns header: {}", columnsHeader);

			Integer firstCol = columnsHeader.size();
			logger.info("First column index: {}", firstCol);

			Set<String> groupset = kpiGroupMap.keySet();
			Set<String> timegroupset = Timeupdatekey.keySet();
			logger.info("KPI groups: {}, Time groups: {}", groupset, timegroupset);

			logger.info("Setting dynamic header with threshold={}", threshold);
			setDynamicHeader(sheet, columnsHeader, headerColIndex, row, fixCellStyle, kpiCellStyle, kpiGroupMap,
					groupset, kpiIdList, timegroupset, Timeupdatekey, threshold);

			logger.info("Header creation completed successfully");

		} catch (Exception e) {
			logger.error("Error in creating headers: {}", ExceptionUtils.getStackTrace(e));
		}
		logger.info("Returning rowIndex: {}", rowIndex + 1);
		return ++rowIndex;
	}

	private static void setDynamicHeader(Sheet sheet, List<String> columnsHeader, Integer headerColIndex, Row row,
			XSSFCellStyle fixCellStyle, XSSFCellStyle kpiCellStyle, Map<String, List<String>> kpiGroupMap,
			Set<String> groupset, List<String> kpiIdList, Set<String> timegroupset,
			Map<String, List<String>> Timeupdatekey, String threshold) {

		try {

			for (String header : columnsHeader) {
				addCellToRow(fixCellStyle, row, headerColIndex, header);
				if (header.equalsIgnoreCase(columnsHeader.get(0))) {
					sheet.setColumnWidth(headerColIndex, 2500);
				} else if (header.equalsIgnoreCase(columnsHeader.get(1))) {
					sheet.setColumnWidth(headerColIndex, 1700);
				} else {
					sheet.setColumnWidth(headerColIndex, 6500);

				}
				headerColIndex++;
			}

			//
			// for(String kpiGroup : groupset){
			// for (String kpiName : kpiGroupMap.get(kpiGroup)) {
			// kpiIdList.add(StringUtils.substringBefore(kpiName, Symbol.HYPHEN_STRING));
			// addCellToRow(kpiCellStyle, row, headerColIndex, kpiName);
			// sheet.setColumnWidth(headerColIndex, CELL_WIDTH_LARGE);
			// headerColIndex++;
			// }
			// }

			for (String timeGroup : timegroupset) {
				for (String timename : Timeupdatekey.get(timeGroup)) {
					addCellToRow(kpiCellStyle, row, headerColIndex, timename);
					sheet.setColumnWidth(headerColIndex, CELL_WIDTH_LARGE);
					headerColIndex++;
				}
			}
			if (threshold.equalsIgnoreCase("false")) {
				addCellToRow(kpiCellStyle, row, headerColIndex, "Threshold count");
			}
			sheet.setColumnWidth(headerColIndex, CELL_WIDTH_LARGE);

		} catch (Exception e) {
			logger.error("error in setting header without group in report {}", ExceptionUtils.getStackTrace(e));
		}
	}

	private static Integer setDynamicHeaderWithGroup(Sheet sheet, Integer rowIndex, List<String> columnsHeader,
			Integer headerColIndex, Integer groupColIndex, Row row, XSSFCellStyle fixCellStyle,
			XSSFCellStyle fixed2CellStyle, XSSFCellStyle kpiCellStyle, Map<String, List<String>> kpiGroupMap,
			Integer firstCol, Set<String> groupset, List<String> kpiIdList) {

		try {
			kpiIdList.clear();
			Integer lastCol;
			Row row1 = row;
			for (String kpiGroup : groupset) {
				lastCol = firstCol + kpiGroupMap.get(kpiGroup).size() - 1;
				if (kpiGroupMap.get(kpiGroup).size() != 1) {
					sheet.addMergedRegion(new CellRangeAddress(rowIndex, rowIndex, firstCol, lastCol));
				}
				addCellToRow(kpiCellStyle, row, firstCol, kpiGroup);
				groupColIndex++;
				firstCol = lastCol + 1;
			}
			row = getNewRow(sheet, ++rowIndex, CELL_HEIGHT_HEADER_NORMAL);
			for (String header : columnsHeader) {
				addCellToRow(fixed2CellStyle, row1, headerColIndex, Symbol.EMPTY_STRING);
				addCellToRow(fixCellStyle, row, headerColIndex, header);
				if (header.equalsIgnoreCase(columnsHeader.get(0))) {
					sheet.setColumnWidth(headerColIndex, 2500);
				} else if (header.equalsIgnoreCase(columnsHeader.get(1))) {
					sheet.setColumnWidth(headerColIndex, 1700);
				} else {
					sheet.setColumnWidth(headerColIndex, 6500);
				}
				headerColIndex++;
			}
			for (String kpiGroup : groupset) {
				for (String kpiName : kpiGroupMap.get(kpiGroup)) {
					kpiIdList.add(StringUtils.substringBefore(kpiName, Symbol.HYPHEN_STRING));
					addCellToRow(kpiCellStyle, row, headerColIndex, kpiName);
					sheet.setColumnWidth(headerColIndex, CELL_WIDTH_LARGE);
					headerColIndex++;
				}
			}
		} catch (Exception e) {
			logger.error("error in setting header in report {}", ExceptionUtils.getStackTrace(e));
		}
		return rowIndex;
	}

	private void setDynamicHeader(Sheet sheet, List<String> columnsHeader, Integer headerColIndex, Row row,
			XSSFCellStyle fixCellStyle, XSSFCellStyle kpiCellStyle, Map<String, Map<String, String>> kpiGroupMap,
			Set<String> groupset, List<String> kpiIdList) {
		try {
			kpiIdList.clear();
			for (String header : columnsHeader) {
				addCellToRow(fixCellStyle, row, headerColIndex, header);
				if (header.equalsIgnoreCase(columnsHeader.get(0))) {
					sheet.setColumnWidth(headerColIndex, 2500);
				} else if (header.equalsIgnoreCase(columnsHeader.get(1))) {
					sheet.setColumnWidth(headerColIndex, 1700);
				} else {
					sheet.setColumnWidth(headerColIndex, 6500);

				}
				headerColIndex++;
			}
			for (String kpiGroup : groupset) {
				for (Map.Entry<String, String> kpiHeaderMap : kpiGroupMap.get(kpiGroup).entrySet()) {
					String kpiName = kpiHeaderMap.getKey();
					String headerName = kpiHeaderMap.getValue();
					kpiIdList.add(StringUtils.substringBefore(kpiName, Symbol.HYPHEN_STRING));
					addCellToRow(kpiCellStyle, row, headerColIndex, headerName);
					sheet.setColumnWidth(headerColIndex, CELL_WIDTH_LARGE);
					headerColIndex++;
				}
			}
		} catch (Exception e) {
			logger.error("error in setting header without group in report {}", ExceptionUtils.getStackTrace(e));
		}
	}

	private Integer setKPIDynamicHeaderWithGroup(Sheet sheet, Integer rowIndex, List<String> columnsHeader,
			Integer headerColIndex, Integer groupColIndex, Row row, XSSFCellStyle fixCellStyle,
			XSSFCellStyle fixed2CellStyle, XSSFCellStyle kpiCellStyle, Map<String, Map<String, String>> kpiGroupMap,
			Integer firstCol, Set<String> groupset, List<String> kpiIdList) {

		try {
			kpiIdList.clear();
			Integer lastCol;
			Row row1 = row;
			for (String kpiGroup : groupset) {
				lastCol = firstCol + kpiGroupMap.get(kpiGroup).size() - 1;
				if (kpiGroupMap.get(kpiGroup).size() != 1) {
					sheet.addMergedRegion(new CellRangeAddress(rowIndex, rowIndex, firstCol, lastCol));
				}
				addCellToRow(kpiCellStyle, row, firstCol, kpiGroup);
				groupColIndex++;
				firstCol = lastCol + 1;
			}
			row = getNewRow(sheet, ++rowIndex, CELL_HEIGHT_HEADER_NORMAL);
			for (String header : columnsHeader) {
				addCellToRow(fixed2CellStyle, row1, headerColIndex, Symbol.EMPTY_STRING);
				addCellToRow(fixCellStyle, row, headerColIndex, header);
				if (header.equalsIgnoreCase(columnsHeader.get(0))) {
					sheet.setColumnWidth(headerColIndex, 2500);
				} else if (header.equalsIgnoreCase(columnsHeader.get(1))) {
					sheet.setColumnWidth(headerColIndex, 1700);
				} else {
					sheet.setColumnWidth(headerColIndex, 6500);
				}
				headerColIndex++;
			}
			for (String kpiGroup : groupset) {
				for (Map.Entry<String, String> kpiHeaderMap : kpiGroupMap.get(kpiGroup).entrySet()) {
					String kpiName = kpiHeaderMap.getKey();
					String headerName = kpiHeaderMap.getValue();
					kpiIdList.add(StringUtils.substringBefore(kpiName, Symbol.HYPHEN_STRING));
					addCellToRow(kpiCellStyle, row, headerColIndex, headerName);
					sheet.setColumnWidth(headerColIndex, CELL_WIDTH_LARGE);
					headerColIndex++;
				}
			}
		} catch (Exception e) {
			logger.error("error in setting header in report {}", ExceptionUtils.getStackTrace(e));
		}
		return rowIndex;
	}

	private List<String> getListFromString(String metaColumns, String separator) {
		return new ArrayList<>(Arrays.asList(metaColumns.split(separator)));
	}

	private Map<String, List<String>> getKpiGoupWiseMap(String kpiJson) {
		Map<String, List<String>> kpiGroupMap = new LinkedHashMap<>();
		try {
			JSONArray kpiJsonArray = new JSONArray(kpiJson);
			for (Integer i = 0; i < kpiJsonArray.length(); i++) {
				JSONObject jsonObject = kpiJsonArray.getJSONObject(i);
				String kpiName = getJsonValue(jsonObject, "kpiName", Symbol.EMPTY_STRING);
				String kpiGroup = getJsonValue(jsonObject, "kpigroup", Symbol.EMPTY_STRING);
				if (!kpiGroupMap.containsKey(kpiGroup)) {
					List<String> wrapperList = new ArrayList<>();
					wrapperList.add(kpiName);
					kpiGroupMap.put(kpiGroup, wrapperList);
				} else {
					List<String> wrapperList = kpiGroupMap.get(kpiGroup);
					wrapperList.add(kpiName);
					kpiGroupMap.put(kpiGroup, wrapperList);
				}
			}
		} catch (Exception e) {
			logger.error("Exception parse KPI Json :{}", ExceptionUtils.getStackTrace(e));
		}
		return kpiGroupMap;
	}

	private Map<String, Map<String, String>> getKpiGroupWiseMap(String kpiJson, String extension) {
		Map<String, Map<String, String>> kpiGroupMap = new LinkedHashMap<>();
		String kpiGroup = Symbol.EMPTY_STRING;
		try {
			JSONArray kpiJsonArray = new JSONArray(kpiJson);
			for (Integer i = 0; i < kpiJsonArray.length(); i++) {
				JSONObject jsonObject = kpiJsonArray.getJSONObject(i);
				String kpiName = getJsonValue(jsonObject, "kpiName", Symbol.EMPTY_STRING);
				String headerName = getJsonValue(jsonObject, "headerName", Symbol.EMPTY_STRING);
				String type = getJsonValue(jsonObject, "type", Symbol.EMPTY_STRING);
				if (com.enttribe.commons.lang.StringUtils.isEmpty(headerName)) {
					headerName = kpiName;
				}
				if (!extension.equalsIgnoreCase("csv")) {
					kpiGroup = getJsonValue(jsonObject, "kpigroup", Symbol.EMPTY_STRING);
				}
				if (!kpiGroupMap.containsKey(kpiGroup)) {
					Map<String, String> wrapper = new LinkedHashMap<>();
					wrapper.put(kpiName, headerName);
					kpiGroupMap.put(kpiGroup, wrapper);
				} else {
					Map<String, String> wrapper = kpiGroupMap.get(kpiGroup);
					wrapper.put(kpiName, headerName);
					kpiGroupMap.put(kpiGroup, wrapper);
				}
			}
		} catch (Exception e) {
			logger.error("Exception parse KPI Json :{}", ExceptionUtils.getStackTrace(e));
		}
		// logger.info("kpiGroupMap : {}", kpiGroupMap);
		return kpiGroupMap;
	}

	private Map<String, String> getKpicodeGoupWiseMap(String kpiJson) {
		Map<String, String> kpiGroupMap = new LinkedHashMap<>();
		try {
			JSONArray kpiJsonArray = new JSONArray(kpiJson);
			for (Integer i = 0; i < kpiJsonArray.length(); i++) {
				String kpiid = "";
				JSONObject jsonObject = kpiJsonArray.getJSONObject(i);
				String headerName = getJsonValue(jsonObject, "headerName", Symbol.EMPTY_STRING);
				String kpiName = getJsonValue(jsonObject, "kpiName", Symbol.EMPTY_STRING);
				String type = getJsonValue(jsonObject, "type", Symbol.EMPTY_STRING);

				if (com.enttribe.commons.lang.StringUtils.isEmpty(headerName)) {
					headerName = kpiName;
				}
				String threshold_condition = getJsonValue(jsonObject, "threshold_conditions", Symbol.EMPTY_STRING);
				String kpiNameThreshold = headerName + "##" + threshold_condition;
				kpiid = StringUtils.substringBefore(kpiName, "-");
				if (!kpiGroupMap.containsKey(kpiid)) {
					kpiGroupMap.put(kpiid, kpiNameThreshold);
				}
			}
		} catch (Exception e) {
			logger.error("Exception parse KPI Json :{}", ExceptionUtils.getStackTrace(e));
		}
		return kpiGroupMap;
	}

	private static Map<String, List<String>> getTimeGoupWiseMap(String timeKeyList, List<String> kpiIdList) {
		Map<String, List<String>> kpiGroupMap = new LinkedHashMap<>();
		try {
			List<String> updateTimeKeyList = new ArrayList<>();
			for (String timeKey : timeKeyList.split(Symbol.COMMA_STRING)) {
				logger.info("timeKey :{}", timeKey);

				String updatetimekey = timeKey.replace("'", "");
				String date = updatetimekey;
				updateTimeKeyList.add(date);
				kpiIdList.add(date);
				System.out.println("kpiIdList.add" + kpiIdList);

			}

			kpiGroupMap.put(Symbol.EMPTY_STRING, updateTimeKeyList);
			// System.out.println("timestringmap" + kpiGroupMap);
		} catch (Exception e) {
			logger.error("Exception parse KPI Json :{}", ExceptionUtils.getStackTrace(e));
		}
		return kpiGroupMap;
	}

	public static Date parseTimeByFrequency(String timeInString, String frequency) {
		DateFormat formatter = null;
		Date date = new Date();
		// logger.info("timeInString :{} ", timeInString);
		try {
			if (timeInString.length() == 8) {
				formatter = new SimpleDateFormat("YYMMDDHH");
				date = formatter.parse(timeInString);
			} else if (timeInString.length() == 6) {
				formatter = new SimpleDateFormat("YYMMDD");
				date = formatter.parse(timeInString);
			} else if (timeInString.length() == 4) {
				if (frequency.equalsIgnoreCase("PERDAY")) {
					date = getStartDateofWeek(Integer.parseInt(timeInString.substring(2)),
							Integer.parseInt("20" + timeInString.substring(0, 2)));
				} else if (frequency.equalsIgnoreCase("PERWEEK") || frequency.equalsIgnoreCase("PERMONTH")) {
					date = getStartDateofMonth(Integer.parseInt(timeInString.substring(2)),
							Integer.parseInt("20" + timeInString.substring(0, 2)));
				}
			} else if (timeInString.length() == 10) {
				formatter = new SimpleDateFormat("YYMMDDHHMM");
				date = formatter.parse(timeInString);
			}
		} catch (ParseException e) {
			logger.error("Error in parsing Time : {} frequency {}  Error {}", timeInString, frequency,
					ExceptionUtils.getStackTrace(e));
		}
		return date;
	}

	// public static Date getStartDateofWeek(Integer weekNumber, Integer year) {
	// Calendar cal = Calendar.getInstance();
	// cal.set(Calendar.WEEK_OF_YEAR, weekNumber);
	// cal.set(Calendar.YEAR, year);
	// cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
	// cal.set(Calendar.HOUR_OF_DAY, 00);
	// cal.set(Calendar.MINUTE, 00);
	// cal.set(Calendar.SECOND, 00);
	// return cal.getTime();
	// }

	public static Date getStartDateofWeek(Integer weekNumber, Integer year) {
		DateTime dt = new DateTime();
		dt = dt.withYear(year);
		dt = dt.withWeekOfWeekyear(weekNumber);
		dt = dt.withDayOfWeek(1);
		dt = dt.withHourOfDay(0);
		dt = dt.withMinuteOfHour(0);
		dt = dt.withSecondOfMinute(0);
		return dt.toDate();
	}

	public static Date getStartDateofMonth(Integer monthNumber, Integer year) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MONTH, (monthNumber - 1));
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.DATE, 01);
		cal.set(Calendar.HOUR_OF_DAY, 00);
		cal.set(Calendar.MINUTE, 00);
		cal.set(Calendar.SECOND, 00);
		return cal.getTime();
	}

	private String getJsonValue(JSONObject configJson, String jsonKey, String jsonValueKey) throws JSONException {
		if (configJson.has(jsonKey) && !configJson.isNull(jsonKey)) {
			jsonValueKey = !configJson.getString(jsonKey).equalsIgnoreCase("null") ? configJson.getString(jsonKey)
					: jsonValueKey;
		}
		return jsonValueKey;
	}

	public static void writeHeaderName(String[] headerName, Sheet sheet, org.apache.poi.ss.usermodel.Row row) {
		int cellNum = 0;
		for (String header : headerName) {
			Cell cell = row.createCell(cellNum++);
			writeValue(cell, header);
		}
	}

	/**
	 * Gets the new row.
	 *
	 * @param sheet      the sheet
	 * @param index      the index
	 * @param cellHeight the cell height
	 * @return the new row
	 */
	public static Row getNewRow(Sheet sheet, Integer index, Integer cellHeight) {
		Row row = sheet.createRow(index);
		if (cellHeight != null)
			row.setHeightInPoints(cellHeight);
		return row;
	}

	/**
	 * Gets the font for excel.
	 *
	 * @param wb     the wb
	 * @param isBold the is bold
	 * @param height the height
	 * @return the font for excel
	 */
	public static Font getFontForExcel(Workbook wb, boolean isBold, Integer height) {
		Font font = wb.createFont();
		font.setBold(true);
		return font;
	}

	public static XSSFCellStyle getCellStyle(Workbook wb, Color bordercolor, Color cellForeground,
			BorderStyle borderStyle, XSSFFont font, boolean left, boolean right, boolean top, boolean bottom,
			HorizontalAlignment alignCenter) {
		XSSFColor clr = null;
		if (bordercolor != null) {
			byte[] rgb = new byte[] {
					(byte) bordercolor.getRed(),
					(byte) bordercolor.getGreen(),
					(byte) bordercolor.getBlue()
			};
			clr = new XSSFColor(rgb, null);
		}

		XSSFCellStyle style = (XSSFCellStyle) wb.createCellStyle();
		if (left) {
			style.setLeftBorderColor(clr);
			style.setBorderLeft(borderStyle);
		}

		if (right) {
			style.setBorderRight(borderStyle);
			style.setRightBorderColor(clr);
		}

		if (top) {
			style.setBorderTop(borderStyle);
			style.setTopBorderColor(clr);
		}

		if (bottom) {
			style.setBorderBottom(borderStyle);
			style.setBottomBorderColor(clr);
		}

		style.setAlignment(alignCenter);
		style.setVerticalAlignment(VerticalAlignment.CENTER);
		if (cellForeground != null) {
			style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			byte[] rgb = new byte[] {
					(byte) cellForeground.getRed(),
					(byte) cellForeground.getGreen(),
					(byte) cellForeground.getBlue()
			};
			XSSFColor foreClr = new XSSFColor(rgb, null);
			style.setFillForegroundColor(foreClr);
		}
		if (font != null) {
			style.setFont(font);
		}
		style.setWrapText(true);
		return style;
	}

	/**
	 * Adds the cell to row.
	 *
	 * @param style       the style
	 * @param row         the row
	 * @param columnIndex the column index
	 * @param value       the value
	 */
	public static void addCellToRow(CellStyle style, Row row, Integer columnIndex, Object value) {
		org.apache.poi.ss.usermodel.Cell tddCell = row.createCell(columnIndex);
		tddCell.setCellStyle(style);
		if (value == null) {
			tddCell.setCellValue(Symbol.HASH_STRING);
		}
		if (value instanceof String) {
			tddCell.setCellValue((String) value);
		}
		if (value instanceof Double) {
			tddCell.setCellValue((Double) value);
		}
		if (value instanceof Integer) {
			tddCell.setCellValue((Integer) value);
		}
		if (value instanceof Long) {
			tddCell.setCellValue((Long) value);
		}
		if (value instanceof Float) {
			tddCell.setCellValue((Float) value);
		}
	}

	/**
	 * Gets the cell style.
	 *
	 * @param wb             the wb
	 * @param bordercolor    the bordercolor
	 * @param cellForeground the cell foreground
	 * @param font           the font
	 * @param left           the left
	 * @param right          the right
	 * @param top            the top
	 * @param bottom         the bottom
	 * @return the cell style
	 */
	public static CellStyle getCellStyle(Workbook wb, IndexedColors bordercolor, Short cellForeground, Font font,
			boolean left, boolean right, boolean top, boolean bottom) {
		short clr = bordercolor == null ? IndexedColors.BLACK.getIndex() : bordercolor.getIndex();
		CellStyle style = wb.createCellStyle();
		if (left) {
			style.setLeftBorderColor(clr);
			style.setBorderLeft(BorderStyle.THIN);
		}
		if (right) {
			style.setBorderRight(BorderStyle.THIN);
			style.setRightBorderColor(clr);
		}
		if (top) {
			style.setBorderTop(BorderStyle.THIN);
			style.setTopBorderColor(clr);
		}
		if (bottom) {
			style.setBorderBottom(BorderStyle.THIN);
			style.setBottomBorderColor(clr);
		}
		style.setAlignment(HorizontalAlignment.CENTER);
		style.setVerticalAlignment(VerticalAlignment.CENTER);
		if (cellForeground != null) {
			style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			style.setFillForegroundColor(cellForeground);
		}
		if (font != null) {
			style.setFont(font);

		}
		style.setWrapText(true);
		return style;
	}

	/**
	 * Write value on cell.
	 *
	 * @param row              the row
	 * @param cellStyle        the cell style
	 * @param isAutosizeEnable the is autosize enable
	 * @param sheet            the sheet
	 * @param index            the index
	 * @param value            the value
	 * @return the int
	 */
	public static int writeValueOnCell(Row row, CellStyle cellStyle, boolean isAutosizeEnable, Sheet sheet, int index,
			Integer value) {
		try {
			Cell cell = row.createCell(index);
			writeValue(cell, value);
			cell.setCellStyle(cellStyle);

		} catch (Exception e) {
			logger.error("error in writeValueOnCell" + ExceptionUtils.getStackTrace(e));
		}
		return index;
	}

	public static int writeValueOnCell(Row row, CellStyle cellStyle, boolean isAutosizeEnable, Sheet sheet, int index,
			Double value) {
		try {
			Cell cell = row.createCell(index);
			if (isValidDouble(value)) {
				writeValue(cell, value);
				cell.setCellStyle(cellStyle);
			} else {
				cell.setCellValue(Symbol.HYPHEN_STRING);
				cell.setCellStyle(cellStyle);
			}
		} catch (Exception e) {
			logger.error("error in writeValueOnCell" + ExceptionUtils.getStackTrace(e));
		}
		return index;
	}

	/**
	 * Write value on cell.
	 *
	 * @param row              the row
	 * @param cellStyle        the cell style
	 * @param isAutosizeEnable the is autosize enable
	 * @param sheet            the sheet
	 * @param i                the i
	 * @param value            the value
	 * @return the int
	 */
	public static int writeValueOnCell(Row row, CellStyle cellStyle, boolean isAutosizeEnable, Sheet sheet, int index,
			String value) {
		try {
			Cell cell = row.createCell(index);
			cell.setCellValue(value == null ? Symbol.HYPHEN_STRING : value);
			cell.setCellStyle(cellStyle);
			if (isAutosizeEnable) {
				sheet.autoSizeColumn(index);
			}
		} catch (Exception e) {
			logger.error("error in creating rows for Excel");
		}
		return index;
	}

	/**
	 * Write value on cell.
	 *
	 * @param row              the row
	 * @param cellStyle        the cell style
	 * @param isAutosizeEnable the is autosize enable
	 * @param sheet            the sheet
	 * @param index            the index
	 * @param value            the value
	 * @return the int
	 */
	public int writeValueOnCell(Row row, CellStyle cellStyle, boolean isAutosizeEnable, Sheet sheet, int index,
			Double value, Integer count, XSSFCellStyle greenkpiCellStyle, XSSFCellStyle redkpiCellStyle,
			String threshold_conditions) {
		String result = "0";
		if (threshold_conditions != null && threshold_conditions != ("") && !threshold_conditions.isEmpty()
				&& value != null && isValidDouble(value)) {
			String condition = "";
			if (threshold_conditions.contains("between")) {
				String[] threshold = threshold_conditions.split("#")[1].split("to");
				String betweenCondition = value + ">=" + threshold[0] + " && " + value + "<=" + threshold[1];
				condition = betweenCondition;

			} else {
				condition = value + threshold_conditions;
			}
			Expression e = new Expression(condition);
			result = e.eval();
		}
		try {
			Cell cell = row.createCell(index);
			if (isValidDouble(value)) {
				if (result.contains("1")) {
					writeValue(cell, value);
					cell.setCellStyle(greenkpiCellStyle);
					count++;
				} else {
					writeValue(cell, value);
					cell.setCellStyle(redkpiCellStyle);
				}
			} else {
				cell.setCellValue(Symbol.HYPHEN_STRING);
				cell.setCellStyle(cellStyle);
			}
		} catch (Exception e) {
			logger.error("error in writeValueOnCell" + ExceptionUtils.getStackTrace(e));
		}
		return count;
	}

	/**
	 * Checks if is valid double.
	 *
	 * @param value the value
	 * @return true, if is valid double
	 */
	public static boolean isValidDouble(Double value) {
		try {
			return (value != null && !Double.isNaN(value));
		} catch (Exception e) {
			logger.error("getting exception on validate double = {}", value);
			return false;
		}
	}

	/**
	 * Write value.
	 *
	 * @param cell   the cell
	 * @param object the object
	 */
	public static void writeValue(Cell cell, Object object) {
		if (object == null) {
			cell.setCellValue(Symbol.HYPHEN_STRING);
		} else if (object instanceof String) {
			cell.setCellValue((String) object);
		} else if (object instanceof Double) {
			cell.setCellValue((Double) object);
		} else if (object instanceof Integer) {
			cell.setCellValue((Integer) object);
		} else if (object instanceof Long) {
			cell.setCellValue((Long) object);
		} else if (object instanceof Float) {
			cell.setCellValue((Float) object);
		} else if (object instanceof Date) {
			cell.setCellValue((Date) object);
		}
	}

	/*
	 *
	 * ReportUtils Class Code Need to Move after Testing
	 *
	 */
	public static final String TREND_REPORT = "Trend Report";
	public static final String TREND_TYPE = "Trend";
	public static final String CUSTOM = "CUSTOM";
	public static final String VENDOR_JSONKEY = "vendor";
	public static final String FREQUENCY_JSONKEY = "frequency";
	public static final String NETYPE_JSONKEY = "netype";
	public static final String REPORTLEVEL = "reportLevel";
	public static final String REPORTTYPE = "reportType";
	public static final String REPORTGEOGRAPHY = "geography";
	public static final String _DDMMYYYY = "_ddMMyyyy";
	public static final String All = "All";
	public static final String PERFORMANCE_REPORT = "Performance Report";
	public static final String EXCEPTION_TYPE = "Exception";
	public static final String KPI = "KPI";
	public static final String HHMMSS = "HHmmss";
	public static final String CELL = "Cell";
	public static final String ENODEB = "eNodeB";
	public static final String ENODEB_NODE = "eNodeB";
	public static final String REPORTNAME = "reportName";
	public static final String MAXTIME = "maxTime";
	public static final String MINTIME = "minTime";
	public static final String DATES = "dates";

	private String getExcelReportName(HashMap<String, String> parameterMap) {
		String domain = parameterMap.get(DOMAIN_JSONKEY);
		String vendor = parameterMap.get(VENDOR_JSONKEY);
		String neType = parameterMap.get(NETYPE_JSONKEY);
		String frequencies = parameterMap.get(FREQUENCY_JSONKEY);
		String reportType = parameterMap.get(REPORTTYPE);
		String reportName = parameterMap.get(REPORTNAME);
		String reportLevel = parameterMap.get(REPORTLEVEL);
		String geography = parameterMap.get(REPORTGEOGRAPHY);
		String nodeName = getUpdateNodeName(parameterMap.get(NODE_JSONKEY));

		logger.info("Start building Excel report name...");
		logger.info(
				"Input values: domain={}, vendor={}, neType={}, frequencies={}, reportType={}, reportName={}, reportLevel={}, geography={}, nodeName={}",
				domain, vendor, neType, frequencies, reportType, reportName, reportLevel, geography, nodeName);

		logger.info("reportName before replace: {}", reportName);

		try {
			String date = new SimpleDateFormat(_DDMMYYYY).format(new Date());

			if (StringUtils.equalsIgnoreCase(neType, All)) {
				logger.info("neType is '{}', setting vendor to '{}'", neType, All);
				vendor = All;
			}

			String frequency = StringUtils.replace(frequencies, Symbol.COMMA_STRING, Symbol.UNDERSCORE_STRING);
			logger.info("Processed frequency: {}", frequency);

			if (StringUtils.equalsIgnoreCase(reportType, PERFORMANCE_REPORT)) {
				logger.info("Report type '{}' matched PERFORMANCE_REPORT, setting to '{}'", reportType, KPI);
				reportType = KPI;
			} else if (StringUtils.equalsIgnoreCase(reportType, TREND_REPORT)) {
				logger.info("Report type '{}' matched TREND_REPORT, setting to '{}'", reportType, TREND_TYPE);
				reportType = TREND_TYPE;
			} else if (StringUtils.equalsIgnoreCase(reportType, "Custom Aggregation Report")) {
				logger.info("Report type '{}' matched Custom Aggregation Report, setting to '{}'", reportType, CUSTOM);
				reportType = CUSTOM;
			} else {
				logger.info("Report type '{}' did not match known values, setting to 'OTF'", reportType);
				reportType = "OTF";
			}

			reportName = StringEscapeUtils.unescapeHtml(reportName) + Symbol.UNDERSCORE_STRING + reportType
					+ Symbol.UNDERSCORE_STRING + domain + Symbol.UNDERSCORE_STRING + vendor + Symbol.UNDERSCORE_STRING
					+ nodeName + Symbol.UNDERSCORE_STRING + frequency + Symbol.UNDERSCORE_STRING + reportLevel
					+ Symbol.UNDERSCORE_STRING + geography + date + Symbol.UNDERSCORE_STRING
					+ new SimpleDateFormat(HHMMSS).format(new Date()) + ".xlsx";

			logger.info("Generated report name: {}", reportName);

		} catch (Exception e) {
			logger.error("Error in getting full name of Report: {}", Utils.getStackTrace(e));
		}

		return reportName;
	}

	public static List<Map<String, Date>> getDateRangeNew(String dayNo, String duration, String starTime,
			String endTime, List<Date> dateList, List<String> hourList) throws ParseException {
		List<Map<String, Date>> dateRangeList = new ArrayList<>();
		Map<String, Date> dateRange = new HashMap<>();
		Date maxTime = null;
		Date minTime = null;
		if (duration.equalsIgnoreCase(DURATION_DAILY)) {
			maxTime = DateUtils.addMilliseconds(DateUtils.ceiling(DateUtils.addDays(new Date(), -1), Calendar.DATE),
					-1);
			minTime = DateUtils.truncate(DateUtils.addDays(new Date(), -Integer.valueOf(dayNo)), Calendar.DATE);
		} else if (duration.equalsIgnoreCase(DURATION_HOURLY)) {
			if (dayNo != null && !dayNo.equalsIgnoreCase("")) {
				maxTime = DateUtils.addHours(new Date(), -1);
				minTime = DateUtils.addHours(new Date(), -(Integer.valueOf(dayNo)));
				logger.info("dayno in if part for minTime  : {} and minTime : {} and dayno : {}", minTime, maxTime,
						dayNo);

			} else {
				maxTime = DateUtils.addHours(new Date(), -1);
				minTime = DateUtils.addHours(new Date(), -24);
				logger.info("dayno for minTime  : {} and minTime : {} and dayno : {}", minTime, maxTime, dayNo);
			}
		} else if (duration.equalsIgnoreCase(DURATION_WEEKLY) || duration.equalsIgnoreCase(DURATION_WEEKLY_BBH)) {
			Date[] preWeekStartEndDate = getPreviousWeekStartEndDate(dayNo);
			minTime = preWeekStartEndDate[0];
			maxTime = preWeekStartEndDate[1];
		} else if (duration.equalsIgnoreCase(DURATION_MONTHLY)) {
			Date[] preWeekStartEndDate = getPreviousMonthStartEndDate(dayNo);
			minTime = preWeekStartEndDate[0];
			maxTime = preWeekStartEndDate[1];
		} else if (duration.equalsIgnoreCase(DATES)) {
			if (dateList != null && !dateList.isEmpty()) {
				logger.info("inside specfic");
				for (Date date : dateList) {
					Map<String, Date> newMap = new HashMap<>();
					newMap.put(MINTIME, date);
					newMap.put(MAXTIME, DateUtils.addHours(date, hourList.size() - 1));
					dateRangeList.add(newMap);
				}
				return dateRangeList;
			} else {
				minTime = new SimpleDateFormat("MMM dd,yyyy HH:mm").parse(starTime);
				maxTime = new SimpleDateFormat("MMM dd,yyyy HH:mm").parse(endTime);
			}
		}
		dateRange.put(MINTIME, minTime);
		dateRange.put(MAXTIME, maxTime);
		dateRangeList.add(dateRange);
		logger.info("Search time interval is : {} ", dateRange);
		return dateRangeList;
	}

	// public static Date[] getPreviousWeekStartEndDate() {
	// Calendar calendar = Calendar.getInstance();
	// calendar.setTime(new Date());
	// Date maxTime =
	// DateUtils.addMilliseconds(DateUtils.ceiling(lastDayOfLastWeek(calendar),
	// Calendar.DATE), 0);
	// Date minTime = DateUtils.truncate(firstDayOfLastWeek(calendar),
	// Calendar.DATE);
	// return new Date[] { minTime, maxTime };
	// }
	public static Date[] getPreviousWeekStartEndDate(String dayno) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		Date maxTime = DateUtils.addMilliseconds(DateUtils.ceiling(lastDayOfLastWeek(calendar), Calendar.DATE), -0);
		Date minTime = DateUtils.truncate(firstDayOfLastWeek(calendar, dayno), Calendar.DATE);
		return new Date[] { minTime, maxTime };
	}

	/**
	 * First day of last week.
	 *
	 * @param c the c
	 * @return the date
	 */
	// public static Date firstDayOfLastWeek(Calendar c) {
	// c = (Calendar) c.clone();
	// c.add(Calendar.WEEK_OF_YEAR, -1);
	// c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
	// return c.getTime();
	// }

	public static Date firstDayOfLastWeek(Calendar c, String dayno) {
		c = (Calendar) c.clone();
		c.add(Calendar.WEEK_OF_YEAR, -Integer.valueOf(dayno));
		c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		return c.getTime();
	}

	/**
	 * Last day of last week.
	 *
	 * @param c the c
	 * @return the date
	 */
	public static Date lastDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		c.add(Calendar.DAY_OF_MONTH, -2);
		return c.getTime();
	}

	/**
	 * Gets the previous month start end date.
	 *
	 * @return the previous month start end date
	 */
	// public static Date[] getPreviousMonthStartEndDate() {
	// Calendar calStart = Calendar.getInstance();
	// calStart.add(Calendar.MONTH, -1);
	// calStart.set(Calendar.DATE, 1);
	// calStart.set(Calendar.HOUR_OF_DAY, 0);
	// calStart.set(Calendar.MINUTE, 0);
	// calStart.set(Calendar.SECOND, 0);
	// calStart.set(Calendar.MILLISECOND, 0);
	// Date firstDateOfPreviousWeek = calStart.getTime();
	// calStart.set(Calendar.DATE, calStart.getActualMaximum(Calendar.DAY_OF_MONTH)
	// + 1);
	// Date lastDateOfPreviousWeek = calStart.getTime();
	// return new Date[] { firstDateOfPreviousWeek, lastDateOfPreviousWeek };
	// }

	public static Date[] getPreviousMonthStartEndDate(String dayno) {
		Calendar calStart = Calendar.getInstance();
		Calendar calStart1 = Calendar.getInstance();
		calStart.add(Calendar.MONTH, -Integer.valueOf(dayno));
		calStart.set(Calendar.DATE, 1);
		calStart.set(Calendar.HOUR_OF_DAY, 0);
		calStart.set(Calendar.MINUTE, 0);
		calStart.set(Calendar.SECOND, 0);
		calStart.set(Calendar.MILLISECOND, 0);
		Date firstDateOfPreviousWeek = calStart.getTime();
		calStart1.set(Calendar.DATE, 1);
		calStart1.add(Calendar.DAY_OF_MONTH, -1);
		calStart1.set(Calendar.HOUR_OF_DAY, 0);
		calStart1.set(Calendar.MINUTE, 0);
		calStart1.set(Calendar.SECOND, 0);
		calStart1.set(Calendar.MILLISECOND, 0);
		Date lastDateOfPreviousMonth = calStart1.getTime();
		return new Date[] { firstDateOfPreviousWeek, lastDateOfPreviousMonth };
	}

	public static String getUpdateNodeName(String node) {
		try {
			String nodeType = StringUtils
					.substringAfter(StringUtils.substringBeforeLast(node, Symbol.HYPHEN_STRING), " ").trim()
					.toUpperCase();
			node = updateCustomNode(nodeType);
		} catch (Exception e) {
			logger.error("Exception in getting Netype from Report Configuration :{}", ExceptionUtils.getStackTrace(e));
		}
		return node;
	}

	public static String updateCustomNode(String nodeTypeForReport) {
		String node = nodeTypeForReport;
		if (CELL.equalsIgnoreCase(nodeTypeForReport)) {
			node = CELL;
		} else if (ENODEB.equalsIgnoreCase(nodeTypeForReport)) {
			node = ENODEB_NODE;
		} else if (nodeTypeForReport.equalsIgnoreCase("SMALL_CELL")) {
			node = "Small Cell";
		} else if (nodeTypeForReport.equalsIgnoreCase("VCU")) {
			node = "vCU";
		} else if (nodeTypeForReport.equalsIgnoreCase("VDU")) {
			node = "vDU";
		}
		return node;
	}

	public static FileSystem getFileSystem(String uri) throws Exception {
		FileSystem fileSystem = null;

		try {
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

			// S3A Configuration
			conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
			conf.set("fs.s3a.endpoint", sparkMinioEndpointUrl); // for MinIO
			conf.set("fs.s3a.access.key", sparkMinioAccessKey);
			conf.set("fs.s3a.secret.key", sparkMinioSecretKey);
			conf.set("fs.s3a.path.style.access", "true");
			conf.set("fs.s3a.fast.upload", "true");
			conf.set("fs.s3a.buffer.dir", "/tmp/");
			conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

			fileSystem = FileSystem.get(new java.net.URI(uri), conf);
		} catch (Exception var4) {
			logger.error("Exception while connecting to S3A", var4);
			throw var4;
		}
		logger.info("Successfully connected to filesystem for URI: {}", uri);
		return fileSystem;
	}

	/*************************************
	 * END PMReportUtils class Code
	 ***************************/
}