package com.enttribe.pm.job.quarterly.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.sparkrunner.annotations.Dynamic;
import com.enttribe.sparkrunner.constants.SparkRunnerConstants;
import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.processor.audit.ProcessorAudit;
import com.enttribe.sparkrunner.processors.Processor;
import com.enttribe.sparkrunner.util.AWSS3Util;
import com.enttribe.sparkrunner.util.ConfigUtil;
import com.enttribe.sparkrunner.util.Utils;
import com.enttribe.sparkrunner.utils.exceptions.BusinessException;

public class ListMINIOCustom extends Processor implements Serializable {

	private static final long serialVersionUID = 1L;

	private static Logger logger = LoggerFactory.getLogger(ListMINIOCustom.class);

	@Dynamic
	public String path;
	public String columnName;
	public String tempTable;
	public String isRecursive;
	public String bucket;
	private String minioAccessKey = null;
	private String minioSecretKey = null;

	public ListMINIOCustom() {
		super();
	}

	public ListMINIOCustom(int processorId, String processorName, String hdfsFolderPath, String columnName,
			String tempTable) {
		super(processorId, processorName);
		this.path = hdfsFolderPath;
		this.columnName = columnName;
		this.tempTable = tempTable;
	}

	public ListMINIOCustom(int processorId, String processorName, String hdfsFolderPath, String columnName,
			String bucket, String tempTable, String isRecursive) {
		super(processorId, processorName);
		this.path = hdfsFolderPath;
		this.columnName = columnName;
		this.bucket = bucket;
		this.tempTable = tempTable;
		this.isRecursive = isRecursive;
	}

	@Override
	public Dataset<Row> executeAndGetResultDataframe(JobContext jobContext) throws Exception {
		Map<String, String> contextMap = jobContext.getParameters();
		this.bucket = contextMap.get("SPARK_MINIO_BUCKET_NAME_PM");
		minioAccessKey = contextMap.get("SPARK_MINIO_ACCESS_KEY");
		minioSecretKey = contextMap.get("SPARK_MINIO_SECRET_KEY");
		this.path = this.bucket + ":" + contextMap.get("PREVIOUS_RAW_FILE_PATH");
		this.columnName = "fileName";
		this.tempTable = "fileTablePrevious";
		this.isRecursive = "true";

		logger.info("ListMINIOCustom: Bucket: {}, Path: {}, ColumnName: {}, TempTable: {}, IsRecursive: {}",
				this.bucket, this.path, this.columnName, this.tempTable, this.isRecursive);
		logger.info("ListMINIOCustom: MinioAccessKey: {}, MinioSecretKey: {}", minioAccessKey, minioSecretKey);

		List<String> files = new ArrayList<>();

		String[] bucketArr = this.bucket.split(Symbol.COMMA_STRING);
		Map<String, Set<String>> generateBucketPathMap = generateBucketPathMap();
		boolean isRecursive = Boolean.parseBoolean(this.isRecursive);
		String fileSystem = ConfigUtils.getString(ConfigUtil.MINIO_SYSTEM);

		try {
			if (Utils.hasValidValue(fileSystem) && fileSystem.equalsIgnoreCase(SparkRunnerConstants.AWS)
					|| Utils.hasValidValue(fileSystem) && fileSystem.equalsIgnoreCase(SparkRunnerConstants.MINIO)) {
				getAllFilesFromAWSS3(files, bucketArr, generateBucketPathMap, isRecursive);
			} else {

				logger.error("Invalid File System Configured : {}", fileSystem);

				ProcessorAudit.exceptionLog(SparkRunnerConstants.LIST_HDFS_FILES, this.processorName,
						this.applicationId,
						"Exception in ListFiles", "Invalid File System Configured : " + fileSystem);

			}
		} catch (Exception e) {

			String errorMessage = "Exception Occured While Listing Files From " + this.bucket + ":" + this.path + " - "
					+ e.getMessage();
			ProcessorAudit.exceptionLog(SparkRunnerConstants.LIST_HDFS_FILES, this.processorName, this.applicationId,
					"Exception in ListMINIOCustom: ", errorMessage);
			logger.error(errorMessage, e);
		}

		if (files.isEmpty()) {
			logger.info("No files found in Specified Path(s): {}. Creating an Empty DataFrame.", this.path);
			StructType schema = getSchemaForFileRdd(this.columnName);
			List<Row> defaultRowList = new ArrayList<>();
			defaultRowList.add(RowFactory
					.create("s3a://performance/JOB/RawFiles/JUNIPER/20260619/0900/2057_202506190900.json.zip"));
			this.dataFrame = jobContext.createDataFrame(defaultRowList, defaultRowList.size(), schema);
		} else {
			logger.info("Found {} files in Specified Path(s): {}", files.size(), this.path);
			List<Row> fileList = getListOfRowFromListOfString(files);
			StructType schema = getSchemaForFileRdd(this.columnName);
			this.dataFrame = jobContext.createDataFrame(fileList, files.size(), schema);
		}

		Utils.createOrReplaceTempView(this.tempTable, this.dataFrame, this.processorName);
		return this.dataFrame;
	}

	private void getAllFilesFromAWSS3(List<String> files, String[] bucketArr, Map<String, Set<String>> prefixMap,
			boolean isRecursive) throws Exception {
		for (String bucket : bucketArr) {
			bucket = bucket.trim();
			if (AWSS3Util.isBucketExist(AWSS3Util.getS3Client(), bucket)) {
				if (prefixMap == null) {
					List<String> listOfFiles = AWSS3Util.listMINIO(bucket, isRecursive);
					if (listOfFiles.size() > SparkRunnerConstants.ZERO_INT) {
						files.addAll(listOfFiles);
					}
				} else {
					Set<String> prefixSet = prefixMap.get(bucket);
					for (String prefixpath : prefixSet) {
						List<String> listOfFiles = AWSS3Util.listMINIO(bucket, prefixpath, isRecursive);
						files.addAll(listOfFiles);
					}
				}
			} else {

				String errorMessage = "Folder Doesn't Exist :: " + bucket;
				ProcessorAudit.exceptionLog(SparkRunnerConstants.LIST_HDFS_FILES, this.processorName,
						this.applicationId,
						"Exception in ListMINIOCustom", errorMessage);
				logger.error(errorMessage);
			}

		}
	}

	private Map<String, Set<String>> generateBucketPathMap() throws BusinessException {
		if (this.path != null && this.path.length() > 0) {
			String[] folderPathArr = this.path.split(Symbol.COMMA_STRING);

			Map<String, Set<String>> result = new HashMap<>();
			for (String bucketpath : folderPathArr) {
				String[] bucketpatharr = bucketpath.split(":");
				String bucketname = bucketpatharr[0].trim();
				if (bucketname.length() > 0 && bucketpatharr.length > 1) {
					if (result.containsKey(bucketname)) {
						if (bucketpatharr[1] != null && bucketpatharr[1].trim().length() > 1)
							result.get(bucketname).add(bucketpatharr[1]);
					} else {
						HashSet<String> path = new HashSet<>();
						if (bucketpatharr[1] != null && bucketpatharr[1].trim().length() > 1) {
							path.add(bucketpatharr[1]);
							result.put(bucketname, path);
						}
					}
				} else {
					String errorMessage = "Invalid Path Prefix or Bucket " + this.processorName + " :: Bucket Name : "
							+ bucketname + " Path Prefix : " + bucketpatharr;
					throw new BusinessException(errorMessage);
				}
			}

			return result;
		}
		return null;
	}

	private List<Row> getListOfRowFromListOfString(List<String> files) {
		List<Row> fileList = new ArrayList<>();
		for (String string : files) {
			Row row = RowFactory.create(string);
			fileList.add(row);

		}
		return fileList;
	}

	private StructType getSchemaForFileRdd(String columnName) {
		StructField[] structFields = new StructField[SparkRunnerConstants.ONE_INT];
		structFields[SparkRunnerConstants.ZERO_INT] = DataTypes.createStructField(columnName, DataTypes.StringType,
				false);

		return DataTypes.createStructType(structFields);
	}

	@Override
	public List<String> validProcessorConfigurations() {
		List<String> list = new ArrayList<>();
		if (!Utils.hasValidValue(this.path)) {

			String errorMessage = "Exception in ListMINIOCustom Processor Named " + this.processorName
					+ " : HDFS Folder Path is " + this.path + " That Must Not Be " + this.path;
			list.add(errorMessage);
		}
		return list;
	}
}