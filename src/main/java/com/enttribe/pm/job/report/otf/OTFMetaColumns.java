package com.enttribe.pm.job.report.otf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.lang.StringUtils;

import com.enttribe.sparkrunner.context.JobContext;
import com.enttribe.sparkrunner.udf.AbstractUDF;

import scala.collection.JavaConverters;

public class OTFMetaColumns implements UDF2<scala.collection.immutable.Map<String, String>, String, Row>, AbstractUDF {

	private static final String DOUBLE_HASH = Symbol.HASH_STRING + Symbol.HASH_STRING;
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(OTFMetaColumns.class);
	// private JobContext jobcontext;
    public JobContext jobcontext;
	@Override
	public Row call(scala.collection.immutable.Map<String, String> geoDetailConcated, String moHeirachy)
			throws Exception {
		Map<String, String> metaInfo = new HashMap<>(JavaConverters.mapAsJavaMap(geoDetailConcated));
		metaInfo.put("moHeirachy", moHeirachy);
		updateMetaInfoMap(metaInfo);


        // /*
        //  * only for testing
        //  */
        // jobcontext.getParameters().forEach((key, value) -> {
		// 	// logger.info("key :{} value :{} ", key, value);
		// 	com.enttribe.pm.job.report.otf.App.jobContext.setParameters(key, value);
		// });

		logger.info("Meta Info : {}", metaInfo);
		return RowFactory.create(metaInfo);
	}

	// public static void main(String[] args) throws ParseException {
	// 	String timeKey = "200828081500";
	// 	SimpleDateFormat formate = new SimpleDateFormat("yyMMddHHmmss");
	// 	SimpleDateFormat requiredDateFormate = new SimpleDateFormat("dd/MM/yyyy");
	// 	SimpleDateFormat requiredHourFormate = new SimpleDateFormat("HH:mm");
	// 	Date date = formate.parse(timeKey);
	// 	System.out.println(date);
	// 	System.out.println(requiredDateFormate.format(date));
	// 	System.out.println(requiredHourFormate.format(date));
	// }

	// private static String getTimeForQuaterly(String timeKey) {
	// 	Integer min = Integer.valueOf(StringUtils.substring(timeKey, 8, 10));
	// 	String hour = StringUtils.substring(timeKey, 6, 8);
	// 	if (min < 15) {
	// 		return hour + "00";
	// 	} else if (min >= 15 && min < 30) {
	// 		return hour + "15";
	// 	} else if (min >= 30 && min < 45) {
	// 		return hour + "30";
	// 	} else if (min >= 45 && min < 60) {
	// 		return hour + "45";
	// 	}
	// 	return null;
	// }

	private Map<String, String> updateMetaInfoMap(Map<String, String> metaInfo) {
		// String parameter = jobcontext.getParameter("GeographyL0");
		// metaInfo.put("DL0", parameter);
		// metaInfo.put("L0", parameter);
		metaInfo.put("DL0", "INDIA");
		metaInfo.put("L0", "INDIA");
		return metaInfo;
	}

	// private Map<String, String> getHbaseColumnMap(String[] geoDetails) {
	// 	Map<String, String> hbasecolumnsMap = new HashMap<>();
	// 	for (String geography : geoDetails) {
	// 		String geoKey = StringUtils.substringBefore(geography, Symbol.AT_STRING);
	// 		String geoValue = StringUtils.substringAfter(geography, Symbol.AT_STRING);
	// 		if (!geoValue.equalsIgnoreCase("null"))
	// 			hbasecolumnsMap.put(geoKey, geoValue);
	// 	}
	// 	return hbasecolumnsMap;
	// }

	@Override
	public String getName() {
		return "CreateMetaData";
	}

	@Override
	public DataType getReturnType() {
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("metaData",
				DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
		return DataTypes.createStructType(fields);
	}

}