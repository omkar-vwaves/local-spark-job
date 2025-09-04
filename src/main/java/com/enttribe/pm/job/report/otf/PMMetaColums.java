package com.enttribe.pm.job.report.otf; 

import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.lang.StringUtils;
import com.enttribe.sparkrunner.udf.AbstractUDF;

 
public class PMMetaColums implements UDF1<String,Row>,AbstractUDF{

	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(PMMetaColums.class);

	@Override
	public Row call(String geoDetailConcated) throws Exception {
		String[] geoDetails = geoDetailConcated.split(Symbol.HASH_STRING + Symbol.HASH_STRING);
		Map<String, String> metaInfo = getHbaseColumnMap(geoDetails);
		return RowFactory.create(metaInfo);
	}

	private Map<String, String> getHbaseColumnMap(String[] geoDetails) {
		Map<String, String> hbasecolumnsMap = new HashMap<>();
		for (String geography : geoDetails) {
			String geoKey = substringBefore(geography, Symbol.AT_STRING);
			String geoValue = substringAfter(geography, Symbol.AT_STRING);
			if (!StringUtils.isEmpty(geoValue)) {
				if(geoValue=="T5G"||geoValue=="T2G"||geoValue=="T3G")
                    geoValue="T"+geoValue;
				hbasecolumnsMap.put(geoKey, geoValue);
			}
		}
		return hbasecolumnsMap;
	}

	@Override
	public String getName() {
		return "PMMetaColums";
	}

	@Override
	public DataType getReturnType() {
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("metaData",
				DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true));
		return DataTypes.createStructType(fields);
	}

}