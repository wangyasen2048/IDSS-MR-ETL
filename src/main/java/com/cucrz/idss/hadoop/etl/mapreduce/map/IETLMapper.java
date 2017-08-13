package com.cucrz.idss.hadoop.etl.mapreduce.map;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public interface IETLMapper {
	Map<String, Map<String, String>> etlMapSetupDisCache(Context context,
			Map<String, Map<String, String>> storage);

	Map<String, Counter> etlMapSetupGetCounter(Context context);

	String etlMapper(String inputLine, String inFileName, String operId,
			String inputDateID,Context context, Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap,Map<String,String> newChannel,Map<String,String> newCloumn);
//	void outputNewMediaInfo(Context context,Map<String, Map<String, String>> storage);
}
