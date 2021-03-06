package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.fujian;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.job.baseETL.DataEtlJob;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;
/**
 * 
 * @author 叶振河 
 * @createTime: 2015年4月24日 下午5:40:53
 */
public class ETLMapper implements IETLMapper {
	private static Logger log = Logger.getLogger(ETLMapper.class);
	private static Configuration conf = null;
	private static Map<String, String> channelName = null;
	private static Map<String, String> channelChannel = null;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;

	// mapper的计数器
	@Override
	public Map<String, Counter> etlMapSetupGetCounter(Context context) {
		Map<String, Counter> map = new HashMap<String, Counter>();
		Counter mapTotleRows = (Counter) context.getCounter("map", "TotleRows");
		Counter mapOutputRows = (Counter) context.getCounter("map",
				"mapOutputRows");
		Counter mapFmtError = (Counter) context
				.getCounter("map", "mapFmtError");
		Counter mapDateIdError = (Counter) context.getCounter("map",
				"mapDateIdError");
		Counter mapParamError = (Counter) context.getCounter("map",
				"mapParamError");
		map.put("TotleRows", mapTotleRows);
		map.put("mapOutputRows", mapOutputRows);
		map.put("mapFmtError", mapFmtError);
		map.put("mapDateIdError", mapDateIdError);
		map.put("mapParamError", mapParamError);
		
		log.info("福建 MAP SETUP");
		return map;
	}

	@Override
	public String etlMapper(String inputLine, String inFilename, String operId,
			String inputDateID, Context context,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap, Map<String, String> newChannel,
			Map<String, String> newCloumn) {
		Log.info("福建 MAP ETL");
		Counter mapParamError = counterMap.get("mapParamError");
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		String record = "";
		EventChange ec = new EventChange();
		record = ec.parseEvent(inputLine, inputDateID);
		return record;
	}

	@Override
	public Map<String, Map<String, String>> etlMapSetupDisCache(
			Context context, Map<String, Map<String, String>> storage) {
		Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
		Map<String, String> cacheFileNameMap = context.getConfiguration()
				.getValByRegex("^idss_ETL_Cache");
		Set<String> cacheFileNameSet = cacheFileNameMap.keySet();
		for (String cacheFileName : cacheFileNameSet) {
			cacheFileName = cacheFileNameMap.get(cacheFileName);
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						cacheFileName));
				resultMap.put(cacheFileName,
						ReadCacheUtil.readCache(reader, cacheFileName));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return resultMap;
	}
	
	public static void main(String[] args) {
		ETLMapper etlMap = new ETLMapper();
		Map<String, Map<String, String>> storage = new HashMap<String, Map<String,String>>();
		Map<String, String> map1 = new HashMap<String, String>();
		map1.put("1", "2");
		storage.put("ss", map1);
		
		String line = "onid|networkId|TSID|frequency|serviceId|serviceType|serviceName|channelNum|CAStatus|videoPID|audioPID|audioChannel|audioLevel|mode|status|operatorKey";
		etlMap.etlMapper(line, "filename", "1520", "20150101", null, null, null, null, null);
		
	}

}
