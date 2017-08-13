package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.dongfang;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.dom4j.Document;
import org.mortbay.log.Log;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.FormCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper{
	private static Configuration conf=null;
	private static Map<String,String> channelName=null;
	private static Map<String,String> channelChannel=null;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;
	@Override
	public Map<String, Map<String, String>> etlMapSetupDisCache(
			Context context, Map<String, Map<String, String>> storage) {
		Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
		Map<String,String> cacheFileNameMap=context.getConfiguration().getValByRegex("^idss_ETL_Cache");
		Set<String> cacheFileNameSet=cacheFileNameMap.keySet();
		for(String cacheFileName:cacheFileNameSet){
			cacheFileName=cacheFileNameMap.get(cacheFileName);
		try {
		BufferedReader reader = new BufferedReader(new FileReader(cacheFileName));
			resultMap.put(cacheFileName, ReadCacheUtil.readCache(reader, cacheFileName));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		return resultMap;
		}

	@Override
	public Map<String, Counter> etlMapSetupGetCounter(Context context) {
		Map<String, Counter> map = new HashMap<String, Counter>();
		Counter mapTotleRows = (Counter) context.getCounter("map", "TotleRows");
		Counter mapOutputRows = (Counter) context.getCounter("map", "mapOutputRows");
		Counter mapFmtError = (Counter) context.getCounter("map", "mapFmtError");
		Counter mapDateIdError = (Counter) context.getCounter("map", "mapDateIdError");
		map.put("TotleRows", mapTotleRows);
		map.put("mapOutputRows", mapOutputRows);
		map.put("mapFmtError", mapFmtError);
		map.put("mapDateIdError", mapDateIdError);
		return map;
	}

	@Override
	public String etlMapper(String inputLine, String inFilename, String operId,
			String inputDateID,Context context, Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap,Map<String, String> newChannel,
			Map<String, String> newCloumn) {
		this.newChannel = newChannel;
		this.newCloumn = newCloumn;
		conf=context.getConfiguration();
		VODInfoList = storage.get(conf.get("idss_ETL_Cache_VODInfoList"));
		channelName=storage.get(conf.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelChannel=storage.get(conf.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Counter mapFmtError =counterMap.get("mapFmtError");
		Counter mapDateIdError =counterMap.get("mapDateIdError");
		String record="";
		if(!inputLine.startsWith("SQL>")){
			String[] split=inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
			if(split.length>4){
			String date="";
			try{
			date=split[5].substring(0,10);
			}catch (Exception e){
	 			Log.warn("日期格式不正确，记录丢弃");
	 			return "";
	 		}
			String head=split[0]+OtherConstants.VERTICAL_DELIM+TypeConstans.default_Dongfang
					+OtherConstants.TAB_DELIM+split[5]+OtherConstants.VERTICAL_DELIM
					+TypeConstans.EVENT_VOD_PROGRAM_BIZ+OtherConstants.VERTICAL_DELIM;
			if(DateCheck.dateCheckRule(date, inputDateID)){
				int index=split[4].indexOf(".");
			String proID = split[4].substring(0,index);
			if(proID.startsWith("AD")){
				
				record=split[0]+OtherConstants.VERTICAL_DELIM+TypeConstans.default_Dongfang
						+OtherConstants.TAB_DELIM+split[5]+OtherConstants.VERTICAL_DELIM
						+TypeConstans.EVENT_AFFECT_ADS+OtherConstants.VERTICAL_DELIM
						+proID+OtherConstants.VERTICAL_DELIM;
			}else{
			record=head+proID+OtherConstants.VERTICAL_DELIM+OtherConstants.VERTICAL_DELIM+"0|0|1|0|";
			String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
			ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList, proID
					+ delim+"",""+delim+"",delim,"090100");
			}
			}else{
				mapDateIdError.increment(1);
				return "";
			}
		}
			}else{
				return "";
			}
			return record;
	}


	
}
