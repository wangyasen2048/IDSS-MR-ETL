package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.hebei;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper {
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
		map.put("TotleRows", mapTotleRows);
		map.put("mapOutputRows", mapOutputRows);
		map.put("mapFmtError", mapFmtError);
		map.put("mapDateIdError", mapDateIdError);
		return map;
	}

	@Override
	public String etlMapper(String inputLine, 
			                String inFilename,
			                String operId,
			                String inputDateID,
			                Context context,
			                Map<String, Map<String, String>> storage,
			                Map<String, Counter> counterMap, 
			                Map<String, String> newChannel,
			                Map<String, String> newCloumn) {
		this.newChannel = newChannel;
		this.newCloumn = newCloumn;
		
		conf = context.getConfiguration();
		VODInfoList = storage.get(conf.get("idss_ETL_Cache_VODInfoList"));
		channelName = storage.get(conf
				.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelChannel = storage.get(conf
				.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Counter mapFmtError = counterMap.get("mapFmtError");
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		
		String record = "";
		String[] split = inputLine.split(OtherConstants.COMMA_DELIM);
		String head="";
		String date="";
		String time="";
		if(split.length>4){			
			String userId=split[4];
			date=split[2]; 
		    if (DateCheck.dateCheckRule(date, inputDateID) && (!userId.equals(""))) {
				time=split[3];
				String bizType = split[1];
				head =  userId + OtherConstants.VERTICAL_DELIM +TypeConstans.default_Hebei
				        + OtherConstants.TAB_DELIM + date +" "+ time + OtherConstants.VERTICAL_DELIM;
				String para=getBizParameter(bizType);	
				if (!para.equals("")) {
					record = head + para;
				} else {
					return "";
				}
		    } else {
				mapDateIdError.increment(1);
				return "";
		    }	
		}
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
	
	public String getBizParameter(String bizType){
		String other="";
		if(bizType.length()==8){
			if(bizType.equals("00010000")){
				other=TypeConstans.ON_CODE;
			}else if(bizType.equals("00020000")){
				other=TypeConstans.EVENT_VOD_PAGE_BIZ;
			}else if(bizType.equals("00030000")){
				other=TypeConstans.EVENT_VOD_PROGRAM_BIZ+"|||0|0|1|0|||||";
			}else if(bizType.equals("00040000")){
				other=TypeConstans.EVENT_TIMESHIFT_BIZ;       //  回看/时移  
			}else if(bizType.equals("00090000")){
				other=TypeConstans.OFF_CODE;
			}else if(bizType.equals("FFF10000")){
				other=TypeConstans.STATUS_CHANNEL_LIST;
			}else if(bizType.equals("FFF80000")){
				other=TypeConstans.HEART_BEAT;				
			}else{
				String tsid=bizType.substring(0, 4);
				String sid=bizType.substring(4, 8);
				if(tsid.equals("0000")){                   //0000XXXX 双向页面数据 
					other=TypeConstans.EVENT_VOD_TELEAPP;	
				}else if(sid.substring(0, 2).equals("FF")){
					other="1808";	       //RAM+CPU 
				}else{   //直播+直播心跳
					other=TypeConstans.EVENT_LIVE_BIZ
							+ OtherConstants.VERTICAL_DELIM
							+ "|"+ tsid
							+ OtherConstants.VERTICAL_DELIM
							+ sid
							+ "|0|0|"+bizType+"||"
							+ ReadCacheUtil.getUniqueChannel(newChannel,channelName, channelChannel,"##"+Integer.toString(Integer.parseInt(sid, 16)), "");	
					
									
				}
			}
		 }else if(bizType.length()==12){
			String index=bizType.substring(0, 4);
			String tsid=bizType.substring(4, 8);
			String sid=bizType.substring(8, 12);
			if(index.equals("FFF4")){
				other=TypeConstans.STATUS_CHANNEL_LIST;	//导航条->频道列表	
			}else if(index.equals("FFF5")){
				other=TypeConstans.STATUS_VOICE_BAR;   //声音条
			}
			
		}else if(bizType.equals("FFF2")&& bizType.length()==4){
			other="1535";      //搜台操作
		}
		return other;		
	}

}
