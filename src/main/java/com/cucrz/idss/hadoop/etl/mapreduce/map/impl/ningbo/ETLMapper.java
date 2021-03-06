package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.ningbo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
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
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

/**
 * 宁波ETL Mapper
 * @author qianzhiqin
 * @creatTime 2015年5月18日 
 */
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
	public String etlMapper(String inputLine, String inFilename, String operId,
			String inputDateID, Context context,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap, Map<String, String> newChannel,
			Map<String, String> newCloumn) {
		this.newChannel = newChannel;
		this.newCloumn = newCloumn;
		conf = context.getConfiguration();
		VODInfoList = storage.get(conf.get("idss_ETL_Cache_VODInfoList"));
		channelName = storage.get(conf.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelChannel = storage.get(conf.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Counter mapFmtError = counterMap.get("mapFmtError");
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		String record = "";
		try {
			String[] split = inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
			
			if (split.length >= 4) {
				String date = "";
				String compTime = "";
				try {
					String dateStr = split[2]+"000";
					date = DateUtil.getDateFormMsec(dateStr, DateUtil.DATE_TIME_FORMATER);
					compTime =  DateUtil.getFormatDateStr(date, DateUtil.DATE_TIME_FORMATER, DateUtil.DATEFORMATER);
				} catch (Exception e) {
					return record;
				}
				if(!inputDateID.equals(compTime)){
					return "";
				}
				record = split[0] + OtherConstants.VERTICAL_DELIM + TypeConstans.default_Ningbo + OtherConstants.TAB_DELIM + date + OtherConstants.VERTICAL_DELIM ;
				String type = split[3];
				String[] typeSplit = {};
				if(split.length > 4){
					typeSplit = split[4].split(OtherConstants.EXCLAMATION_DELIM);
				}
				String result = parseType(type,typeSplit);
				if (result != null || result.length() != 0) {
					record += result;
				}else{
					return "";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return record;
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

			//本地调试
//			String localFileName = "E:/conf/"+cacheFileName;
			
			try {
				BufferedReader reader = new BufferedReader(new FileReader(cacheFileName));
				resultMap.put(cacheFileName,ReadCacheUtil.readCache(reader, cacheFileName));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return resultMap;
	}
	
	/**
	 * 类型转换
	 * @param spilt
	 * @return
	 */
	public String parseType(String type,String[] spilt ){
		StringBuffer result = new StringBuffer();
		//开机
		if("1".equals(type)){
			result.append(TypeConstans.OFF_CODE + OtherConstants.VERTICAL_DELIM + "1");
		}else if("257".equals(type)){
			//数字电视
//			Network_ID
//			TS_ID
//			Service_ID
//			直播频道换台方式
//			直播频道播放状态
//			逻辑频道号
//			频道名称
//			当前统一频道编号
			String channelId = "";
			if(spilt.length > 3){
				channelId = spilt[2];
			}
//			channelName.get(channelId);
			String channelQG = channelName.get(channelId)==null?"":channelName.get(channelId);
			result.append(TypeConstans.EVENT_LIVE_BIZ + OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM);
			result.append("0" + OtherConstants.VERTICAL_DELIM + "0" + OtherConstants.VERTICAL_DELIM );
			result.append(channelId + OtherConstants.VERTICAL_DELIM  + OtherConstants.VERTICAL_DELIM );
			result.append(channelQG);
		}else {
			//其他的都转为心跳
			result.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM + "1");
		}
		return result.toString();
	}
	
	public String getBizParameter(String bizType, String[] bizSplit) {
		String other = "";
		// String[] split=other.split(OtherConstants.VERTICAL_DELIM_REGEX);
		if (bizType.equals(TypeConstans.EVENT_LIVE__DATA_BIZ)
				|| bizType.equals(TypeConstans.EVENT_LIVE_BIZ)
				|| bizType.equals(TypeConstans.EVENT_LIVE_DEFAULTTV)
				|| bizType.equals(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ)) {
			if (bizSplit.length > 2) {
				other = " | | |0|"
						+ bizSplit[1]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[2]
						+ OtherConstants.VERTICAL_DELIM
						+ " |"
						+ ReadCacheUtil.getUniqueChannel(newChannel,
								channelName, channelChannel, bizSplit[0], "");

			}
		} else if (bizType.equals(TypeConstans.EVENT_VOD_TELEAPP)) {
			if (bizSplit.length > 0) {
				other = bizSplit[0];
			}
		} else if (bizType.equals(TypeConstans.EVENT_LOOKBACK_PRO_BIZ)) {
			if (bizSplit.length > 6) {
				try {
					other = bizSplit[0]
							+ OtherConstants.VERTICAL_DELIM
							+ bizSplit[1]
							+ OtherConstants.VERTICAL_DELIM
							+ bizSplit[2]
							+ OtherConstants.VERTICAL_DELIM
							+ DateUtil.DATE_FORMATER
									.format(DateUtil.DATEFORMATER
											.parse(bizSplit[3]))
							+ OtherConstants.VERTICAL_DELIM
							+ DateUtil.TIME_FORMATER
									.format(DateUtil.TIMEFORMATER
											.parse(bizSplit[4]))
							+ OtherConstants.VERTICAL_DELIM
							+ bizSplit[5]
							+ OtherConstants.VERTICAL_DELIM
							+ bizSplit[6]
							+ OtherConstants.VERTICAL_DELIM
							+ ReadCacheUtil.getUniqueChannel(newChannel,
									channelName, channelChannel, "",
									bizSplit[2]);
				} catch (ParseException e) {
					e.printStackTrace();
					return "";
				}
			}
		} else if (bizType.equals(TypeConstans.EVENT_LOOKBACK_PAGE_BIZ)) {
			if (bizSplit.length > 2) {
				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2];
			}
		} else if (bizType.equals(TypeConstans.EVENT_VOD_PROGRAM_BIZ)) {
			if (bizSplit.length > 6) {

				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[3] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[4] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[5] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[6];
				String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
				ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList, bizSplit[0]
						+ delim + bizSplit[1], bizSplit[5] + delim + bizSplit[6],delim,TypeConstans.default_Ningbo);
			}
		} else if (bizType.equals(TypeConstans.EVENT_VOD_PAGE_BIZ)) {
			if (bizSplit.length > 4) {
				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[3] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[4];
			} else if (bizSplit.length > 2) {
				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2] + OtherConstants.VERTICAL_DELIM
						+ OtherConstants.VERTICAL_DELIM;
			}
		} else if (bizType.equals(TypeConstans.EVENT_TIMESHIFT_BIZ)) {
			if (bizSplit.length > 2) {
				other = " | | |"
						+ DateUtil.DATE_TIME_FORMATER.format(Long
								.parseLong(bizSplit[1]) * 1000)
						+ OtherConstants.VERTICAL_DELIM + bizSplit[2]
						+ OtherConstants.VERTICAL_DELIM + " |" + bizSplit[0];
			}
		} else if (bizType.equals(TypeConstans.OFF_CODE)
				|| bizType.equals(TypeConstans.ON_CODE)) {
			if (bizSplit.length > 0) {
				other = bizSplit[0];
			}
		} else if (bizType.equals(TypeConstans.OTHER_UNDEFINED)) {
			if (bizSplit.length > 0) {
				other = bizSplit[0];
			}
		}
		return other;
	}

}
