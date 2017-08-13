package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.hunanyouxian;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper {
	private static Configuration conf;
	private static Map<String, String> channelNameList;
	private static Map<String, String> channelCodeList;
	private static Map<String, String> regionCodeList;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;

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
				Log.warn("缓存文件格式或名称有误，请检查！" + cacheFileName);
				e.printStackTrace();
			}
		}
		return resultMap;
	}

	@Override
	public Map<String, Counter> etlMapSetupGetCounter(Context context) {
		Map<String, Counter> map = new HashMap<String, Counter>();
		Counter mapTotleRows = (Counter) context.getCounter("map", "TotleRows");
		Counter mapFmtError = (Counter) context
				.getCounter("map", "mapFmtError");
		Counter mapDateIdError = (Counter) context.getCounter("map",
				"mapDateIdError");
		Counter mapOutputRows = (Counter) context.getCounter("map",
				"mapOutputRows");
		map.put("TotleRows", mapTotleRows);
		map.put("mapOutputRows", mapOutputRows);
		map.put("mapFmtError", mapFmtError);
		map.put("mapDateIdError", mapDateIdError);
		return map;
	}

	@Override
	public String etlMapper(String inputLine, String inFileName, String operId,
			String inputDateID, Context context,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap, Map<String, String> newChannel,
			Map<String, String> newCloumn) {
		this.newChannel = newChannel;
		this.newCloumn = newCloumn;
		conf = context.getConfiguration();
		VODInfoList = storage.get(conf.get("idss_ETL_Cache_VODInfoList"));
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		Counter mapFmtError = counterMap.get("mapFmtError");
		regionCodeList = storage.get(conf
				.get("idss_ETL_Cache_UniqueRegionList"));
		channelNameList = storage.get(conf
				.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelCodeList = storage.get(conf
				.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		String[] split = inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
		if (split.length > 5) {
			if (split[2] != null || !split[2].equals("")) {
				String date = "";
				String time = "";
				try {
					date = DateUtil.DATE_FORMATER.format(DateUtil.DATEFORMATER
							.parse(split[1]));
					time = DateUtil.TIME_FORMATER.format(DateUtil.TIME_FORMATER
							.parse(split[5]));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				if (split[4] != null || !split[4].equals("")) {
					String region = ReadCacheUtil.getUniqueRegion(
							regionCodeList, split[4],
							TypeConstans.default_Hunandianxin);
					String head = split[2] + OtherConstants.VERTICAL_DELIM
							+ region + OtherConstants.TAB_DELIM + date + " "
							+ time + OtherConstants.VERTICAL_DELIM;
					String other = convertRecord(split);
					if (other != null) {
						return head + other;
					}
				}
			}
		}
		mapFmtError.increment(1);
		return "";
	}

	public static String convertRecord(String[] split) {
		if (split[0].equals("1")) {
			return getLiveEvent(split);
		} else if (split[0].equals("2")) {
			return getNVODEvent(split);
		} else if (split[0].equals("3")) {
			return getTimeShiftEvent(split);
		} else if (split[0].equals("4")) {
			return getLookBackEvent(split);
		} else if (split[0].equals("5")) {
			return getVODPageEvent(split);
		} else if (split[0].equals("6")) {
			return getVODProgramEvent(split);
		} else if (split[0].equals("7")) {
			return getADEvent(split);
		} else if(split[0].equals("0")){
			return getExceptionEvent(split);
		}else if(split[0].equals("8")){
			return getSearchEvent(split);
		}else{
			return null;
		}
	}

	public static String getLiveEvent(String[] split) {
		if(split.length>12){
		String uniqChannel = ReadCacheUtil.getUniqueChannel(newChannel,
				channelNameList, channelCodeList, "#" + split[10] + "#"
						+ split[9], split[11]);
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_LIVE_BIZ)
				.append(OtherConstants.VERTICAL_DELIM).append("")
				.append(OtherConstants.VERTICAL_DELIM).append(split[9])
				.append(OtherConstants.VERTICAL_DELIM).append(split[8])
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append("1")
				.append(OtherConstants.VERTICAL_DELIM).append(split[12])
				.append(OtherConstants.VERTICAL_DELIM).append(split[11])
				.append(OtherConstants.VERTICAL_DELIM).append(uniqChannel);
		return tmp.toString();
		}else{
			return null;
		}
	}

	public static String getTimeShiftEvent(String[] split) {
		if(split.length>12){
		String uniqChannel = ReadCacheUtil.getUniqueChannel(newChannel,
				channelNameList, channelCodeList, "#" + split[10] + "#"
						+ split[9], split[11]);
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_TIMESHIFT_BIZ)
				.append(OtherConstants.VERTICAL_DELIM).append("")
				.append(OtherConstants.VERTICAL_DELIM).append(split[9])
				.append(OtherConstants.VERTICAL_DELIM).append(split[8])
				.append(OtherConstants.VERTICAL_DELIM).append(split[12])
				.append(OtherConstants.VERTICAL_DELIM).append("1")
				.append(OtherConstants.VERTICAL_DELIM).append(uniqChannel);
		return tmp.toString();
		}else{
			return null;
		}
	}

	public static String getLookBackEvent(String[] split) {
		if(split.length>11){
		String uniqChannel = ReadCacheUtil.getUniqueChannel(newChannel,
				channelNameList, channelCodeList, "", split[9]);
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_LOOKBACK_PRO_BIZ)
				.append(OtherConstants.VERTICAL_DELIM).append(split[10])
				.append(OtherConstants.VERTICAL_DELIM).append(split[11])
				.append(OtherConstants.VERTICAL_DELIM).append(split[9])
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append("1")
				.append(OtherConstants.VERTICAL_DELIM).append(uniqChannel);
		return tmp.toString();
		}else{
			return null;
		}
	}

	public static String getVODPageEvent(String[] split) {
		if(split.length>11){
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_VOD_PAGE_BIZ)
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM).append(split[10])
				.append(OtherConstants.VERTICAL_DELIM).append(split[11])
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append("0");
		return tmp.toString();
		}else{
			return null;
		}
	}

	public static String getVODProgramEvent(String[] split) {
		if(split.length>14){
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_VOD_PROGRAM_BIZ)
				.append(OtherConstants.VERTICAL_DELIM).append(split[12])
				.append(OtherConstants.VERTICAL_DELIM).append(split[13])
				.append(OtherConstants.VERTICAL_DELIM).append(split[14])
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append("1")
				.append(OtherConstants.VERTICAL_DELIM).append(split[10])
				.append(OtherConstants.VERTICAL_DELIM).append(split[11])
				.append(OtherConstants.VERTICAL_DELIM).append(split[9])
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM);
		return tmp.toString();
		}else{
			return null;
		}
	}

	public static String getADEvent(String[] split) {
		if(split.length>8){
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_AFFECT_ADS)
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append(split[8])
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM).append("0");
		return tmp.toString();
		}else{
			return null;
		}
	}

	public static String getExceptionEvent(String[] split) {
		if(split.length>8){
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.STATUS_EXCEPTION)
				.append(OtherConstants.VERTICAL_DELIM).append(split[7])
				.append(OtherConstants.VERTICAL_DELIM).append(split[7]);
		return tmp.toString();
		}else{
			return null;
		}
	}
	public static String getNVODEvent(String[] split){
		if(split.length>11){
		StringBuilder tmp = new StringBuilder();
		tmp.append(TypeConstans.EVENT_UNUSE)
		.append(OtherConstants.VERTICAL_DELIM).append(split[9])
		.append(OtherConstants.VERTICAL_DELIM).append(split[10])
		.append(OtherConstants.VERTICAL_DELIM).append(split[11]);
		return tmp.toString();
		}else{
			return null;
		}
	}
	public static String getSearchEvent(String[] split){
		if(split.length>5){
			StringBuilder tmp = new StringBuilder();
			tmp.append(TypeConstans.EVENT_UNUSE)
			.append(OtherConstants.VERTICAL_DELIM);
			return tmp.toString();
			}else{
				return null;
			}
	}
}
