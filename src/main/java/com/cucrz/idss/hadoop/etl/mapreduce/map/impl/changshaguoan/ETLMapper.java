package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.changshaguoan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper {
	private static Configuration conf = null;
	private static Map<String, String> channelNameList = null;
	private static Map<String, String> channelCodeList = null;
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
	public String etlMapper(String inputLine, String inFileName, String operId,
			String inputDateID, Context context,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap, Map<String, String> newChannel,
			Map<String, String> newCloumn) {
		this.newChannel = newChannel;
		this.newCloumn = newCloumn;
		conf = context.getConfiguration();
		VODInfoList = storage.get(conf.get("idss_ETL_Cache_VODInfoList"));
		channelNameList = storage.get(conf
				.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelCodeList = storage.get(conf
				.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Counter mapFmtError = counterMap.get("mapFmtError");
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		String record = "";
		String[] splits = inputLine.split(OtherConstants.COMMA_DELIM);
		if (splits.length > 14) {
			if (!splits[2].equals("") && !splits[3].equals("")) {
				if (splits[9].trim().equals("TV")) {
					record = getLiveEvent(splits, mapDateIdError);
				} else if (splits[9].trim().equals("APP")) {
					record = getAPPEVent(splits, mapDateIdError);
				} else {
					mapFmtError.increment(1);
				}
			} else {
				mapFmtError.increment(1);
			}
		} else {
			mapFmtError.increment(1);
		}
		return record;
	}

	public static String getLiveEvent(String[] splits, Counter mapDateIdError) {
		String date = splits[3].substring(splits[3].indexOf("_") + 1);
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
			Date date1 = format.parse(date);
			date = DateUtil.DATE_TIME_FORMATER.format(date1);
		} catch (ParseException e) {
			e.printStackTrace();
			mapDateIdError.increment(1);
			return "";
		}
		String uniqChannel = ReadCacheUtil.getUniqueChannel(newChannel,
				channelNameList, channelCodeList, "#" + splits[6] + "#"
						+ splits[7], splits[10]);
		StringBuffer tmp = new StringBuffer();
		tmp.append(splits[2]).append(OtherConstants.VERTICAL_DELIM)
				.append(TypeConstans.default_Changsha)
				.append(OtherConstants.TAB_DELIM).append(date)
				.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_LIVE_BIZ)
				.append(OtherConstants.VERTICAL_DELIM)
				.append(OtherConstants.VERTICAL_DELIM).append(splits[6])
				.append(OtherConstants.VERTICAL_DELIM).append(splits[7])
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM).append("0")
				.append(OtherConstants.VERTICAL_DELIM)
				.append("#" + splits[6] + "#" + splits[7])
				.append(OtherConstants.VERTICAL_DELIM).append(splits[10])
				.append(OtherConstants.VERTICAL_DELIM).append(uniqChannel);
		return tmp.toString();
	}

	public static String getAPPEVent(String[] splits, Counter mapDateIdError) {
		String date = splits[3].substring(splits[3].indexOf("_") + 1);
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
			Date date1 = format.parse(date);
			date = DateUtil.DATE_TIME_FORMATER.format(date1);
		} catch (ParseException e) {
			e.printStackTrace();
			mapDateIdError.increment(1);
			return "";
		}
		StringBuffer tmp = new StringBuffer();
		tmp.append(splits[2]).append(OtherConstants.VERTICAL_DELIM)
				.append(TypeConstans.default_Changsha)
				.append(OtherConstants.TAB_DELIM).append(date)
				.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_VOD_TELEAPP)
				.append(OtherConstants.VERTICAL_DELIM).append(splits[14])
				.append(OtherConstants.VERTICAL_DELIM).append(splits[4]);
		return tmp.toString();
	}
}
