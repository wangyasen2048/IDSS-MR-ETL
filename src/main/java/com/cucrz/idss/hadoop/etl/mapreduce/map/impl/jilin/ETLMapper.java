package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.jilin;

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
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper {
	private static Configuration conf = null;
	private static Map<String, String> channelName = null;
	private static Map<String, String> channelChannel = null;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;
	private static Logger log =Logger.getLogger(ETLMapper.class);
	private static String region="";
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
		channelName = storage.get(conf
				.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelChannel = storage.get(conf
				.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Counter mapParamError = counterMap.get("mapParamError");
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		String record = "";
		String[] split = inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
		if (split.length > 3) {
			if (split[0].length() == 9) {
				String bizType = split[3];
				 region = split[1];
				String date = "";
				String time = "";
				try {
					date = DateUtil.DATE_FORMATER.format(Long
							.parseLong(split[2]) * 1000);
					time = DateUtil.TIME_FORMATER.format(Long
							.parseLong(split[2]) * 1000);
				} catch (Exception e) {
					Log.warn("日期格式不正确，记录丢弃");
					return "";
				}
				String head = split[0] + OtherConstants.VERTICAL_DELIM
						+ region + OtherConstants.TAB_DELIM + date + " "
						+ time + OtherConstants.VERTICAL_DELIM + bizType
						+ OtherConstants.VERTICAL_DELIM;
				String[] bizSplit = { " " };
				if (split.length > 4) {
					bizSplit = split[4].split(OtherConstants.EXCLAMATION_DELIM_REGEX);
				}
				if (DateCheck.dateCheckRule(date, inputDateID)) {
					String para = getBizParameter(bizType, bizSplit);
					if (para != "") {
						record = head + para;
					} else {
						head = split[0] + OtherConstants.VERTICAL_DELIM
								+ region + OtherConstants.TAB_DELIM + date
								+ " " + time + OtherConstants.VERTICAL_DELIM
								+ TypeConstans.HEART_BEAT
								+ OtherConstants.VERTICAL_DELIM;
						record = head + "Z";
						mapParamError.increment(1);
					}
				} else {
					mapDateIdError.increment(1);
					return record;
				}
			} else {
				return record;
			}
		} else {
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

	public String getBizParameter(String bizType, String[] bizSplit) {
		String other = "";
		// String[] split=other.split(OtherConstants.VERTICAL_DELIM_REGEX);
		// 直播
		if (bizType.equals(TypeConstans.EVENT_LIVE__DATA_BIZ)
				|| bizType.equals(TypeConstans.EVENT_LIVE_BIZ)
				|| bizType.equals(TypeConstans.EVENT_LIVE_DEFAULTTV)
				|| bizType.equals(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ)) {
			if (bizSplit.length >=7) {
				other = bizSplit[0]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[1]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[2]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[3]
						+ OtherConstants.VERTICAL_DELIM
						+ "1"
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[5]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[6]
						+ OtherConstants.VERTICAL_DELIM
						+ ReadCacheUtil.getUniqueChannel(newChannel,
								channelName, channelChannel, bizSplit[0]+"#"
										+ bizSplit[1]+"#" + bizSplit[2],
								bizSplit[6]);
			}

		}
		// 回看节目
		else if (bizType.equals(TypeConstans.EVENT_LOOKBACK_PRO_BIZ)) {
			if (bizSplit.length == 7) {
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
						+ "1"
						+OtherConstants.VERTICAL_DELIM
						+ ReadCacheUtil.getUniqueChannel(newChannel,
								channelName, channelChannel, "", bizSplit[2]);
				} catch (ParseException e) {
					e.printStackTrace();
					return "";
				}
			}
		}
		// 回看页面
		else if (bizType.equals(TypeConstans.EVENT_LOOKBACK_PAGE_BIZ)) {
			if (bizSplit.length == 3) {
			other = bizSplit[0] + OtherConstants.VERTICAL_DELIM + bizSplit[1]
					+ OtherConstants.VERTICAL_DELIM + bizSplit[2];
			}
		}
		// VOD节目
		else if (bizType.equals(TypeConstans.EVENT_VOD_PROGRAM_BIZ)) {
			if (bizSplit.length == 7) {
				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[3] + OtherConstants.VERTICAL_DELIM
						+ "1" + OtherConstants.VERTICAL_DELIM
						+ bizSplit[5] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[6];
				String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
				ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList, bizSplit[0]
						+ delim + bizSplit[1], bizSplit[5] + delim + bizSplit[6],delim,region);

			}
		}
		// VOD页面
		else if (bizType.equals(TypeConstans.EVENT_VOD_PAGE_BIZ)) {
			if (bizSplit.length == 5) {
				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[3] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[4];
			}
		}
		// 时移
		else if (bizType.equals(TypeConstans.EVENT_TIMESHIFT_BIZ)) {
			if (bizSplit.length == 7) {
				other = bizSplit[0]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[1]
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[2]
						+ OtherConstants.VERTICAL_DELIM
						+ DateUtil.DATE_TIME_FORMATER.format(Long
								.parseLong(bizSplit[3]) * 1000)
						+ OtherConstants.VERTICAL_DELIM
						+ "1"
						+ OtherConstants.VERTICAL_DELIM
						+ bizSplit[5]
						+ OtherConstants.VERTICAL_DELIM
						+ ReadCacheUtil.getUniqueChannel(newChannel,
								channelName, channelChannel, bizSplit[0]+"#"
										+ bizSplit[1] +"#"+ bizSplit[2],
								bizSplit[5]);
			}
		}
		// 电视应用
		else if (bizType.equals(TypeConstans.EVENT_VOD_TELEAPP)) {
			other = bizSplit[0];
		}
		// 状态类
		else if (bizType.equals(TypeConstans.STATUS_AUDIENCE_INFORMATION)
				|| bizType.equals(TypeConstans.STATUS_COMMENT_INFORMATION)
				|| bizType.equals(TypeConstans.STATUS_CHANNEL_INFORMATION_BAR)
				|| bizType.equals(TypeConstans.STATUS_CHANNEL_LIST)
				|| bizType.equals(TypeConstans.STATUS_EXCEPTION)
				|| bizType.equals(TypeConstans.STATUS_HOBBY_LIST)
				|| bizType.equals(TypeConstans.STATUS_HOT_RECOMMEND_LIST)
				|| bizType.equals(TypeConstans.STATUS_MUTIPLE_VIEWS)
				|| bizType.equals(TypeConstans.STATUS_PERSONAL_RECOMMEND_LIST)
				|| bizType.equals(TypeConstans.STATUS_PROGRAM_GUIDE)
				|| bizType.equals(TypeConstans.STATUS_QUESTIONNAIRE_PAGE)
				|| bizType.equals(TypeConstans.STATUS_SCHEDULING_PROGRAM)
				|| bizType.equals(TypeConstans.STATUS_VOICE_BAR)) {
			other = bizSplit[0];
		}
		// 广告
		else if (bizType.equals(TypeConstans.EVENT_AFFECT_ADS)
				|| bizType.equals(TypeConstans.EVENT_NOT_AFFECT_ADS)) {
			if (bizSplit.length == 5) {
				other = bizSplit[0] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[1] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[2] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[3] + OtherConstants.VERTICAL_DELIM
						+ bizSplit[4];
			}
		}
		// 开关机
		else if (bizType.equals(TypeConstans.OFF_CODE)
				|| bizType.equals(TypeConstans.ON_CODE)) {
			other = bizSplit[0];
		}
		// 心跳
		else if (bizType.equals(TypeConstans.HEART_BEAT)) {
			other = bizSplit[0];
		}
		// 未知业务
		else if (bizType.equals(TypeConstans.OTHER_UNDEFINED)) {
			other = bizSplit[0];
		}
		return other;
	}

}
