package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.tianjinliantong;

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

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper {
	private static Configuration conf = null;
	private static Map<String, String> channelNameList = null;
	private static Map<String, String> channelCodeList= null;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;
	private static Logger log =Logger.getLogger(ETLMapper.class);
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
//		Counter mapParamError = counterMap.get("mapParamError");
//		Counter mapDateIdError = counterMap.get("mapDateIdError");
		String record = "";
//		String[] split = inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
		int fileIndex = inFileName.lastIndexOf(OtherConstants.FILE_SEPARATOR);
		String fileName = inFileName.substring(fileIndex + 1);
		try {
		if(fileName.contains("zhongxing_")){
			record=zhongxingData(fileName,inputLine);
		}else if(fileName.contains("huawei_")){
			record=huaweiData(fileName,inputLine);
		}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
				e.printStackTrace();
			}
		}
		return resultMap;
	}

	public String zhongxingData(String fileName,String inputLine) throws ParseException{
		String record="";
		StringBuilder tmp= new StringBuilder();
		String[] splits=inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
		if(fileName.equals("zhongxing_vod.txt")){
			if(splits.length>3){
				record = tmp.append(splits[0]).append(OtherConstants.VERTICAL_DELIM)
						.append("120000").append(OtherConstants.TAB_DELIM)
						.append(DateUtil.getDateTimeStr(DateUtil.DATE_TIME_FULL.parse(splits[1])))
						.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_VOD_PROGRAM_BIZ)
						.append(OtherConstants.VERTICAL_DELIM).append(splits[3])
						.append(OtherConstants.VERTICAL_DELIM)
						.append(OtherConstants.VERTICAL_DELIM).append("0")
				        .append(OtherConstants.VERTICAL_DELIM).append("0")
				        .append(OtherConstants.VERTICAL_DELIM).append("1")
				        .append(OtherConstants.VERTICAL_DELIM).append("0")
				        .append(OtherConstants.VERTICAL_DELIM)
				        .append(OtherConstants.VERTICAL_DELIM)
				        .toString();
				ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList,splits[3]
						+ delim , delim,delim,"120000");
			}
		}else if(fileName.equals("zhongxing_live.txt")){
			record = tmp.append(splits[0]).append(OtherConstants.VERTICAL_DELIM)
					.append("120000").append(OtherConstants.TAB_DELIM)
					.append(DateUtil.getDateTimeStr(DateUtil.DATE_TIME_FULL.parse(splits[1])))
					.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_LIVE_BIZ)
					.append(OtherConstants.VERTICAL_DELIM)
					.append(OtherConstants.VERTICAL_DELIM)
					.append(OtherConstants.VERTICAL_DELIM).append(splits[3])
			        .append(OtherConstants.VERTICAL_DELIM).append("0")
			        .append(OtherConstants.VERTICAL_DELIM).append("1")
			        .append(OtherConstants.VERTICAL_DELIM).append(splits[3])
			        .append(OtherConstants.VERTICAL_DELIM)
			        .append(OtherConstants.VERTICAL_DELIM)
			        .append(ReadCacheUtil.getUniqueChannel(newChannel, channelNameList, channelCodeList, "##"+splits[3], null))
			        .toString();
		}else if(fileName.equals("zhongxing_teleapp.txt")){
			record = tmp.append(splits[0]).append(OtherConstants.VERTICAL_DELIM)
					.append("120000").append(OtherConstants.TAB_DELIM)
					.append(DateUtil.getDateTimeStr(DateUtil.DATE_TIME_FULL.parse(splits[1])))
					.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_VOD_TELEAPP)
					.append(OtherConstants.VERTICAL_DELIM)
					.append(OtherConstants.VERTICAL_DELIM).append(splits[3])
					.append(OtherConstants.VERTICAL_DELIM)
			        .toString();
		}
		return record;
	}
	public String huaweiData(String fileName,String inputLine) throws ParseException{
		String record="";
		StringBuilder tmp= new StringBuilder();
		String[] splits=inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
		if(fileName.equals("huawei_Contentview.txt")){
			if(splits.length>3){
				if(splits[3].equals("1")){
					record = tmp.append(splits[0]).append(OtherConstants.VERTICAL_DELIM)
							.append("120000").append(OtherConstants.TAB_DELIM)
							.append(DateUtil.getDateTimeStr(DateUtil.DATE_TIME_FULL.parse(splits[1])))
							.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_VOD_PROGRAM_BIZ)
							.append(OtherConstants.VERTICAL_DELIM).append(splits[6])
							.append(OtherConstants.VERTICAL_DELIM).append(splits[5])
							.append(OtherConstants.VERTICAL_DELIM).append("0")
					        .append(OtherConstants.VERTICAL_DELIM).append("0")
					        .append(OtherConstants.VERTICAL_DELIM).append("1")
					        .append(OtherConstants.VERTICAL_DELIM).append("0")
					        .append(OtherConstants.VERTICAL_DELIM)
					        .append(OtherConstants.VERTICAL_DELIM)
					        .toString();
					ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList,splits[6] + delim+splits[5] , delim,delim,"120000");
				}else if(splits[3].equals("2")){
					record = tmp.append(splits[0]).append(OtherConstants.VERTICAL_DELIM)
							.append("120000").append(OtherConstants.TAB_DELIM)
							.append(DateUtil.getDateTimeStr(DateUtil.DATE_TIME_FULL.parse(splits[1])))
							.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_LIVE_BIZ)
							.append(OtherConstants.VERTICAL_DELIM)
							.append(OtherConstants.VERTICAL_DELIM)
							.append(OtherConstants.VERTICAL_DELIM).append(splits[4])
					        .append(OtherConstants.VERTICAL_DELIM).append("0")
					        .append(OtherConstants.VERTICAL_DELIM).append("1")
					        .append(OtherConstants.VERTICAL_DELIM).append(splits[4])
					        .append(OtherConstants.VERTICAL_DELIM).append(splits[5])
					        .append(OtherConstants.VERTICAL_DELIM)
					        .append(ReadCacheUtil.getUniqueChannel(newChannel, channelNameList, channelCodeList, "##"+splits[4], splits[5]))
					        .toString();
				}else if(splits[3].equals("3")){
					record = tmp.append(splits[0]).append(OtherConstants.VERTICAL_DELIM)
							.append("120000").append(OtherConstants.TAB_DELIM)
							.append(DateUtil.getDateTimeStr(DateUtil.DATE_TIME_FULL.parse(splits[1])))
							.append(OtherConstants.VERTICAL_DELIM).append(TypeConstans.EVENT_LOOKBACK_PRO_BIZ)
							.append(OtherConstants.VERTICAL_DELIM).append(splits[6])
							.append(OtherConstants.VERTICAL_DELIM)
							.append(OtherConstants.VERTICAL_DELIM).append(splits[5])
							.append(OtherConstants.VERTICAL_DELIM)
							.append(OtherConstants.VERTICAL_DELIM)
					        .append(OtherConstants.VERTICAL_DELIM).append("0")
					        .append(OtherConstants.VERTICAL_DELIM).append("1")
					        .append(OtherConstants.VERTICAL_DELIM)
					        .append(ReadCacheUtil.getUniqueChannel(newChannel, channelNameList, channelCodeList, "##"+splits[4], splits[5]))
					        .toString();
				}				
			}
		}else if(fileName.equals("huawei_Userunfo.txt")){
			
		}else if(fileName.equals("huawei_Orderlog.txt")){
			
		}
		return record;
	}
	Object o;


}
