package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.sichuan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.mortbay.log.Log;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

/**
 * 四川ETL Mapper
 * @author qianzhiqin
 * 
 * @creatTime 2015年5月12日 
 */
@SuppressWarnings("all")
public class ETLMapper implements IETLMapper {
	private static Logger log = Logger.getLogger(ETLMapper.class);
	private static Configuration conf;
	private static Map<String, String> channelName;
	private static Map<String, String> channelChannel;
	private static Map<String, String> regionCodeList;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;

	
	/**
	 * 生成 {@link Counter }的计数器，可对数据进行统计
	 */
	@Override
	public Map<String, Counter> etlMapSetupGetCounter(Context context) {
		Map<String, Counter> map = new HashMap<String, Counter>();
		Counter mapTotleRows = (Counter) context.getCounter("map", "TotleRows");
		Counter mapFmtError = (Counter) context.getCounter("map", "mapFmtError");
		Counter mapDateIdError = (Counter) context.getCounter("map","mapDateIdError");
		Counter mapOutputRows = (Counter) context.getCounter("map","mapOutputRows");
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
		Counter mapDateIdError = counterMap.get("mapDateIdError");
		this.newChannel = newChannel;
		this.newCloumn = newCloumn;
		conf = context.getConfiguration();
		VODInfoList = storage.get(conf.get("idss_ETL_Cache_VODInfoList"));
		channelName = storage.get(conf.get("idss_ETL_Cache_NameToUniqueChannel"));
		channelChannel = storage.get(conf.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		regionCodeList=storage.get(conf.get("idss_ETL_Cache_UniqueRegionList"));
		String result = "";
		String[] spilt = inputLine.split(OtherConstants.COMMA_DELIM);
		if(spilt.length == 8){
			String date;
			String time;
			try {
				date = DateUtil.getFormatDateStr(spilt[2], DateUtil.DATE_FORMATER, DateUtil.DATEFORMATER);
				time = DateUtil.getFormatDateStr(spilt[2]+" "+spilt[3], DateUtil.DATE_TIME_FORMATER, DateUtil.DATE_TIME_FORMATER);
			} catch (ParseException e) {
				return result;
			}
			if(inputDateID.equals(date)){
				String area = spilt[7];
				String areaCode = regionCodeList.get(area);
				if(areaCode==null||areaCode.length()==0){
//					log.info("area error !!");
					return result;
				}
//				try {
//					areaCode = RegionCode.valueOf("area"+area.substring(0,2)).getCode();
//				} catch (Exception e) {
//					areaCode = TypeConstans.default_Sichuan; 
//				}
				result = spilt[0] + OtherConstants.VERTICAL_DELIM + areaCode + OtherConstants.TAB_DELIM + time + OtherConstants.VERTICAL_DELIM;
				result += parseActionType(spilt[1]);
			}
		}
		return result;
	}
	
	@Override
	public Map<String, Map<String, String>> etlMapSetupDisCache(Context context, Map<String, Map<String, String>> storage) {
		Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
		Map<String, String> cacheFileNameMap = context.getConfiguration().getValByRegex("^idss_ETL_Cache");
		Set<String> cacheFileNameSet = cacheFileNameMap.keySet();
		
		for (String cacheFileName : cacheFileNameSet) {
			cacheFileName = cacheFileNameMap.get(cacheFileName);
			
			//本地调试
//			String localFileName = "E:/conf/"+cacheFileName;
			
			try {
				BufferedReader reader = new BufferedReader(new FileReader(cacheFileName));
				resultMap.put(cacheFileName,ReadCacheUtil.readCache(reader, cacheFileName));
			} catch (Exception e) {
				Log.warn("缓存文件格式或名称有误，请检查！");
				e.printStackTrace();
			}
		}
		return resultMap;
	}
	
	public static String parseActionType(String actionType){
//		00090000，开机/唤醒机顶盒
//		FFF10000，打开频道列表
//		F0F10000，关闭频道列表
//		FFF30000，打开主菜单
//		F0F30000，关闭主菜单
//		FFF80000，离开直播状态
//
//		FFF4+SID，直播导航条     如FFF495e6  是在频道95e6呼出导航条 
//		FFF5+SID，音量条         如FFF504b0  是在SID=04b0频道呼出音量条
//		FFE5+SID，收藏某频道     如FFE503eb  是对SID=03eb频道进行收藏动作
//		FFE6+SID，预定某频道     如FFE6025c  是对SID=025c频道进行预定动作
//
//		TSID+SID，直播状态  非以上状态为直播信息 【简单判断则为非00090000且非Fxxxxxxx】 
//		0014953e  TSID=0014  SID=953e 通过频点表确定频道
//		00391645  TSID=0039  SID=1645  
		
		StringBuffer result = new StringBuffer();
		if("00090000".equals(actionType)){
			result.append(TypeConstans.ON_CODE + OtherConstants.VERTICAL_DELIM + "1");
		}else if("FFF10000".equals(actionType)){
			result.append(TypeConstans.STATUS_CHANNEL_LIST );
		}else if("F0F10000".equals(actionType)){
			//退出列表 >> 心跳
			result.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
		}else if("FFF30000".equals(actionType)){
			//打开主菜单 >> 图文应用
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}else if("F0F30000".equals(actionType)){
			//关闭主菜单 >> 心跳
			result.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
		}else if("FFF80000".equals(actionType)){
			//离开直播状态 >> 0xFFFF	未知业务
			result.append(TypeConstans.OTHER_UNDEFINED );
		}else if(actionType.startsWith("FFF4")){
			//FFF4+SID，直播导航条     如FFF495e6  是在频道95e6呼出导航条 
			// >> 电子节目指南EPG
			result.append(TypeConstans.STATUS_PROGRAM_GUIDE );
			//			String channelId = actionType.replace("FFF4", "");
		}else if(actionType.startsWith("FFF5")){
			//FFF5+SID，音量条         如FFF504b0  是在SID=04b0频道呼出音量条
			result.append(TypeConstans.STATUS_VOICE_BAR);
		}else if(actionType.startsWith("FFE5")){
			//FFE5+SID，收藏某频道     如FFE503eb  是对SID=03eb频道进行收藏动作
			// >> 喜爱频道
			//			频道编号
			//			频道名称
			//			频道ID
			//			网络ID
			//			传输流ID
			//			状态  1为增加，0为删除
			String channelId = actionType.replace("FFE5", "");
			result.append(TypeConstans.STATUS_LIKE_CHANNEL + OtherConstants.VERTICAL_DELIM );
			result.append( channelId + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM );
			result.append( channelId + OtherConstants.VERTICAL_DELIM +"0"+ OtherConstants.VERTICAL_DELIM );
			result.append( "0"+OtherConstants.VERTICAL_DELIM + "1" );
		}else if(actionType.startsWith("FFE6")){
			//FFE6+SID，预定某频道     如FFE6025c  是对SID=025c频道进行预定动作
			// >> 预订节目  0x0222
			//			频道编号
			//			频道名称
			//			频道ID
			//			网络ID
			//			传输流ID
			//			节目名称
			String channelId = actionType.replace("FFE6", "");
			result.append(TypeConstans.STATUS_RESERVE_PROGRAM + OtherConstants.VERTICAL_DELIM );
			result.append( channelId + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM );
			result.append( channelId + OtherConstants.VERTICAL_DELIM + "0" +OtherConstants.VERTICAL_DELIM );
			result.append( "0" + OtherConstants.VERTICAL_DELIM + "1" );
		}else if(!actionType.startsWith("F") && !"00090000".equals(actionType) ){
			//数字电视
			String tsid = actionType.substring(0,4);
			String sid = actionType.substring(4,8);
			String channelname = channelName.get(actionType)==null?"":channelName.get(actionType);
			//			Network_ID
			//			TS_ID
			//			Service_ID
			//			直播频道换台方式
			//			直播频道播放状态
			//			逻辑频道号
			//			频道名称
			//			当前统一频道编号
			result.append(TypeConstans.EVENT_LIVE_BIZ + OtherConstants.VERTICAL_DELIM);
			result.append( OtherConstants.VERTICAL_DELIM +tsid + OtherConstants.VERTICAL_DELIM + sid + OtherConstants.VERTICAL_DELIM);
			result.append("0"+ OtherConstants.VERTICAL_DELIM + "0"+ OtherConstants.VERTICAL_DELIM + actionType + OtherConstants.VERTICAL_DELIM + channelname + OtherConstants.VERTICAL_DELIM + channelname  );
		}
		return result.toString();
	}
}
