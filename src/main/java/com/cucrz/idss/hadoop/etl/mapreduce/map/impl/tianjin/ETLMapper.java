package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.tianjin;

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
	private static SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddhhmmss");
	private static SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
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
		inputLine = inputLine.replaceAll("&", "&amp;");
		if (!"".equals(inputLine)) {
			result = ETLMapper.parseXlm(inputLine,inputDateID);
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
	/**
	 * 将inputLine 转化为xml，然后进行actionType转换
	 * @param line
	 * @return
	 * @throws Exception
	 */
	public static String parseXlm(String line,String inputDate) {
		Document doc = null;
		EVBean evbean = new EVBean();
		String header = "";
		String result = "";
		try {
			doc = DocumentHelper.parseText(line);
		} catch (DocumentException e) {
//			e.printStackTrace();
			return result;
		}
		Element up = doc.getRootElement().element("UP");
		String stbid = up.attributeValue("STBID");
		String caid = up.attributeValue("CAID");
		String areacode = up.attributeValue("AREACODE");
		String date = up.attributeValue("DATE");
//		String userid = up.attributeValue("USERID");
//		System.out.println(up.attributeValue("STBID"));
		areacode = ReadCacheUtil.getUniqueRegion(regionCodeList,caid ,TypeConstans.default_Tianjin);
		header = caid + OtherConstants.VERTICAL_DELIM + areacode + OtherConstants.TAB_DELIM;
		
		if(inputDate.equals(date)){
			
			Iterator<Element> evs = up.elementIterator();
			while (evs.hasNext()) {
				Element ev = evs.next();
				evbean.setE(ev.attributeValue("E"));
				evbean.setE1(ev.attributeValue("E1").replaceAll("&", "&amp;"));
				evbean.setE2(ev.attributeValue("E2"));
				evbean.setE3(ev.attributeValue("E3"));
				evbean.setE4(ev.attributeValue("E4"));
				evbean.setE5(ev.attributeValue("E5"));
				evbean.setD1(ev.attributeValue("D1"));
				evbean.setD2(ev.attributeValue("D2"));
				evbean.setD3(ev.attributeValue("D3"));
				evbean.setT(ev.attributeValue("T"));
				String time = "";
				try {
					time = DateUtil.getFormatDateStr((date+evbean.getE()),DateUtil.DATE_TIME_FULL,DateUtil.DATE_TIME_FORMATER);
				} catch (ParseException e) {
					continue;
				}
				if(date.length()==8){
					if(evbean.getActionType(date,channelName) == null){
						return result;
					}else{
						result += header+time+OtherConstants.VERTICAL_DELIM +evbean.getActionType(date,channelName)+OtherConstants.ENTER_DELIM;
					}
				}
		}
	
		}
		return result;
	}

}
