package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.shanxi;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializerFactory.Config;
import org.dom4j.Attribute;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;

public class shanxiUtil {
	private static Logger log = Logger.getLogger(shanxiUtil.class);
	public static final String VOD_PAGE = "02";
	public static final String VOD_PROGRAM = "03";
	public static final String TEST_DATA_CODE = "001";
	public static final String CACHE_FILE_COMMENT = "ods.txt";
	public static final String CACHE_FILE_COLUMN = "col.txt";

	public static String getNotNullValue(Attribute attribute) throws Exception {
		String value = attribute.getValue().trim();
		if (null == value || "".equals(value)) {
			throw new Exception("末获取必须值！");
		} else {
			return value;
		}
	}

	public static String[] getVODInfo(Map<String, String> newCloumn,Map<String, String> VODInfoList,
			String programInfo,String cloumnInfo,String delim) {
		String vod=programInfo+delim+cloumnInfo;
		String[] vodInfo;
		if (VODInfoList != null) {
			if (VODInfoList.containsKey(programInfo)) {			
				vod=VODInfoList.get(programInfo);
			}else{
				newCloumn.put(programInfo+delim+cloumnInfo,null);
			}
		}else{
			newCloumn.put(programInfo+delim+cloumnInfo,null);
		}
		vodInfo=vod.split(Pattern.quote(delim),5);
		return vodInfo;
	}
	
	public static String[] getTVApp(String url,Map<String, String> newCloumn, 
			Map<String, Map<String, String>> storage,Context context) throws Exception {
		Configuration conf =context.getConfiguration();
		String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
		String[] tail=new String[2];	
		Map<String,String> VODInfoList=storage.get(context.getConfiguration()
				.get("idss_ETL_Cache_VODInfoList"));
		Map<String,String> tvAppList=storage.get(context.getConfiguration()
					.get("idss_ETL_Cache_TVApp"));
		String vodPage = "";
		if(tvAppList!=null){
		Set<String> tvAppSet =tvAppList.keySet();
		for(String s:tvAppSet){
			if(url.indexOf(s)>-1){
				tail[0]=TypeConstans.EVENT_VOD_TELEAPP+OtherConstants.VERTICAL_DELIM+tvAppList.get(s);
				return tail;
			}else{
				String cloumnID = "";
				String pageCount = "0";
				String videoCount = "0";
				String pageID = "";
				String[] cloumnInfo = url.split(OtherConstants.COMMA_DELIM);
				if (cloumnInfo.length > 2) {
					if (url.indexOf("?") <= 0) {
						String tmp = cloumnInfo[0].substring(cloumnInfo[0]
								.indexOf(OtherConstants.FILE_SEPARATOR) + 2);
						pageID = tmp.substring(tmp
								.indexOf(OtherConstants.FILE_SEPARATOR));
					} else {
						int index = url.indexOf("?");
						String tmp = url.substring(
								url.indexOf(OtherConstants.FILE_SEPARATOR) + 2, index);
						pageID = tmp.substring(tmp
								.indexOf(OtherConstants.FILE_SEPARATOR));
					}
					cloumnID = pageID + OtherConstants.COMMA_DELIM + cloumnInfo[1]
							+ OtherConstants.COMMA_DELIM + cloumnInfo[2];
					 String[] cloumn=getVODInfo(newCloumn,VODInfoList, null, cloumnID,delim);
					 vodPage = TypeConstans.EVENT_VOD_PAGE_BIZ
							+ OtherConstants.VERTICAL_DELIM + pageID
							+ OtherConstants.VERTICAL_DELIM + cloumnID
							+ OtherConstants.VERTICAL_DELIM 
							+ OtherConstants.VERTICAL_DELIM + pageCount
							+ OtherConstants.VERTICAL_DELIM + videoCount;
					 tail[0]=vodPage;
				}
			}
		}
		return tail;
		}else{
		throw new Exception("电视应用对照表不存在！！无法转换！");
		}
		
	}

	public static String[] getVODEvent(String url, String sourceVal,
			Map<String, Map<String, String>> storage, Map<String, String> newCloumn,
			Map<String, String> VODInfoList,Context context) throws Exception {
		Configuration conf =context.getConfiguration();
		String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
		String vodPro = "";
		String[] tail=new String[2];	
		String[] urlParams = url.substring(url.lastIndexOf("?") + 1).split("&");
		Map<String, String> map = tranMap(urlParams);
		String contentid = getValue(map, "contentid");// 影片ID
		String assetName = getValue(map, "assetName");// 影片名称
		String cloumnID = "";
		String category = getValue(map, "category");
		String videoCount = "0";
		String pageID = "";
		String[] cloumnInfo = sourceVal.split(OtherConstants.COMMA_DELIM);
		if (cloumnInfo.length > 2) {
			if (sourceVal.indexOf("?") <= 0) {
				String tmp = cloumnInfo[0].substring(cloumnInfo[0]
						.indexOf(OtherConstants.FILE_SEPARATOR) + 2);
				pageID = tmp.substring(tmp
						.indexOf(OtherConstants.FILE_SEPARATOR));
			} else {
				int index = sourceVal.indexOf("?");
				String tmp = sourceVal.substring(
						sourceVal.indexOf(OtherConstants.FILE_SEPARATOR) + 2, index);
				pageID = tmp.substring(tmp
						.indexOf(OtherConstants.FILE_SEPARATOR));
			}
			cloumnID = pageID + OtherConstants.COMMA_DELIM + cloumnInfo[1]
					+ OtherConstants.COMMA_DELIM + cloumnInfo[2];
			 String[] cloumn=getVODInfo(newCloumn,VODInfoList, contentid, assetName+delim+cloumnID,delim);
			 vodPro = TypeConstans.EVENT_VOD_PROGRAM_BIZ
						+ OtherConstants.VERTICAL_DELIM + contentid
						+ OtherConstants.VERTICAL_DELIM + assetName
						+ OtherConstants.VERTICAL_DELIM + videoCount
						+ OtherConstants.VERTICAL_DELIM + "0|1|" + cloumnID
						+ OtherConstants.VERTICAL_DELIM+category;
			 tail[0]=vodPro;
		}	
		return tail;
	}

	public static String getValue(Map<String, String> map, String name)
			throws Exception {
		String value = "";
		try {
			value = map.get(name);
			if (null == value)
				value = "";
		} catch (Exception e) {
			value = "";
		}
		return value;
	}

	public static Map<String, String> tranMap(String[] urlParams) {
		Map<String, String> map = new HashMap<String, String>();
		for (String s : urlParams) {
			StringTokenizer str = new StringTokenizer(s, "=");
			map.put(str.nextToken(), str.nextToken());
		}
		return map;
	}

	public static void main(String[] args) {
		String url = "http://10.43.38.9/common/authorSuccess.jsp?stbno=B8BA680AA37D,ipip";
		boolean i = url.contains("/common/");
		System.out.println(i);
	}
}
