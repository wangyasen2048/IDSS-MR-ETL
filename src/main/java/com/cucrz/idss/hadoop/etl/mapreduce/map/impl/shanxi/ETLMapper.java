package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.shanxi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
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

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLMapper implements IETLMapper {
	private static Logger log = Logger.getLogger(ETLMapper.class);
	private static Configuration conf = null;
	private static Map<String, String> channelNameList = null;
	private static Map<String, String> tvappList = null;
	private static Map<String, String> channelChannelList = null;
	private static Map<String, String> uniqueRegionList = null;
	private static Map<String, String> VODInfoList = null;
	private static Map<String, String> newChannel = null;
	private static Map<String, String> newCloumn = null;
	private static Counter mapDateIdError = null;

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
	public String etlMapper(String inputLine, String inFilename, String operId,
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
		tvappList = storage.get(conf
				.get("idss_ETL_Cache_TVApp"));
		channelChannelList = storage.get(conf
				.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		uniqueRegionList = storage.get(conf
				.get("idss_ETL_Cache_UniqueRegionList"));
		mapDateIdError = counterMap.get("mapDateIdError");
		String record = "";
		if (inputLine.trim().indexOf("dt=") > 0) {
			record = getVODRecord(inputLine, operId, inputDateID, storage,
					counterMap,context);
		} else if (inputLine.trim().indexOf("GHApp") > 0) {
			record = getLiveRecord(inputLine, operId, inputDateID, storage,
					counterMap);
		} else {
			return record;
		}
		return record;
	}

	public String getLiveRecord(String xml, String operId, String inputDateID,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap) {
		String record = "";
		StringBuffer tmp = new StringBuffer();
		Document doc;
		try {
			doc = DocumentHelper.parseText(xml);

			Element GHAppElmt = doc.getRootElement();
			Element WICElmt = GHAppElmt.element("WIC");
			Element AElmt = WICElmt.element("A");
			try {
				String caArea = shanxiUtil.getNotNullValue(WICElmt
						.attribute("caArea"));// CA卡区域
				if (caArea.equals("001")) {
					return "";
				}
				String cardNum = shanxiUtil.getNotNullValue(WICElmt
						.attribute("cardNum"));// 用户ID
				String stbNum = shanxiUtil.getNotNullValue(WICElmt
						.attribute("stbNum"));// 机顶盒编号
				String regionId = shanxiUtil.getNotNullValue(WICElmt
						.attribute("regionId"));// 区域ID
				String date = shanxiUtil.getNotNullValue(WICElmt
						.attribute("date"));// 数据日期
				String s = shanxiUtil.getNotNullValue(AElmt.attribute("s"));// 开始时间
				String e = shanxiUtil.getNotNullValue(AElmt.attribute("e"));// 结束时间
				String n = AElmt.attribute("n").getValue();// 网络编号
				String t = AElmt.attribute("t").getValue();// 服务编号
				String sn = shanxiUtil.getNotNullValue(AElmt.attribute("sn"));// 频道名称

				if (DateCheck.dateCheckRule(date, inputDateID)) {
					String commonChannel = ReadCacheUtil.getUniqueChannel(
							newChannel, channelChannelList, tvappList,
							"##", sn);
//					regionId = regionId.substring(0, 3);
//					regionId = ReadCacheUtil.getUniqueRegion(uniqueRegionList,
//							regionId,TypeConstans.default_Shanxi);
					tmp.append(cardNum).append(OtherConstants.VERTICAL_DELIM)
							.append(regionId).append(OtherConstants.TAB_DELIM)
							.append(date + " " + s)
							.append(OtherConstants.VERTICAL_DELIM)
							.append(TypeConstans.EVENT_LIVE_BIZ)
							.append(OtherConstants.VERTICAL_DELIM).append(n)
							.append(OtherConstants.VERTICAL_DELIM)
							.append(OtherConstants.VERTICAL_DELIM).append(t)
							.append(OtherConstants.VERTICAL_DELIM)
							.append("0|0|0|").append(sn)
							.append(OtherConstants.VERTICAL_DELIM)
							.append(commonChannel);
				} else {
					mapDateIdError.increment(1);
					return record;
				}

			} catch (Exception e) {
				log.warn("记录不正确");
			}
		} catch (DocumentException e1) {
			e1.printStackTrace();
		}
		record = tmp.toString();
		return record;
	}

	public String getVODRecord(String xml, String operId, String inputDateID,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap,Context context) {
		String record = "";
		StringBuffer tmp = new StringBuffer();
		Document doc;
		String[] xmls = xml.split("<e");
		for (int i = 1; i < xmls.length; i++) {
			String s = "<e" + xmls[i].replaceAll("&", "&amp;");
			try {
				doc = DocumentHelper.parseText(s);

				Element EElmt = doc.getRootElement();
				// 解析

				String dtVal = "";
				String stbVal = "";
				String cidVal = "";
				String stmVal = "";
				String etmVal = "";
				String dayVal = "";
				String ridVal = "";
				String urlVal = "";
				String pidVal = "";
				String sourceVal = "";

				try {
					String cridVal = EElmt.attribute("crid").getValue();//
					String sidVal = EElmt.attribute("sid").getValue();// 服务编号
					String tsidVal = EElmt.attribute("tsid").getValue();//
					String ipVal = EElmt.attribute("ip").getValue();// ip地址

					dtVal = shanxiUtil.getNotNullValue(EElmt.attribute("dt"));// 数据类型
					stbVal = EElmt.attribute("stb").getValue();// 机顶盒ID
					cidVal = shanxiUtil.getNotNullValue(EElmt.attribute("cid"));// 用户ID
					String snVal = EElmt.attribute("sn").getValue();// 频道名称
					stmVal = shanxiUtil.getNotNullValue(EElmt.attribute("stm"));// 开始时间
					etmVal = EElmt.attribute("etm").getValue();// 结束时间
					dayVal = "20"+ shanxiUtil.getNotNullValue(EElmt.attribute("day"));// 日期
					ridVal = shanxiUtil.getNotNullValue(EElmt.attribute("rid"));// 区域ID
					urlVal = EElmt.attribute("url").getValue();// 播出地址
					pidVal = EElmt.attribute("pid").getValue();// 产品ID
					sourceVal = shanxiUtil.VOD_PROGRAM.equals(dtVal) ? EElmt
							.attribute("source").getValue() : "";// 源地址
//					ridVal = ridVal.substring(0, 3);
//					ridVal = ReadCacheUtil.getUniqueRegion(uniqueRegionList,
//							ridVal,TypeConstans.default_Shanxi);
					if (DateCheck.dateCheckRule(dayVal, inputDateID)) {
						String head = cidVal + OtherConstants.VERTICAL_DELIM
								+ ridVal + OtherConstants.TAB_DELIM + dayVal
								+ " " + stmVal + OtherConstants.VERTICAL_DELIM;
						String[] tail = new String[2];
						if (!dtVal.equals(shanxiUtil.VOD_PROGRAM)) {
							tail = shanxiUtil.getTVApp(urlVal,newCloumn,
									storage,context);
						} else {
							tail = shanxiUtil.getVODEvent(urlVal,
									sourceVal, storage,newCloumn,VODInfoList,context);
						}
						for(String t:tail){
						if (t!=null&&!t.equals("")) {
							tmp.append(head + t).append(OtherConstants.ENTER_DELIM);
						}
						}
					} else {
						mapDateIdError.increment(1);
						tmp.append("").append(OtherConstants.ENTER_DELIM);
					}
				} catch (Exception e) {
					log.warn("记录不正确，为空！");
					e.printStackTrace();
				}
			} catch (DocumentException e) {
				log.warn("");
				e.printStackTrace();
			}
		}
		record = tmp.toString();
		return record;
	}

}
