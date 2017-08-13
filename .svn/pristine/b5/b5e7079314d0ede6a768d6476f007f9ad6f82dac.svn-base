package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.fujian;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class EventChange {
	private static Logger log = Logger.getLogger(EventChange.class);

	public String parseEvent(String line, String inputDateID) {
		try {
			line = line.replaceAll("\r\n", "");

			if (line == null || line.length() == 0)
				return null;

			EventBean obj = new EventBean();
			obj.setSource(line);

			// base 基础信息
			int index = line.indexOf("<|>");
			String base = line;
			String param = "";
			if (index > -1) {
				base = line.substring(0, line.indexOf("<|>"));
				param = line.substring(line.indexOf("<|>") + 3);
			}

			base = base.replace("<", "");
			base = base.replace(">", "");
			base = base.replace("[", "");
			base = base.replace("]", "");
			base = base.replace("?", "");
			String[] baseArray = base.split(",");
			//校验时间，日期不是当天的则直接丢弃
			String lineDate = DateUtil.DATEFORMATER.format(Long.valueOf(baseArray[1]));
			if(!lineDate.equals(inputDateID)){
				log.info("line date=" + lineDate);
				return null;
			}
			
			// String area = getArea.getMap().get(baseArray[3]);
			/*String area = "";
			if (area == null || area.length() == 0)
				area = "350100";*/			
			String area = "350000";
			if("1".equals(baseArray[3]))//福州
				area = "350100";
			else if("2".equals(baseArray[3]))//南平
				area = "350700";
			else if("3".equals(baseArray[3]))//宁德
				area = "350900";
			else if("4".equals(baseArray[3]))//莆田
				area = "350300";
			else if("5".equals(baseArray[3]))//三明
				area = "350400";
			else if("6".equals(baseArray[3]))//漳州
				area = "350600";
			else if("7".equals(baseArray[3]))//龙岩
				area = "350800";
			else if("8".equals(baseArray[3]))//泉州
				area = "350500";
			else if("9".equals(baseArray[3]))//厦门
				area = "350200";
			else if("10".equals(baseArray[3]))//平潭
				area = "350128";
			else if("20".equals(baseArray[3]))//福州8县
				area = "350100";

			EventBean bean = chanageEvent(baseArray[0], param);
			log.info(bean.getErroCode());
			if (bean.getType() == 0)
				return null;

			obj.setType(bean.getType());
			obj.setLtime(Long.valueOf(baseArray[1]) / 1000);
			obj.setUser(baseArray[2]);

			obj.setArea(area);
			obj.setContent(bean.getContent());
			obj.setRzcontent(bean.getRzcontent());

			log.debug("line:" + obj.getSource());
			log.debug("sudp:" + obj.getFromContent());
			log.debug("save:" + obj.getFromRzcontent());
			return obj.getFromMRcontent();
		} catch (Exception e) {
			log.error(line);
			log.error(e.toString());
		}
		return null;
	}

	private EventBean chanageEvent(String type, String content) {
		EventBean result = new EventBean();

		if (type == null || type.length() == 0)
			return result;

		Map<String, String> map = getContent(content);

		if (type.equals("0101")) {// 直播播放
			result = change0101(type, map);
		} else if (type.equals("0102")) {// 时移播放
			result = change0102(type, map);
		} else if (type.equals("0103")) {// 回看播放
			result = change0103(type, map);
		} else if (type.equals("0104")) {// 点播播放
			result = change0104(type, map);
		} else if (type.equals("0105")) {// 点播应用
			result = change0105(type, map);
		} else if (type.equals("0106")) {// 回看应用
			result = change0106(type, map);
		} else if (type.equals("0107")) {// NVOD应用
			result = change0107(type, map);
		} else if (type.equals("0108")) {// NVOD播放
			result = change0108(type, map);
		} else if (type.equals("0121")) {// 广告
			result = change0121(type, map);
		} else if (type.equals("0122")) {// 立即上报广告
			result = change0121(type, map);
		} else if (type.equals("0131")) {// 应用
			result = change0131(type, map);
		} else if (type.equals("0132")) {// URL业务
			result = change0132(type, map);
		} else if (type.equals("0133")) {// APP场景
			result = change0133(type, map);
		} else if (type.equals("0301")) {// 开机
			result = change0301(type, map);
		} else if (type.equals("0302")) {// 关机
			result = change0302(type, map);
		} else if (type.equals("0309")) {// 机顶盒故障
			result = change0309(type, map);
		} else if (type.equals("0701")) {// 心跳
			result = change0701(type, map);
		} else {
			result = change0701(type, map);//其余类型全部转换为心跳
		} 
		return result;
	}

	private static Map<String, String> getContent(String content) {
		HashMap<String, String> map = new HashMap<String, String>();

		if (null == content || content.length() == 0)
			return map;

		String temp = content;
		String[] array = temp.split("<&>");
		if (null == array || array.length == 0)
			return map;

		for (int i = 0; i < array.length; i++) {
			String item = array[i];
			item = item.replace("<", "");
			item = item.replace(">", "");
			item = item.replace("(", "");
			item = item.replace(")", "");
			String key = item.substring(0, item.indexOf(","));
			String value = item.substring(item.indexOf(",") + 1);

			map.put(key, value);
		}

		return map;
	}

	// 直播
	private EventBean change0101(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0101);

			StringBuilder content = new StringBuilder();
//			content.append(map.get("N")).append("|");
//			content.append(map.get("T")).append("|");
			content.append("|");
			content.append("|");
			content.append(map.get("SI")).append("|");
			content.append(map.get("M")).append("|");// 换台方式
			content.append(map.get("S")).append("|");// 播放状态
			content.append(map.get("CN")).append("|");
			content.append(map.get("SN")).append("|");

			if (map.get("N") == null)
				result.setErroCode("0101 N 不存在");
			else if (map.get("N").isEmpty())
				result.setErroCode("0101 N 为空");

			if (map.get("T") == null)
				result.setErroCode("0101 T 不存在");
			else if (map.get("T").isEmpty())
				result.setErroCode("0101 T 为空");

			if (map.get("SI") == null)
				result.setErroCode("0101 SI 不存在");
			else if (map.get("SI").isEmpty())
				result.setErroCode("0101 SI 为空");

			if (map.get("M") == null)
				result.setErroCode("0101 M 不存在");
			else if (map.get("M").isEmpty())
				result.setErroCode("0101 M 为空");

			if (map.get("S") == null)
				result.setErroCode("0101 S 不存在");
			else if (map.get("S").isEmpty())
				result.setErroCode("0101 S 为空");

			if (map.get("CN") == null)
				result.setErroCode("0101 CN 不存在");
			else if (map.get("CN").isEmpty())
				result.setErroCode("0101 CN 为空");

			if (map.get("SN") == null)
				result.setErroCode("0101 SN 不存在");
			else if (map.get("SN").isEmpty())
				result.setErroCode("0101 SN 为空");

			String channelcode = "";
			if (null == channelcode || channelcode.isEmpty())
				channelcode = "";
			if (null == channelcode || channelcode.isEmpty())
				channelcode = "1";

			//channelcode = map.get("N") + "" + map.get("T") + "" + map.get("SI");
			channelcode = map.get("SI");
			content.append(channelcode);

			result.setContent(channelcode);
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 时移
	private EventBean change0102(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0302);

			StringBuilder content = new StringBuilder();
//			content.append(map.get("N")).append("|");
//			content.append(map.get("T")).append("|");
			content.append("|");
			content.append("|");
			content.append(map.get("SI")).append("|");
			try {
				content.append(Long.valueOf(map.get("PT")) / 1000).append("|");
			} catch (Exception e) {
				content.append("").append("|");
			}
			
			if(null == map.get("S") || map.get("S").equals("0") || map.get("S").length() == 0)
				content.append("1").append("|");// 播放状态
			else 
				content.append(map.get("S")).append("|");// 播放状态

			content.append(map.get("SN")).append("|");

			String channelcode = "";
			if (null == channelcode || channelcode.isEmpty())
				// channelcode = getChannel.getMapName().get(map.get("SN"));
				System.out.println("channelcode" + map.get("SN"));
			if (null == channelcode || channelcode.isEmpty())
				channelcode = "1";

			//channelcode = map.get("N") + "" + map.get("T") + "" + map.get("SI");
			channelcode = map.get("SI");
			content.append(channelcode);

			if (map.get("N") == null)
				result.setErroCode("0102 N 不存在");
			else if (map.get("N").isEmpty())
				result.setErroCode("0102 N 为空");

			if (map.get("T") == null)
				result.setErroCode("0102 T 不存在");
			else if (map.get("T").isEmpty())
				result.setErroCode("0102 T 为空");

			if (map.get("SI") == null)
				result.setErroCode("0102 SI 不存在");
			else if (map.get("SI").isEmpty())
				result.setErroCode("0102 SI 为空");

			if (map.get("PT") == null)
				result.setErroCode("0102 PT 不存在");
			else if (map.get("PT").isEmpty())
				result.setErroCode("0102 PT 为空");

			if (map.get("S") == null)
				result.setErroCode("0102 S 不存在");
			else if (map.get("S").isEmpty())
				result.setErroCode("0102 S 为空");

			if (map.get("SN") == null)
				result.setErroCode("0102 SN 不存在");
			else if (map.get("SN").isEmpty())
				result.setErroCode("0102 SN 为空");

			result.setContent(channelcode);
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}
	
	public SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
	public String formatDate(long msel) {
		try {
			Date date = new Date(msel);
			formatter.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
			return formatter.format(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	// 回看
	private EventBean change0103(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0306);

			StringBuilder content = new StringBuilder();
			content.append(map.get("EI")).append("|");
			content.append(map.get("EN")).append("|");
			content.append(map.get("SN")).append("|");

			// 时间格式化
			String datetime = "";
			String date = "";
			String time = "";
			try{
				datetime = formatDate(Long.valueOf(map.get("ST")));
				date = datetime.substring(0, 8);
				time = datetime.substring(9);
			} catch(Exception e){
				//logger.error("回看播放 0103 ST=" + map.get("ST") + " 格式不是时间格式");
			}
			content.append(date).append("|");
			content.append(time).append("|");
			content.append(map.get("CT")).append("|");
			//暂时把回看播放状态由默认0改为1，否则不能计算回看指标
			if(null == map.get("S") || map.get("S").equals("0") || map.get("S").length() == 0)
				content.append("1");// 播放状态
			else
				content.append(map.get("S"));// 播放状态

			if (map.get("EI") == null)
				result.setErroCode("0103 EI 不存在");
			else if (map.get("EI").isEmpty())
				result.setErroCode("0103 EI 为空");

			if (map.get("EN") == null)
				result.setErroCode("0103 EN 不存在");
			else if (map.get("EN").isEmpty())
				result.setErroCode("0103 EN 为空");

			if (map.get("SN") == null)
				result.setErroCode("0103 SN 不存在");
			else if (map.get("SN").isEmpty())
				result.setErroCode("0103 SN 为空");

			if (map.get("ST") == null)
				result.setErroCode("0103 ST 不存在");
			else if (map.get("ST").isEmpty())
				result.setErroCode("0103 ST 为空");

			if (map.get("CT") == null)
				result.setErroCode("0103 CT 不存在");
			else if (map.get("CT").isEmpty())
				result.setErroCode("0103 CT 为空");

			if (map.get("S") == null)
				result.setErroCode("0103 S 不存在");
			else if (map.get("S").isEmpty())
				result.setErroCode("0103 S 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// VOD播放
	private EventBean change0104(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0304);

			StringBuilder content = new StringBuilder();
			content.append(map.get("SI")).append("|");
			content.append(map.get("SN")).append("|");
			content.append(map.get("SX")).append("|");
			content.append(map.get("CT")).append("|");
			if(null == map.get("S") || map.get("S").equals("0") || map.get("S").length() == 0)
				content.append("1").append("|");// 播放状态
			else 
				content.append(map.get("S")).append("|");// 播放状态
			content.append(map.get("CI")).append("|");
			content.append(map.get("CN"));

			if (map.get("SI") == null)
				result.setErroCode("0104 SI 不存在");
			else if (map.get("SI").isEmpty())
				result.setErroCode("0104 SI 为空");

			if (map.get("SN") == null)
				result.setErroCode("0104 SN 不存在");
			else if (map.get("SN").isEmpty())
				result.setErroCode("0104 SN 为空");

			if (map.get("SX") == null)
				result.setErroCode("0104 SX 不存在");
			else if (map.get("SX").isEmpty())
				result.setErroCode("0104 SX 为空");

			if (map.get("CT") == null)
				result.setErroCode("0104 CT 不存在");
			else if (map.get("CT").isEmpty())
				result.setErroCode("0104 CT 为空");

			if (map.get("S") == null)
				result.setErroCode("0104 S 不存在");
			else if (map.get("S").isEmpty())
				result.setErroCode("0104 S 为空");

			if (map.get("CI") == null)
				result.setErroCode("0104 CI 不存在");
			else if (map.get("CI").isEmpty())
				result.setErroCode("0104 CI 为空");

			if (map.get("CN") == null)
				result.setErroCode("0104 CN 不存在");
			else if (map.get("CN").isEmpty())
				result.setErroCode("0104 CN 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	private EventBean change0105(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0303);

			StringBuilder content = new StringBuilder();
			content.append(map.get("P")).append("|");
			content.append(map.get("C")).append("|");
			content.append(map.get("CN")).append("|");
			content.append(map.get("PN")).append("|");
			content.append(map.get("SN"));

			if (map.get("P") == null)
				result.setErroCode("0105 P 不存在");
			else if (map.get("P").isEmpty())
				result.setErroCode("0105 P 为空");

			if (map.get("C") == null)
				result.setErroCode("0105 C 不存在");
			else if (map.get("C").isEmpty())
				result.setErroCode("0105 C 为空");

			if (map.get("CN") == null)
				result.setErroCode("0105 CN 不存在");
			else if (map.get("CN").isEmpty())
				result.setErroCode("0105 CN 为空");

			if (map.get("PN") == null)
				result.setErroCode("0105 PN 不存在");
			else if (map.get("PN").isEmpty())
				result.setErroCode("0105 PN 为空");

			if (map.get("SN") == null)
				result.setErroCode("0105 SN 不存在");
			else if (map.get("SN").isEmpty())
				result.setErroCode("0105 SN 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 回看应用
	private EventBean change0106(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0305);

			StringBuilder content = new StringBuilder();
			content.append(map.get("P")).append("|");
			content.append(map.get("ST")).append("|");
			content.append(map.get("CN"));

			if (map.get("P") == null)
				result.setErroCode("0106 P 不存在");
			else if (map.get("P").isEmpty())
				result.setErroCode("0106 P 为空");

			if (map.get("ST") == null)
				result.setErroCode("0106 ST 不存在");
			else if (map.get("ST").isEmpty())
				result.setErroCode("0106 ST 为空");

			if (map.get("CN") == null)
				result.setErroCode("0106 CN 不存在");
			else if (map.get("CN").isEmpty())
				result.setErroCode("0106 CN 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// NOVD应用
	private EventBean change0107(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0301);

			StringBuilder content = new StringBuilder();
			content.append(map.get("SN"));

			if (map.get("SN") == null)
				result.setErroCode("0107 SN 不存在");
			else if (map.get("U").isEmpty())
				result.setErroCode("0107 SN 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// NOVD播放
	private EventBean change0108(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0101);

			StringBuilder content = new StringBuilder();
			content.append(map.get("N")).append("|");
			content.append(map.get("T")).append("|");
			content.append(map.get("SI")).append("|");
			content.append("").append("|");// 换台方式
			content.append(map.get("S")).append("|");// 播放状态
			content.append("").append("|");
			content.append(map.get("SN")).append("|");

			if (map.get("N") == null)
				result.setErroCode("0101 N 不存在");
			else if (map.get("N").isEmpty())
				result.setErroCode("0101 N 为空");

			if (map.get("T") == null)
				result.setErroCode("0101 T 不存在");
			else if (map.get("T").isEmpty())
				result.setErroCode("0101 T 为空");

			if (map.get("SI") == null)
				result.setErroCode("0101 SI 不存在");
			else if (map.get("SI").isEmpty())
				result.setErroCode("0101 SI 为空");

			if (map.get("S") == null)
				result.setErroCode("0101 S 不存在");
			else if (map.get("S").isEmpty())
				result.setErroCode("0101 S 为空");

			if (map.get("SN") == null)
				result.setErroCode("0101 SN 不存在");
			else if (map.get("SN").isEmpty())
				result.setErroCode("0101 SN 为空");

			// String channelcode = getChannel.getMapIDS().get(map.get("N") + "_" + map.get("T") + "_" + map.get("SI"));
			System.out.println(map.get("N") + "_" + map.get("T") + "_" + map.get("SI"));
			String channelcode = "";

			if (null == channelcode || channelcode.isEmpty())
				channelcode = "";
			if (null == channelcode || channelcode.isEmpty())
				channelcode = "1";

			//channelcode = map.get("N") + "" + map.get("T") + "" + map.get("SI");
			channelcode = map.get("SI");
			content.append(channelcode);

			result.setContent(channelcode);
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 广告
	private EventBean change0121(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0601);

			StringBuilder content = new StringBuilder();
			content.append(map.get("P")).append("|");

			Long time = (Long.valueOf(map.get("ET")) - Long.valueOf(map.get("ST"))) / 1000;
			content.append(time).append("|");

			content.append(map.get("F")).append("|");
			content.append(map.get("ID")).append("|");
			content.append(map.get("N"));

			if (map.get("P") == null)
				result.setErroCode("0121 P 不存在");
			else if (map.get("P").isEmpty())
				result.setErroCode("0121 P 为空");

			if (map.get("ET") == null)
				result.setErroCode("0121 ET 不存在");
			else if (map.get("ET").isEmpty())
				result.setErroCode("0121 ET 为空");

			if (map.get("ST") == null)
				result.setErroCode("0121 ST 不存在");
			else if (map.get("ST").isEmpty())
				result.setErroCode("0121 ST 为空");

			if (map.get("F") == null)
				result.setErroCode("0121 F 不存在");
			else if (map.get("F").isEmpty())
				result.setErroCode("0121 F 为空");

			if (map.get("ID") == null)
				result.setErroCode("0121 ID 不存在");
			else if (map.get("ID").isEmpty())
				result.setErroCode("0121 ID 为空");

			if (map.get("N") == null)
				result.setErroCode("0121 N 不存在");
			else if (map.get("N").isEmpty())
				result.setErroCode("0121 N 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 应用
	private EventBean change0131(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0301);

			StringBuilder content = new StringBuilder();
			content.append(map.get("N"));

			if (map.get("N") == null)
				result.setErroCode("0131 N 不存在");
			else if (map.get("U").isEmpty())
				result.setErroCode("0131 N 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// URL业务
	private EventBean change0132(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(0x0307);

			StringBuilder content = new StringBuilder();
			String url = map.get("U");
			if(url.indexOf("?") > 0)
				url = url.substring(0, url.indexOf("?"));
			
			content.append(url);

			if (map.get("U") == null)
				result.setErroCode("0132 U 不存在");
			else if (map.get("U").isEmpty())
				result.setErroCode("0132 U 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// APP场景
	private EventBean change0133(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			if ("0x0210".equals(map.get("SC")))
				result.setType(0x0210);
			else if ("0x0211".equals(map.get("SC")))
				result.setType(0x0211);
			else if ("0x0212".equals(map.get("SC")))
				result.setType(0x0212);
			else if ("0x0213".equals(map.get("SC")))
				result.setType(0x0213);
			else if ("0x0214".equals(map.get("SC")))
				result.setType(0x0214);
			else if ("0x0215".equals(map.get("SC")))
				result.setType(0x0215);
			else if ("0x0216".equals(map.get("SC")))
				result.setType(0x0505);
			else if ("0x0217".equals(map.get("SC")))
				result.setType(0x0506);
			else if ("0x0218".equals(map.get("SC")))
				result.setType(0x0507);
			else if ("0x0219".equals(map.get("SC")))
				result.setType(0x0508);
			else if ("0x0220".equals(map.get("SC")))
				result.setType(0xffff);
			else if ("0x0221".equals(map.get("SC")))
				result.setType(0xffff);

			if (map.get("SC") == null)
				result.setErroCode("0133 SC 不存在");
			else if (map.get("SC").isEmpty())
				result.setErroCode("0133 SC 为空");

		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 开机
	private EventBean change0301(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(1);
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 待机
	private EventBean change0302(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(2);
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 机顶盒故障
	private EventBean change0309(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(4);

			StringBuilder content = new StringBuilder();

			if ("10001".equals(map.get("EC")))
				content.append("1").append("|").append("网络连接错误");

			if (map.get("EC") == null)
				result.setErroCode("0309 EC 不存在");
			else if (map.get("EC").isEmpty())
				result.setErroCode("0309 EC 为空");

			result.setContent(content.toString());
			result.setRzcontent(content.toString());
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	// 心跳
	private EventBean change0701(String type, Map<String, String> map) {
		EventBean result = new EventBean();
		try {
			result.setType(3);
		} catch (Exception e) {
			log.error(e.toString());
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		/*File file = new File("D:/wei/data2.txt");
		String encoding = "UTF-8";
		InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
		BufferedReader bufferedReader = new BufferedReader(read);
		String lineTxt = null;
		String result = "";
		while ((lineTxt = bufferedReader.readLine()) != null) {
			System.out.println(lineTxt);
			EventChange event = new EventChange();
			result = event.parseEvent(lineTxt);
			System.out.println(result + "==");
		}
		read.close();
		String line = "<[0301,1418614079356,1370086166,102]><|>\r\n";
		System.out.println("over");*/
	}

}
