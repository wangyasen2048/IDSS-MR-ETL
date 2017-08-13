package com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.DefaultProperty;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;

public class RuleUtil {
	private static Logger log = Logger.getLogger(RuleUtil.class);
	
	public static Map<String, String> getConf(Context context){
		Map<String,String> map = new HashMap<String, String>();
		if(context!=null){
			Configuration conf = context.getConfiguration();
			map = conf.getValByRegex("^idss_");
			if (!map.containsKey("idss_ETL_Rule_OC")) {
				map.put("idss_ETL_Rule_OC", DefaultProperty.idss_ETL_Rule_OC);
			}
			if (!map.containsKey("idss_ETL_Rule_LoseHeartBeat")) {
				map.put("idss_ETL_Rule_LoseHeartBeat",
						DefaultProperty.idss_ETL_Rule_LoseHeartBeat);
			}
			if (!map.containsKey("idss_ETL_Rule_FindNewBaseData")) {
				map.put("idss_ETL_Rule_FindNewBaseData",
						DefaultProperty.idss_ETL_Rule_NewBaseData);
			}
			if (!map.containsKey("idss_ETL_POWER_ONOFF_INTERVAL_TIME")) {
				map.put("idss_ETL_POWER_ONOFF_INTERVAL_TIME",
						DefaultProperty.POWER_ONOFF_INTERVAL_TIME);
			} else {
				int pooit = Integer.valueOf(map
						.get("idss_ETL_POWER_ONOFF_INTERVAL_TIME"));
				if (pooit < 5) {
					log.warn("开关机超时时间过小(低于5s)，使用默认超时时间="
							+ DefaultProperty.POWER_ONOFF_INTERVAL_TIME + "s");
					map.put("idss_ETL_POWER_ONOFF_INTERVAL_TIME",
							DefaultProperty.POWER_ONOFF_INTERVAL_TIME);
				} else if (pooit > 86400) {
					log.warn("开关机超时时间过大(大于86400s)，使用默认超时时间="
							+ DefaultProperty.POWER_ONOFF_INTERVAL_TIME + "s");
					map.put("idss_ETL_POWER_ONOFF_INTERVAL_TIME",
							DefaultProperty.POWER_ONOFF_INTERVAL_TIME);
				}
			}
			if (!map.containsKey("idss_ETL_POWER_ON_SUB_TIME")) {
				map.put("idss_ETL_POWER_ON_SUB_TIME",
						DefaultProperty.POWER_ON_SUB_TIME);
			} else {
				int pooit = Integer.valueOf(map
						.get("idss_ETL_POWER_ONOFF_INTERVAL_TIME"));
				int post = Integer.valueOf(map
						.get("idss_ETL_POWER_ON_SUB_TIME"));
				if (post > (pooit / 2)) {
					log.warn("开机前置时间大于开关机超时时间的一半，使用当前超时时间的一半=" + pooit / 2
							+ "s");
					map.put("idss_ETL_POWER_ON_SUB_TIME",
							String.valueOf(pooit / 2));
				}
			}
			if (!map.containsKey("idss_ETL_POWER_OFF_POSTPONED_TIME")) {
				map.put("idss_ETL_POWER_OFF_POSTPONED_TIME",
						DefaultProperty.POWER_OFF_POSTPOSTIVE_TIME);
			} else {
				int popt = Integer.valueOf(map
						.get("idss_ETL_POWER_OFF_POSTPONED_TIME"));
				int post = Integer.valueOf(map
						.get("idss_ETL_POWER_ON_SUB_TIME"));
				int pooit = Integer.valueOf(map
						.get("idss_ETL_POWER_ONOFF_INTERVAL_TIME"));
				if (popt > (pooit - post)) {
					log.warn("关机后延时间大于开关机超时时间减去开机前置时间，使用当前配置的超时时间减去开机前置时间=" + (pooit -post)
							+ "s");
					map.put("idss_ETL_POWER_OFF_POSTPONED_TIME",
							String.valueOf(pooit / 2));
				}
			}
			if (!map.containsKey("idss_ETL_NOCHANNEL_INTERVAL_TIME")) {
				map.put("idss_ETL_NOCHANNEL_INTERVAL_TIME",
						DefaultProperty.NOCHANNEL_INTERVAL_TIME);
			} else {
				int nit = Integer.valueOf(map
						.get("idss_ETL_NOCHANNEL_INTERVAL_TIME"));
				int pooit = Integer.valueOf(map
						.get("idss_ETL_POWER_ONOFF_INTERVAL_TIME"));
				if (nit > pooit) {
					log.warn("空频道间隔时长大于开关机超时时间，使用开关机超时时间的一半=" + pooit / 2 + "s");
					map.put("idss_ETL_NOCHANNEL_INTERVAL_TIME",
							String.valueOf(pooit / 2));
				}
			}
			if (!map.containsKey("idss_ETL_HEART_BEAT_INTERVAL_TIME")) {
				map.put("idss_ETL_HEART_BEAT_INTERVAL_TIME",
						DefaultProperty.HEART_BEAT_INTERVAL_TIME);
			}
			if (!map.containsKey("idss_ETL_SAME_EVENT_MERGE_TIME")) {
				map.put("idss_ETL_SAME_EVENT_MERGE_TIME",
						DefaultProperty.MERGE_INTERVAL_TIME);
			} else {
				int semt = Integer.valueOf(map
						.get("idss_ETL_SAME_EVENT_MERGE_TIME"));
				int pooit = Integer.valueOf(map
						.get("idss_ETL_POWER_ONOFF_INTERVAL_TIME"));
				if (semt > pooit) {
					log.warn("合并间隔时长大于开关机超时时间，使用开关机超时时间的一半=" + pooit / 2 + "s");
					map.put("idss_ETL_SAME_EVENT_MERGE_TIME",
							String.valueOf(pooit / 2));
				}
			}
		}else {		
			log.warn("There is no properties start with 'idss' in property file! use default properties");
			//默认规则
			map.put("idss_ETL_Rule_MergeSameEvent", DefaultProperty.idss_ETL_Rule_Merge_Same_Event);
			map.put("idss_ETL_Rule_OC", DefaultProperty.idss_ETL_Rule_OC);
			map.put("idss_ETL_Rule_LoseHeartBeat", DefaultProperty.idss_ETL_Rule_LoseHeartBeat);
			map.put("idss_ETL_Rule_NoChannel", DefaultProperty.idss_ETL_Rule_NoChannel);
			map.put("idss_ETL_Rule_SampleUser", DefaultProperty.idss_ETL_Rule_SampleUser);
			map.put("idss_ETL_Rule_FindNewBaseData", DefaultProperty.idss_ETL_Rule_NewBaseData);
			//默认时间参数
			map.put("idss_ETL_POWER_ONOFF_INTERVAL_TIME", DefaultProperty.POWER_ONOFF_INTERVAL_TIME);
			map.put("idss_ETL_POWER_ON_SUB_TIME", DefaultProperty.POWER_ON_SUB_TIME);
			map.put("idss_ETL_POWER_OFF_POSTPONED_TIME", DefaultProperty.POWER_OFF_POSTPOSTIVE_TIME);
			map.put("idss_ETL_NOCHANNEL_INTERVAL_TIME", DefaultProperty.NOCHANNEL_INTERVAL_TIME);
			map.put("idss_ETL_HEART_BEAT_INTERVAL_TIME", DefaultProperty.HEART_BEAT_INTERVAL_TIME);
		}			
		return map;
	}

	public static boolean isAffectTime(String line,int biz) {
		String split[] = line.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String bizType = split[biz];
		int bizz=0;
		try{
			 bizz=Integer.valueOf(bizType);
		}catch (Exception e){
			log.warn("类型不正确");
			e.printStackTrace();
			return true;
		}
		switch (bizz) {
		case 4:return false;
		case 512:return false;
		case 513:return false;
		case 514:return false;
		case 528:return false;
		case 529:return false;
		case 530:return false;
		case 531:return false;
		case 532:return false;
		case 533:return false;
		case 534:return false;
		case 535:return false;
		case 536:return false;
		case 537:return false;
		case 544:return false;
		case 1025:return false;
		case 1026:return false;
		case 1027:return false;
		case 1028:return false;
		case 1537:return false;		
		}
		
		return true;
	}
	
	public static class RecordCompartor implements Comparator<String> {

		@Override
		public int compare(String o1, String o2) {			
//			String[] sp1 = o1.split(OtherConstants.VERTICAL_DELIM_REGEX);
//			String[] sp2 = o2.split(OtherConstants.VERTICAL_DELIM_REGEX);	
//			int idx1=o1.indexOf(sp1[0]);
//			int idx2=o2.indexOf(sp2[0]);
//			String cp1=o1.substring(idx1);
//			String cp2=o2.substring(idx2);
			return o1.compareTo(o2);
		}
	}

}
