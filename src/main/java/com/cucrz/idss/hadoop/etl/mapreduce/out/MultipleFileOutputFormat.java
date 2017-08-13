/**
 * @Copyright 2008-2014 北京中传瑞智市场调查有限公司
 *
 */
package com.cucrz.idss.hadoop.etl.mapreduce.out;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
/**
 * 所有业务输出格式化类
 * @author 魏强
 *
 * @createtime  2015年3月5日
 */


public class MultipleFileOutputFormat extends MultipleOutputFormat<Text,Text> {

	@Override
	protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {
		String filename = "evt_tv";
		String evt_live = "evt_live";
		String evt_lookbackpage = "evt_lookbackpage";
		String evt_lookbackprogram = "evt_lookbackprogram";
		String evt_teleapp = "evt_teleapp";
		String evt_timeshift = "evt_timeshift";
		String evt_vodpage = "evt_vodpage";
		String evt_video = "evt_video";
		String evt_not_affect_ads = "evt_not_affect_ads";
		String evt_affect_ads = "evt_affect_ads";
		String line = value.toString();
		int index = line.indexOf(OtherConstants.VERTICAL_DELIM);
		String bizType = line.substring(0,index);
		if(bizType.equals(String.valueOf(TypeConstans.EMPTY_CHANNEL)))
			filename = String.valueOf(evt_live);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_LIVE_BIZ)))
			filename = String.valueOf(evt_live);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_LIVE__DATA_BIZ)))
			filename = String.valueOf(evt_live);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ)))
			filename = String.valueOf(evt_live);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_VOD_TELEAPP)))
			filename = String.valueOf(evt_teleapp);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_TIMESHIFT_BIZ)))
			filename = String.valueOf(evt_timeshift);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_VOD_PAGE_BIZ)))
			filename = String.valueOf(evt_vodpage);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_VOD_PROGRAM_BIZ)))
			filename = String.valueOf(evt_video);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_LOOKBACK_PAGE_BIZ)))
			filename = String.valueOf(evt_lookbackpage);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_LOOKBACK_PRO_BIZ)))
			filename = String.valueOf(evt_lookbackprogram);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_AFFECT_ADS)))
			filename = String.valueOf(evt_affect_ads);
		if(bizType.equals(String.valueOf(TypeConstans.EVENT_NOT_AFFECT_ADS)))
			filename = String.valueOf(evt_not_affect_ads);
		if (conf!=null&&conf.size()>0){
			String event1 = conf.get("output.event1");
			String oper = conf.get("etl.oper");
			String date = conf.get("etl.inputDate");
			if(event1.equals("true")){
				filename=oper+"_"+date+".txt";
			}
		}
		return filename;
	}
}
