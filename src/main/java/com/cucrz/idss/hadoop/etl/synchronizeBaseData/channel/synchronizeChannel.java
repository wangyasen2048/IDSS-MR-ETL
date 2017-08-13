package com.cucrz.idss.hadoop.etl.synchronizeBaseData.channel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.channelBean;

public class synchronizeChannel {
	private static Logger log = Logger.getLogger(synchronizeChannel.class);
//合并频道到hdfs
	public static void synchromizeChannelToHDFS(Configuration conf) {
		Map<String, String> mysqlParameters = new HashMap<String, String>();
		mysqlParameters.put("url", conf.get("idss_ETL_Mysql_url"));
		mysqlParameters.put("username", conf.get("idss_ETL_Mysql_username"));
		mysqlParameters.put("password", conf.get("idss_ETL_Mysql_password"));
		mysqlParameters.put("operID", conf.get("operatorID"));
		String region = getDefaultRegion(conf.get("operatorID"));
		Set<channelBean> channelList = mysqlChannel.getChannelFromMysql(region,
				mysqlParameters, conf.get("operatorID"),
				conf.get("inputDateID"));
		if (channelList != null && channelList.size() > 0) {
			channelList.remove(null);
			hdfsChannel.updateHDFSFile(conf, channelList);
		} else {
			log.warn("-------数据库无频道数据！------------");
		}
	}
//合并频道到mysql
	public static void synchronizeChannelToMysql(Configuration conf) {
		Set<channelBean> channelList = hdfsChannel.getChannelFromHDFS(conf);
		Map<String, String> mysqlParameters = new HashMap<String, String>();
		mysqlParameters.put("url", conf.get("idss_ETL_Mysql_url"));
		mysqlParameters.put("username", conf.get("idss_ETL_Mysql_username"));
		mysqlParameters.put("password", conf.get("idss_ETL_Mysql_password"));
		mysqlParameters.put("operID", conf.get("operatorID"));
		String region = getDefaultRegion(conf.get("operatorID"));
		if (channelList != null && channelList.size() > 0) {
			channelList.remove(null);
			mysqlChannel.updateChannelToMysql(channelList, region,
					mysqlParameters);
		} else {
			log.warn("-----------HDFS无频道数据！------------");
		}
	}

	public static String getDefaultRegion(String operID) {
		if (operID.equals("0101")) {
			return TypeConstans.default_Gehua;
		} else if (operID.equals("0901")) {
			return TypeConstans.default_Dongfang;
		} else if (operID.equals("1901")) {
			return TypeConstans.default_Guangdong;
		} else if (operID.equals("1902")) {
			return TypeConstans.default_Zhujiang;
		} else if (operID.equals("1601")) {
			return TypeConstans.default_Henan;
		} else {
			return "unknown";
		}
	}

}
