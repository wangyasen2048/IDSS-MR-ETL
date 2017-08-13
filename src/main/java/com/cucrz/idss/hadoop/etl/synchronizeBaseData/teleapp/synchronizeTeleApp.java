package com.cucrz.idss.hadoop.etl.synchronizeBaseData.teleapp;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;

import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.teleappBean;

public class synchronizeTeleApp {
	public static void synchromizeTeleAppFromMysqlToHDFS(Configuration conf) {
		Map<String, String> mysqlParameters = new HashMap<String, String>();
		mysqlParameters.put("url", conf.get("idss_ETL_Mysql_url"));
		mysqlParameters.put("username", conf.get("idss_ETL_Mysql_username"));
		mysqlParameters.put("password", conf.get("idss_ETL_Mysql_password"));
		mysqlParameters.put("operID", conf.get("operatorID"));
		Set<teleappBean> teleappSet = mysqlTeleApp.getMysqlTeleApp( mysqlParameters, conf.get("operatorID"),
				conf.get("inputDateID"));
		if(teleappSet!=null&&teleappSet.size()>0){
			teleappSet.remove(null);
		hdfsTeleApp.updateTeleApp(conf, teleappSet);
		}else{
			Log.warn("数据库中没有电视应用信息！！");
		}
		
		
	}
}
