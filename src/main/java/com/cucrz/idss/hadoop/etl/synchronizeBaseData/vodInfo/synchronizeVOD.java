package com.cucrz.idss.hadoop.etl.synchronizeBaseData.vodInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.columnBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.video2columnBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.videoBean;

public class synchronizeVOD {
	private static Logger log = Logger.getLogger(synchronizeVOD.class);
	
//合并vod信息到hdfs
	public static void synchromizeVODInfoToHDFS(Configuration conf){
		Set<video2columnBean> video2cloumnSet = new HashSet<video2columnBean>();
		Map<String,String> mysqlParameters=new HashMap<String, String>();
 		mysqlParameters.put("url", conf.get("idss_ETL_Mysql_url"));
 		mysqlParameters.put("username", conf.get("idss_ETL_Mysql_username"));
 		mysqlParameters.put("password", conf.get("idss_ETL_Mysql_password"));
 		mysqlParameters.put("operID", conf.get("operatorID"));
 		mysqlVOD.getVODFromMysql(mysqlParameters,conf.get("operatorID"),conf.get("inputDateID"),conf);
	}
	
	
	public static void synchronizeVODInfoToMysql(Configuration conf){
		List vodList=hdfsVOD.getVODInfoFromHDFS(conf);
		Set<columnBean> cloumnSet = new HashSet<columnBean>();
		Set<videoBean> videoSet = new HashSet<videoBean>();
		Set<video2columnBean> video2cloumnSet = new HashSet<video2columnBean>();
		if(vodList!=null&&vodList.size()>0){
		cloumnSet=(Set<columnBean>)vodList.get(0);
		videoSet=(Set<videoBean>)vodList.get(1);
		video2cloumnSet=(Set<video2columnBean>)vodList.get(2);
 		Map<String,String> mysqlParameters=new HashMap<String, String>();
 		mysqlParameters.put("url", conf.get("idss_ETL_Mysql_url"));
 		mysqlParameters.put("username", conf.get("idss_ETL_Mysql_username"));
 		mysqlParameters.put("password", conf.get("idss_ETL_Mysql_password"));
 		mysqlParameters.put("operID", conf.get("operatorID"));
 		if(cloumnSet==null){
 			System.out.println("cloumn is null!!!!!!!!!!!!!!!");
 		}
 		if(cloumnSet!=null&&cloumnSet.size()>0){
 			cloumnSet.remove(null);
 		mysqlVOD.updateCloumnToMysql(cloumnSet, mysqlParameters);
 		}else{
 			log.warn("-----------HDFS无栏目数据！------------");
 		}
 		if(videoSet!=null&&videoSet.size()>0){
 			videoSet.remove(null);
 	 		mysqlVOD.updateVideoToMysql(videoSet, mysqlParameters);
 	 		}else{
 	 			log.warn("-----------HDFS无影片数据！------------");
 	 		}
 		if(video2cloumnSet!=null&&video2cloumnSet.size()>0){
 			video2cloumnSet.remove(null);
 	 		mysqlVOD.updateVideo2CloumnToMysql(video2cloumnSet, mysqlParameters);
 	 		}else{
 	 			log.warn("-----------HDFS无栏目影片映射数据！------------");
 	 		}
		}
	}
	
	public static String getDefaultRegion(String operID){
		if(operID.equals("0101")){
			return TypeConstans.default_Gehua;
		}else if(operID.equals("0901")){
			return TypeConstans.default_Dongfang;				
		}else if(operID.equals("1901")){
			return TypeConstans.default_Guangdong;				
		}else if(operID.equals("1902")){
			return TypeConstans.default_Zhujiang;				
		}else if(operID.equals("1601")){
			return TypeConstans.default_Henan;				
		}else if(operID.equals("2301")){
			return TypeConstans.default_Henan;				
		}else{
			return "unknown";
		}			
	}
}
