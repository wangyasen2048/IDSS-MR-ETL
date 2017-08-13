package com.cucrz.idss.hadoop.etl.synchronizeBaseData.teleapp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.teleappBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.channel.hdfsChannel;

public class hdfsTeleApp {
	
	private static Logger log = Logger.getLogger(hdfsChannel.class);
	//数据写入hdfs
	public static void updateTeleApp(Configuration conf,Set<teleappBean> teleappSet){
		Path CacheTeleApp = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_TVApp"));
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
		
		if (fs.exists(CacheTeleApp)) {
			fs.delete(CacheTeleApp);
		}
		OutputStream osteleapp = fs.create(CacheTeleApp);
		OutputStreamWriter oswteleapp = new OutputStreamWriter(osteleapp);
		BufferedWriter bwteleapp = new BufferedWriter(oswteleapp);
		for (teleappBean teleapp : teleappSet) {
			bwteleapp.write(teleapp.getUrl()
					+ OtherConstants.VERTICAL_DELIM
					+ teleapp.getTeleapp_id());
			bwteleapp.newLine();
		}
		bwteleapp.flush();
		osteleapp.close();
		fs.close();
		log.info("-------------电视应用对照表更新完成！--------------");
		} catch (IOException e) {
			log.warn("-------------电视应用对照列表路径错误！------------");
			e.printStackTrace();	
		}
	}
}
