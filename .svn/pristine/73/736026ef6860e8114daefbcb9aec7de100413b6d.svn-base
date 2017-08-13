package com.cucrz.idss.hadoop.etl.synchronizeBaseData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.channelBean;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class connect2Mysql {
	private static Connection conn = null;
	private static connect2Mysql instance = null;
	private static Logger log = Logger.getLogger(connect2Mysql.class);

	private connect2Mysql(){
		
	}
	
	public  Connection getConnect(Map<String, String> args) {
		Connection c = null;
		if (conn == null) {
			if (args != null && args.size() > 0) {
				String url = args.get("url");
				// String database = args.get("database");
				String userName = args.get("username");
				String password = args.get("password");
				String connection=url + "&user=" + userName
						+ "&password=" + password;
				if(!url.endsWith("?useUnicode=true&characterEncoding=UTF-8")){
					connection=url + "?user=" + userName
							+ "&password=" + password;
				}
				try {
					Class.forName("com.mysql.jdbc.Driver");
					c = DriverManager.getConnection(connection);
					log.info("Create jdbc connection successed 数据库=" + url
							+ ",用户=" + userName);
				} catch (Exception e) {
					log.warn("创建数据库连接失败！连接参数==="+connection);
					e.printStackTrace();
				}
			} else {
				log.info("jdbc connection has been created");
				return c;
			}
		}
		return c;
	}

	public static connect2Mysql getInstance() {
		if (instance == null) {
			synchronized (connect2Mysql.class) {
				instance = new connect2Mysql();
			}
		}
		return instance;
	}
}
