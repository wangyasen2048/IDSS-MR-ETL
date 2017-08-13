package com.cucrz.idss.hadoop.etl.synchronizeBaseData.teleapp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.synchronizeBaseData.connect2Mysql;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.channelBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.teleappBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.channel.mysqlChannel;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.vodInfo.mysqlVOD;

public class mysqlTeleApp {
	private static Connection conn = null;
	private static connect2Mysql instance = null;
	private static Logger log = Logger.getLogger(mysqlChannel.class);

	public static Set<teleappBean> getMysqlTeleApp(Map<String, String> args, String operatorID, String inputDate) {
		instance = connect2Mysql.getInstance();
		conn = instance.getConnect(args);
		String sql = "SELECT teleapp_id,teleapp_code,netoperator_id,"
				+ "teleapp_name,parent_id,level_,url,ordering ,"
				+ "path,create_time,modified_time,state,"
				+ "delete_time,revision_t,revision_n,revision_ft  "
				+ "FROM t_teleapp  "
				+ " WHERE netoperator_id='"+operatorID+"' AND state<>'D'  ";

		try {
			conn.setAutoCommit(false);
			PreparedStatement ps = conn.prepareStatement(sql);
//			ps.setString(1, operatorID);
			ResultSet result = ps.executeQuery(sql);
			Set<teleappBean> teleappSet = new HashSet<teleappBean>();
			while (result.next()) {
				String teleapp_id = getNotNullValue(result.getString(1));
				String teleapp_code = getNotNullValue(result.getString(2));
				String netoperator_id = getNotNullValue(result.getString(3));
				String teleapp_name = getNotNullValue(result.getString(4));
				String parent_id = getNotNullValue(result.getString(5));
				int level_ = result.getInt(6);
				String url = getNotNullValue(result.getString(7));
				int ordering =(result.getInt(8));
				String path = getNotNullValue(result.getString(9));
				String create_time = getNotNullValue(result.getString(10));
				String modified_time = getNotNullValue(result.getString(11));
				String state = getNotNullValue(result.getString(12));
				String delete_time = getNotNullValue(result.getString(13));
				int revision_t = result.getInt(14);
				int revision_n = result.getInt(15);
				int revision_ft = result.getInt(16);
				teleappBean teleapp = new teleappBean(teleapp_id, teleapp_code,
						netoperator_id, teleapp_name, parent_id, level_, url,
						ordering, path, create_time, modified_time, state,
						delete_time, revision_t, revision_n, revision_ft);
				teleappSet.add(teleapp);
			}
			return teleappSet;
		} catch (SQLException e) {
			log.warn("查询电视应用异常！");
			e.printStackTrace();
			return null;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				log.warn("连接关闭异常！");
				e.printStackTrace();
			}
		}
	}
	
	

	public static String getNotNullValue(String value) {
		if (value != null) {
			return value;
		} else {
			return "";
		}
	}
}
