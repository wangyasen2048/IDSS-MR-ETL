package com.cucrz.idss.hadoop.etl.synchronizeBaseData.vodInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.synchronizeBaseData.connect2Mysql;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.columnBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.video2columnBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.videoBean;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class mysqlVOD {
	private static Connection conn = null;
	private static connect2Mysql instance = null;
	private static Logger log = Logger.getLogger(mysqlVOD.class);
//从mysql获取vod信息
	public static void getVODFromMysql(Map<String, String> args,
			String operatorID, String inputDate,Configuration conf)  {
		instance = connect2Mysql.getInstance();
		conn = instance.getConnect(args);
		String d1 = "2015-01-01";
		String d2 = "2015-01-02";
		try {
			Date date = DateUtil.DATEFORMATER.parse(inputDate);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(5, -7);
			d1 = DateUtil.DATE_FORMATER.format(cal.getTime());
			cal.add(5, 8);
			d2 = DateUtil.DATE_FORMATER.format(cal.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String count =  "SELECT count(v.video_id)"
				+ " FROM t_vodcolumn c,t_vodvideo v,t_vodcolumn_vodvideo vc "
				+ "WHERE v.video_code=vc.video_id "
				+ "AND vc.column_id=c.column_code "
				+ "AND c.netoperator_id=vc.netoperator_id "
				+ "AND vc.netoperator_id=v.netoperator_id "
				+ "AND vc.netoperator_id=?  AND v.create_time>? "
				+ "AND v.create_time<? order by v.video_id";
		String query = "SELECT c.column_id,c.netoperator_id,c.column_code,c.parent_code,c.column_name,"
				+ "c.level,c.path,c.order,c.market_id,c.create_time,c.modified_time,"
				+ "v.video_id,v.video_code,v.video_name,"
				+ "v.video_create_time  FROM t_vodcolumn c,t_vodvideo v,t_vodcolumn_vodvideo vc "
				+ "WHERE v.video_code=vc.video_id "
				+ "AND vc.column_id=c.column_code "
				+ "AND c.netoperator_id=vc.netoperator_id "
				+ "AND vc.netoperator_id=v.netoperator_id "
				+ "AND vc.netoperator_id=?  AND v.create_time>? "
				+ "AND v.create_time<? order by v.video_id limit  ?, ?";
		try {
			conn.setAutoCommit(false);
			PreparedStatement pscount = conn.prepareStatement(count);
			pscount.setString(1, operatorID);
			pscount.setString(2, d1);
			pscount.setString(3, d2);
			ResultSet counts = pscount.executeQuery();
			counts.next();
			long number=counts.getLong(1);
			log.info("共有"+number+"条VOD数据");
			long times=(number+49999)/50000;
			log.info("需要分页"+times+"次");
			Set<video2columnBean> video2cloumnSet = new HashSet<video2columnBean>();
			if(times==0L){
				hdfsVOD.updateVODHDFSFile(conf, video2cloumnSet,0L);
			}
			for(long c=0L;c<times;c++){
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, operatorID);
			ps.setString(2, d1);
			ps.setString(3, d2);
			ps.setLong(4,c*50000);
			ps.setLong(5,50000);
			ResultSet result = ps.executeQuery();
//			List vodList = new ArrayList();
//			Set<cloumnBean> cloumnSet = new HashSet<cloumnBean>();
//			Set<videoBean> videoSet = new HashSet<videoBean>();
			
			while (result.next()) {
//				String column_id = getNotNullValue(result.getString(1));
				String netoperator_id = getNotNullValue(result.getString(2));
				String column_code = getNotNullValue(result.getString(3));
//				String parent_code = getNotNullValue(result.getString(4));
				String column_name = getNotNullValue(result.getString(5));
//				int level = result.getInt(6);
//				String path = getNotNullValue(result.getString(7));
//				int order = result.getInt(8);
//				String market_id = getNotNullValue(result.getString(9));
//				String create_time = getNotNullValue(result.getString(10));
				String modified_time = getNotNullValue(result.getString(11));
//				String video_id = getNotNullValue(result.getString(12));
				String video_code = getNotNullValue(result.getString(13));
				String video_name = getNotNullValue(result.getString(14));
				String video_create_time = getNotNullValue(result.getString(15));
//				videoBean video = new videoBean(video_id, video_code,
//						video_name, video_create_time, netoperator_id,
//						video_create_time, modified_time);
				video2columnBean video2cloumn = new video2columnBean(
						column_code, column_name, video_code, video_name,
						netoperator_id, video_create_time, modified_time);
//				cloumnBean cloumn = new cloumnBean(column_id, netoperator_id,
//						column_code, parent_code, column_name, level, path,
//						order, market_id, create_time, modified_time);
//				cloumnSet.add(cloumn);
//				videoSet.add(video);
				video2cloumnSet.add(video2cloumn);
			}
			result.close();
			ps.clearParameters();
			ps.close();
//			vodList.add(0, cloumnSet);
//			vodList.add(1, videoSet);
//			vodList.add(2, video2cloumnSet);
			if(video2cloumnSet!=null&&video2cloumnSet.size()>0){
				video2cloumnSet.remove(null);
			hdfsVOD.updateVODHDFSFile(conf, video2cloumnSet,c);
			video2cloumnSet.clear();
			}else{
				log.warn("-------数据库无VOD数据！------------");
			}
			}			
		} catch (SQLException e) {
			log.warn("查询栏目异常！");
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				log.warn("连接关闭异常！");
				e.printStackTrace();
			}
		}
	}

	public static void updateVideo2CloumnToMysql(
			Set<video2columnBean> video2cloumn, Map<String, String> args) {
		instance = connect2Mysql.getInstance();
		conn = instance.getConnect(args);
		String operID = args.get("operID");
		String insert="insert into t_vodcolumn_vodvideo VALUES(?,?,?,?,?) "
				+ "  ON DUPLICATE KEY UPDATE netoperator_id=?,modified_time=?";
		int counter=0;
		try {
			conn.setAutoCommit(false);
		PreparedStatement psInsert = conn.prepareStatement(insert);
		for (video2columnBean vc : video2cloumn) {
			psInsert.setString(1, vc.getColumn_id());
			psInsert.setString(2,vc.getVideo_id());
			psInsert.setString(3, vc.getNetoperator_id());
			psInsert.setString(4,vc.getModified_time());
			psInsert.setString(5,vc.getModified_time());
			psInsert.setString(6,vc.getNetoperator_id());
			psInsert.setString(7,vc.getModified_time());
			psInsert.addBatch();
			counter++;
			if(counter==5000){
				psInsert.executeBatch();
				counter=0;
				conn.commit();
				psInsert.clearBatch();
			}
		}
		psInsert.executeBatch();
		counter=0;
		conn.commit();
		psInsert.clearBatch();
		} catch (SQLException e) {
			log.warn("更新失败！错误的影片栏目映射");
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.warn("RollBack failed");
			}
		}
			try {
				conn.close();
			} catch (SQLException e) {
				log.warn("连接关闭异常！");
				e.printStackTrace();
			}
		log.info("-----------数据库栏目影片映射同步完成！-----------");
	}

	public static void updateVideoToMysql(Set<videoBean> videoSet,
			Map<String, String> args) {
		instance = connect2Mysql.getInstance();
		conn = instance.getConnect(args);
		String operID = args.get("operID");
		String insert="insert into t_vodvideo  VALUES(?,?,?,?,?,?,?)  "
				+ "ON DUPLICATE KEY UPDATE  video_name=? ,netoperator_id=? ,modified_time=?";
		try {
		conn.setAutoCommit(false);
		PreparedStatement psInsert = conn.prepareStatement(insert);
		int counter=0;
		for (videoBean v : videoSet) {
			psInsert.setString(1, v.getVideo_id());
			psInsert.setString(2, v.getVideo_code());
			psInsert.setString(3, v.getVideo_name());
			psInsert.setString(4, v.getModified_time());
			psInsert.setString(5,v.getNetoperator_id());
			psInsert.setString(6, v.getModified_time());
			psInsert.setString(7, v.getModified_time());
			psInsert.setString(8, v.getVideo_name());
			psInsert.setString(9, v.getNetoperator_id());
			psInsert.setString(10, v.getModified_time());
			psInsert.addBatch();
			counter++;
			if(counter==1000){
				psInsert.executeBatch();
				counter=0;
				conn.commit();
				psInsert.clearBatch();
			}
		}
		 psInsert.executeBatch();
		 counter=0;
		conn.commit();
		psInsert.clearBatch();
		} catch (SQLException e) {
			log.warn("更新失败！错误的影片");
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.warn("RollBack failed");
			}
		}		
			try {
				conn.close();
			} catch (SQLException e) {
				log.warn("连接关闭异常！");
				e.printStackTrace();
			}
		log.info("-----------数据库影片同步完成！-----------");
	}

	public static void updateCloumnToMysql(Set<columnBean> cloumnSet,
			Map<String, String> args) {
		instance = connect2Mysql.getInstance(); 
		conn = instance.getConnect(args);
		String operID = args.get("operID");
		String insert = "insert into t_vodcolumn   VALUES (?,?,?,?,?,?,?,?,?,?,?)   "
				+ "ON DUPLICATE KEY UPDATE  netoperator_id=? ,column_name=? ,"
				+ "modified_time=?";
		int counter=0;
		try {
			conn.setAutoCommit(false);
		PreparedStatement psInsert = conn.prepareStatement(insert); 
		for (columnBean c : cloumnSet) {
			psInsert.setString(1, c.getColumn_id());
			psInsert.setString(2, c.getNetoperator_id());
			psInsert.setString(3, c.getColumn_code());
			psInsert.setString(4, c.getParent_code());
			psInsert.setString(5, c.getColumn_name());
			psInsert.setInt(6, c.getLevel());
			psInsert.setInt(6, 3);
			psInsert.setString(7, c.getPath());
			psInsert.setInt(8, c.getOrder());
			psInsert.setString(9, c.getMarket_id());
			psInsert.setString(10, c.getModified_time());
			psInsert.setString(11, c.getModified_time());
			psInsert.setString(12,  c.getNetoperator_id());
			psInsert.setString(13,  c.getColumn_name());
			psInsert.setString(14,  c.getModified_time());
			psInsert.addBatch();
			counter++;
			if(counter==1000){
				psInsert.executeBatch();
				counter=0;
				conn.commit();
				psInsert.clearBatch();
			}
		}
		psInsert.executeBatch();
		counter=0;
		conn.commit();
		psInsert.clearBatch();
		} catch (SQLException e) {
			log.warn("更新失败！错误的栏目");
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.warn("RollBack failed");
			}
		}
			try {
				conn.close();
			} catch (SQLException e) {
				log.warn("连接关闭异常！");
				e.printStackTrace();
			}		
		log.info("-----------数据库栏目同步完成！-----------");
	}

	public static String getNotNullValue(String value) {
		if (value != null) {
			return value;
		} else {
			return "";
		}
	}
}
