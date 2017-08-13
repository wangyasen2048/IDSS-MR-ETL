package com.cucrz.idss.hadoop.etl.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class DeleteDataUtil {
	private static Logger log = Logger.getLogger(DeleteDataUtil.class);
	private static Connection conn = null;
	private static DeleteDataUtil instance=null;

	
	private DeleteDataUtil(){
		
	}
	
	public static DeleteDataUtil getInstance(){
		if(instance==null){
			synchronized (DeleteDataUtil.class) {
				instance = new	DeleteDataUtil();
			}
		}
		return instance;
	}
	
	
	private static Connection getConnect(Map<String, String> args) {
		Connection c = null;
		if (conn == null) {
			
			if (args != null && args.size() > 0) {
				try {
					String url = args.get("url");
//					String database = args.get("database");
					String userName = args.get("userName");
					String password = args.get("password");
					Class.forName("com.mysql.jdbc.Driver");
					c = DriverManager.getConnection(url + "?user=" + userName + "&password=" + password);
					log.info("Creat jdbc connection successed 数据库="+url+",用户="+userName);
				} catch (Exception e) {
					log.warn("Creat jdbc connection failed! Please check the arguements");
					e.printStackTrace();
				}
			} else {
				log.info("jdbc connection has been created");
				return c;
			}
		}
		return c;
	}

	public void deleteData(Map<String, String> args) {
		conn = getConnect(args);

		String sql = "";
		String type="";
		String tableName="";
		String netoperator_id="";
		String day="";
		String startTime="";
		try {
			conn.setAutoCommit(false);
			if (args != null && args.size() > 0) {
				 type = args.get("type");
			// 1 is indicator for one day;
		   //  2 is indicator for some minutes;
		  //   3 is indicator for more long time;
											
				 tableName = args.get("tableName");
				 netoperator_id = args.get("netoperator_id");
				 day = "";
				 startTime = "";
				if (args.containsKey("day")) {
					String adyTmp=
							args.get("day").substring(0,4)+"-"+args.get("day").substring(4,6)+"-"+args.get("day").substring(6, 8);
							
					if (checkTimeType(adyTmp)) {
						day = adyTmp;
						// example :2015-01-01
					} else {
						log.warn("day type is not right");
					}
				}
				if (args.containsKey("startTime")) {
					String startTimeTmp=
							args.get("startTime").substring(0,4)+"-"+args.get("startTime").substring(4,6)+"-"+args.get("startTime").substring(6, 8);
					if (checkTimeType(startTimeTmp)) {
						startTime = startTimeTmp;
						// example : 2015-01-01
														
					} else {
						log.warn("startTime type is not right");
					}
				}
				int t = Integer.parseInt(type);
				switch (t) {
				case 1:
					sql = "delete from " + tableName
							+ " where netoperator_id='" + netoperator_id
							+ "' and day_='" + day + "'";
					break;
				case 2:
					sql = "delete from " + tableName
							+ " where netoperator_id='" + netoperator_id
							+ "' and DATE_FORMAT (start_time,\"%Y-%m-%d\") = '" + startTime +"'";
					break;
				case 3:
					sql = "truncate " + tableName;
					break;
				}
			}
			log.info("语句="+sql);
			PreparedStatement ps = conn.prepareStatement(sql);
			int result = ps.executeUpdate();
			conn.commit();
			ps.close();
			if (result>0) {
				if(day!=null&&day.length()>0)
					log.info("delete successed! 表名="+tableName+",类型="+type+",运营商ID="+netoperator_id+",日期="+day);
				if(startTime!=null&&startTime.length()>0)
					log.info("delete successed! 表名="+tableName+",类型="+type+",运营商ID="+netoperator_id+",日期="+startTime);
			} else {
				log.warn("delete failed!  Please check the arguements!");
			}

		} catch (SQLException e1) {
			e1.printStackTrace();
				try {
					
					conn.rollback();
					
					log.warn("delete failed!---- RollBack");
				} catch (SQLException e) {
					log.warn("RollBack failed");
					e.printStackTrace();
				}
		}
		try {
			conn.close();
			log.info("数据库连接关闭");
		} catch (SQLException e) {
			log.warn("数据库连接关闭失败！");
			e.printStackTrace();
		}
	}

	public boolean checkTimeType(String time) {
		boolean judge = false;
		String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
		Pattern pattern = Pattern.compile(regex);
		Matcher isNum = pattern.matcher(time);
		if (isNum.matches()) {
			judge = true;
		}
		return judge;
	}
	public static void checkTableName(Map<String,String> map){
		String tableName = map.get("tableName");
		String type = map.get("type");
		boolean mark = tableName.endsWith("_1d");
		if(tableName!=null&&type!=null){
		if(mark==true){
			if(type.equals("1")){
				return ;
			}else{
				map.put("type", "1");
				return ;
			}				
		}else {
			if(type.equals("2")){
				return ;
			}else{
				map.put("type", "2");
				return ;
			}
		}
		}
	}

	public static void deletemain(Map<String, String> args) {
		log.info("Start to delete data rows");
		DeleteDataUtil d =DeleteDataUtil.getInstance();
		d.deleteData(args);
	}

	public static void main(String[] args) {
		
		log.info("进入delete 方法！");
		Map<String, String> map = new HashMap<String, String>();
		try {
			map.put("url", args[0]);
			map.put("userName", args[1]);
			map.put("password", args[2]);
			map.put("type", args[3]);
			map.put("tableName", args[4]);
			checkTableName(map);		
			map.put("netoperator_id", args[5]);
			if(map.get("type").equals("1")){
				map.put("day", args[6]);
			}else if (map.get("type").equals("2")){
				map.put("startTime", args[6]);
			}else if (map.get("type").equals("3")){
			
			}else  {
				log.warn("类型与日期不匹配！");
			}
			
		} catch (Exception e) {
			log.error("输入参数有误");
		}
		
		deletemain(map);
	}
}
