package com.cucrz.idss.hadoop.etl.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;

public class DateUtil {
	private static Logger log = Logger.getLogger(DateUtil.class);
	public static final SimpleDateFormat DATE_FORMATER = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATE_TIME_FORMATER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_TIME_FULL = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat DATEFORMATER = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat TIMEFORMATER = new SimpleDateFormat("HHmmss");
	public static final SimpleDateFormat TIME_FORMATER = new SimpleDateFormat("HH:mm:ss");
	public static final SimpleDateFormat DATE_TIME_FORMATER_HENAN = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

	//转换时间格式yyyyMMdd  为yyyy-MM-dd
	public static String getDateStr(String str) {
		try {
			return DATE_FORMATER.format(DATEFORMATER.parse(str));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 传入一个时间的毫秒数，转化为outFormat {@link SimpleDateFormat} 格式的的String 
	 * @param obj
	 * @param outFormat
	 * @return
	 */
	public static String getDateFormMsec(Object obj,SimpleDateFormat outFormat) throws Exception {
		long millisecond = Long.parseLong(obj.toString());
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(millisecond);
		return outFormat.format(calendar.getTime());
	}
	/**
	 * 根据传如的 dateStr 和 dateStr {@link SimpleDateFormat} 的格式，<br/>
	 * 转化为outFormat {@link SimpleDateFormat} 格式的的String 
	 * @param dateStr
	 * @param inputFormat
	 * @param outFormat
	 * @return
	 * @throws ParseException
	 */
	public static String getFormatDateStr(String dateStr,
			SimpleDateFormat inputFormat, SimpleDateFormat outFormat) throws ParseException  {
		return outFormat.format(inputFormat.parse(dateStr));
	}
	
	//得到当前时间
	public static String getCurrentTime() {
		return getDateTimeStr(System.currentTimeMillis());
	}
	
	//格式化输入的时间
	public static String getDateTimeStr(Object obj) {
		return DATE_TIME_FORMATER.format(obj);
	}
		
	//获取昨天日期
	public static String getYesterday() {
		return DATEFORMATER.format(System.currentTimeMillis() - 24 * 60 * 60 * 1000);
	}
	//获取输入日期的后一天的日期
	public static String getLaterDate(String date,SimpleDateFormat format){
		String laterDate="";
		try {
			Date in = format.parse(date);
			Calendar cal = Calendar.getInstance();
			cal.setTime(in);
			cal.add(Calendar.DAY_OF_MONTH, 1);
			laterDate = format.format(cal.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			log.warn("输入日期有误，返回原输入！");
			return date;
		}
		return laterDate;
	}
	
	//获取输入日期的前一天的日期
	public static String getPreDate(String date,SimpleDateFormat format){
		String preDate="";
		try {
			Date in = format.parse(date);
			Calendar cal = Calendar.getInstance();
			cal.setTime(in);
			cal.add(Calendar.DAY_OF_MONTH, -1);
			preDate = format.format(cal.getTime());		
		} catch (ParseException e) {
			e.printStackTrace();
			log.warn("输入日期有误，返回原输入！");
			return date;
		}
		return preDate;			
	}
	//得到输入的时间点的秒数 （输入日期类型）
	public static long getSecondInDay(String HHmmss, SimpleDateFormat formatter) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(formatter.parse(HHmmss));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return cal.get(Calendar.HOUR_OF_DAY) * 60 * 60 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
	}
	
   //得到输入的时间点的秒数
	public static long getSecondsInDay(String HHmmss) {
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(TIME_FORMATER.parse(HHmmss));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return cal.get(Calendar.HOUR_OF_DAY) * 60 * 60 + cal.get(Calendar.MINUTE) * 60 + cal.get(Calendar.SECOND);
	}
	
	
	//时间加减秒数
	public static String timeCaculation(String input1,String input2,String operation) {
		Date in;
		long i =0;
		try {
			in = DATE_TIME_FORMATER.parse(input1);
			i= in.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		long second = Long.valueOf(input2);
		if (operation.equals("+")){
			long mills =i+(second*1000);
			return getDateTimeStr(mills);
			
		}else if(operation.equals("-")){
			long mills =i-(second*1000);
			return getDateTimeStr(mills);
		}else{
			log.warn("时间运算失败！");
			return "0";
		}
	
	}
	//时间互相加减
	public static String getResultOfTimes(String input1,String input2,String operation){
		String resultTime="";
		Date in1;
		Date in2;
		long result=0;
		try {
			in1=DATE_TIME_FORMATER.parse(input1);
			in2=DATE_TIME_FORMATER.parse(input2);
			if(operation.equals("+")){
				result=in1.getTime()+in2.getTime();
			}else if (operation.equals("-")){
				result=in1.getTime()-in2.getTime();
			}
			resultTime=String.valueOf(result/1000);
		} catch (ParseException e) {
			log.warn("输入时间不正确");
			e.printStackTrace();
		
		}
		return resultTime;						
	}
	
	//获取记录开始时间
	public static String getStartTime(String data){
		String[] splits = data.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String startTime = splits[0];
		return startTime;		
	}
	//获取记录结束时间
	public static String getEndTime(String data){
		String[] splits = data.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String endTime = splits[1];
		return endTime;		
	}
	// 验证时间格式是否正确
	public static void InputFormatIsRight(String param) throws Exception {
		String regEx = "";
		if (Long.valueOf(param.replaceAll("-", "")) < 20000000 || Long.valueOf(param.replaceAll("-", "")) > 20700000) {
			log.warn("时间日期格式不正确！");
			throw new Exception();
		}
		// 简单验证日期渗格式
		regEx = "^(^(\\d{4}|\\d{2})(\\|\\/|\\.)\\d{1,2}\\3\\d{1,2}$)|(^\\d{4}\\d{1,2}\\d{1,2}$)$";
		Pattern pat = Pattern.compile(regEx);
		Matcher mat = pat.matcher(param);
		boolean rs = mat.find();
		if (!rs) {
			log.warn("时间日期格式不正确！");
			throw new Exception();
		}
	}
	//获取任务持续时间
	public static String getDuration(long start,long end){
		long sub = end-start;
		
		 	long days = sub / (1000 * 60 * 60 * 24);  
		    long hours = (sub % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);  
		    long minutes = (sub % (1000 * 60 * 60)) / (1000 * 60);  
		    long seconds = (sub % (1000 * 60)) / 1000;  
		
		return days+"天"+hours+"小时"+minutes+"分"+seconds+"秒";
	}
	//获取任务持续时间
	public static String getDuration(long duration){
		long sub = duration;
		 	long days = sub / (1000 * 60 * 60 * 24);  
		    long hours = (sub % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);  
		    long minutes = (sub % (1000 * 60 * 60)) / (1000 * 60);  
		    long seconds = (sub % (1000 * 60)) / 1000;  
		
		return days+"天"+hours+"小时"+minutes+"分"+seconds+"秒";
	}
}
