package com.cucrz.idss.hadoop.etl.mapreduce.rules;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class DateCheck {
	private static Logger log = Logger.getLogger(DateCheck.class);
	
	
	public static final SimpleDateFormat DATE_FORMATER = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat TIME_FORMATER = new SimpleDateFormat("HH:mm:ss");
	public static final SimpleDateFormat DATEFORMATER = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat TIMEFORMATER = new SimpleDateFormat("HHmmss");
	public static final SimpleDateFormat DATE_TIME_FORMATER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static boolean dateCheckRule(String date,String inputDate){
		 try {
			inputDate=DATE_FORMATER.format(DATEFORMATER.parse(inputDate));
			date=DATE_FORMATER.format(DATE_FORMATER.parse(date));
		} catch (ParseException e) {
			log.warn("日期不正确，无法解析输入日期");
		}
		if(date.equals(inputDate)){
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		System.out.println(DateCheck.dateCheckRule("2015-01-01", "20150101"));
	}
}
