/**
 * <p>Title:ParamBean.java</p>
 * <p>Description:</p>
 * <p>Copyright:Copyright(c)2013</p>
 * <p>Company:ZhongChuan</p>
 * @author zhouhuichun
 * @date 2014-5-23
 * version 1.0
 */
package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.fujian;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.util.DateUtil;


public class EventBean implements Comparable<EventBean>, Cloneable{

	private String user = "";
	private String area = "";
	private String time = "";
	private long ltime = 0;//秒
	private int type;
	private String content = "";
	private String source;
	private String rzcontent = "";
	private String orignalArea;
	private String erroCode;
	
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public long getLtime() {
		return ltime;
	}
	public void setLtime(long ltime) {
		this.ltime = ltime;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	
	public String getRzcontent() {
		return rzcontent;
	}
	public void setRzcontent(String rzcontent) {
		this.rzcontent = rzcontent;
	}
	
	public String getErroCode() {
		return erroCode;
	}
	public void setErroCode(String erroCode) {
		this.erroCode += erroCode+"\n";
	}
	public String getOrignalArea() {
		return orignalArea;
	}
	public void setOrignalArea(String orignalArea) {
		this.orignalArea = orignalArea;
	}
	
	public String getFromRzcontent(){
		if(null != this.rzcontent && this.rzcontent.length() > 0)
			return this.user + "|" + this.area + "|" + this.ltime + "|" + this.type + "|" + this.rzcontent;
		else
			return this.user + "|" + this.area + "|" + this.ltime + "|" + this.type;
	}
	public String getFromMRcontent(){
		if(null != this.rzcontent && this.rzcontent.length() > 0)
			return this.user + "|" + this.area + OtherConstants.TAB_DELIM + formatDate(this.ltime * 1000) + "|" + this.type + "|" + this.rzcontent;
		else
			return this.user + "|" + this.area + OtherConstants.TAB_DELIM + formatDate(this.ltime * 1000) + "|" + this.type + "|";
	}
	public String getFromContent(){
		if(null != this.content && this.content.length() > 0)
			return this.user + "|" + this.orignalArea + "|" + this.area + "|" + this.ltime + "|" + this.type + "|" + this.content;
		else
			return this.user + "|" + this.orignalArea + "|" + this.area + "|" + this.ltime + "|" + this.type;
	}
	@Override
	public int compareTo(EventBean o) {
			return 1;
	}
	
	@Override
	public String toString() {
		return user + "|" + area + "|" + time + "|" + ltime + "|" + type
				+ "|" + content + "|" + source + "|" + rzcontent + "|"
				+ orignalArea ;
	}
	
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String formatDate(long msel) {
		try {
			Date date = new Date(msel);
			formatter.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
			return formatter.format(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
}
