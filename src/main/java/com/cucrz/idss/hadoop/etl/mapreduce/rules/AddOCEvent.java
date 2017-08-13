package com.cucrz.idss.hadoop.etl.mapreduce.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.IETLRules;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleUtil;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

/**
 * @author wei
 *
 */
public class AddOCEvent implements IETLRules {
	static Set<String> set = new TreeSet<String>();
	static Map<String, String> conf = new HashMap<String, String>();
	static Logger log = Logger.getLogger(AddOCEvent.class);
	private static String POWER_ON_SUB_TIME = null;
	private static String POWER_ONOFF_INTERVAL_TIME = null;
	private static String POWER_OFF_POSTPOSTIVE_TIME = null;

	@Override
	public Set<String> executeRule(String user,Set<String> records, Context context,MultipleOutputs<Text, Text> mos,Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap) {
		conf = RuleUtil.getConf(context);
		 Counter reduceOC=counterMap.get("OCEvent");
		 Counter addOCInput=counterMap.get("addOCInput");
		POWER_ON_SUB_TIME = conf.get("idss_ETL_POWER_ON_SUB_TIME");
		POWER_ONOFF_INTERVAL_TIME = conf
				.get("idss_ETL_POWER_ONOFF_INTERVAL_TIME");
		POWER_OFF_POSTPOSTIVE_TIME = conf
				.get("idss_ETL_POWER_OFF_POSTPONED_TIME");
		records = addOCin2Events(reduceOC, records);
//		第一条不为00:00:00时间的非开机记录的增加开机记录  最后一条非关机记录的写入第二天缓存
		int index = records.size();
		int i = 0;
		List<String> quene=new ArrayList<String>();
		Iterator<String> it = records.iterator();
		while (it.hasNext()) {
			String line = it.next();
			 addOCInput.increment(1);
			if (FormCheck.checkLine(line)) {
				String[] splits = line.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String bizType = splits[2];// 业务类型
				 if(quene.size()>0&&!bizType.equals(TypeConstans.HEART_BEAT)){
						if(!bizType.equals(TypeConstans.OFF_CODE)&&!bizType.equals(TypeConstans.LAST_OFF_CODE)){
							 quene.clear();
							 quene.add(line);
						}else{
							quene.clear();
						}
					}
				 if(quene.size()==0&&!bizType.equals(TypeConstans.HEART_BEAT)){
						if(!bizType.equals(TypeConstans.OFF_CODE)&&!bizType.equals(TypeConstans.LAST_OFF_CODE)){
							 quene.clear();
							 quene.add(line);
						}	
				}
				 if(quene.size()==2){
					if(bizType.equals(TypeConstans.HEART_BEAT)){
						quene.remove(1);
						quene.add(line);
					}
				}
				 if(quene.size()==1){
					if(bizType.equals(TypeConstans.HEART_BEAT)){
							 quene.add(line);
					}
				}

				if (i == (index - 1) &&quene.size()>0) {
					String[] le;
					String heartBeat;
					String[] h;
					String lastStartTime="";
					String lastHeartBeatTime="";
					if(quene.size()==1){
						line=quene.get(0);
						le = line.split(OtherConstants.VERTICAL_DELIM_REGEX);
						bizType = le[2];// 业务类型
						lastStartTime = le[0];// 开始时间
					}else if(quene.size()==2){
						line=quene.get(0);
						le = line.split(OtherConstants.VERTICAL_DELIM_REGEX);
						bizType = le[2];// 业务类型
						lastStartTime = le[0];// 开始时间
						heartBeat=quene.get(1);
						h = heartBeat
								.split(OtherConstants.VERTICAL_DELIM_REGEX);
						lastHeartBeatTime = h[0];// 最后一个心跳开始时间
					}
					String time = lastStartTime.substring(0,11);
//					写出最后一条事件	
					try {
						String afterToday=DateUtil.getLaterDate(time, DateUtil.DATE_FORMATER);
						int firstDelim=line.indexOf(OtherConstants.VERTICAL_DELIM,20);
						String lastEvent=afterToday+" 00:00:00"+line.substring(firstDelim);
						if(!bizType.equals("3")&&!bizType.equals(TypeConstans.LAST_OFF_CODE)){
						mos.write(OutputPath.LAST_EVENT, null, new Text(user+OtherConstants.TAB_DELIM+lastEvent),  OutputPath.LAST_EVENT_PREFIX
								+ OutputPath.LAST_EVENT);
//							System.out.println("lastEvent========="+lastEvent);
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			i++;
		}
		return records;
	}

	// 间隔时间较长的事件添加开关机,并添加结束时间
	public Set<String> addOCin2Events(Counter reduceOC, Set<String> records) {
		List<String> quene = new ArrayList<String>();
		List<String> tmpQuene = new ArrayList<String>();
		List<String> removeQuene = new ArrayList<String>();
		Map<String, String> tmpMap = new HashMap<String, String>();
		Iterator<String> it = records.iterator();
		int index = records.size();
		int i = 0;
		while(it.hasNext()){
			String line = it.next();
			if (!RuleUtil.isAffectTime(line,1)) {
				String split[] = line
						.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String bizType = split[1];
				String sTime = DateUtil.getStartTime(line);
				String eTime = sTime;

				if (bizType.equals(TypeConstans.EVENT_NOT_AFFECT_ADS)) {
					String appearTime = split[3];
					try {
						Integer.valueOf(appearTime);
					} catch (Exception e) {
						appearTime = "";
					}
					if (!appearTime.equals("")) {
						eTime = DateUtil.timeCaculation(sTime, appearTime,
								"+");
					}
				}
				String[] times = { sTime, eTime };
				String newLine = addEvent(line, times, bizType, "");
				removeQuene.add(line);
				tmpQuene.add(newLine);
			}
		}
		for(String s:removeQuene){
			records.remove(s);
		}
		i = 0;
		index = records.size();
		it = records.iterator();
		while (it.hasNext()) {
			String line = it.next();
			if (FormCheck.checkLine(line)) {
				String[] splits = line.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String biz = splits[1];// 业务类型
				String startTime = splits[0];// 开始时间
			if (!biz.equals(TypeConstans.ON_CODE) && i == 0) {
				String time=startTime.substring(11,19);
				if(!time.equals("00:00:00")){
				if (isOverZeroTime(startTime) && i == 0) {
					String[] onTime = { DateUtil.timeCaculation(startTime,
							POWER_ON_SUB_TIME, "-") };
					String open = addEvent(line, onTime,
							TypeConstans.ON_CODE, "S");
					quene.add(open);
				} else {
					String[] onTime = { startTime.substring(0, 11)
							+ "00:00:00" };
					String open = addEvent(line, onTime,
							TypeConstans.ON_CODE, "S");
					quene.add(open);
				}
			}
			}
			if (quene.size() < 2) {
				if (RuleUtil.isAffectTime(line,1)) {
					quene.add(line);
				} 
				 if(quene.size() < 2&&i == (index - 1)&& (!biz.equals(TypeConstans.OFF_CODE))){
						String time = startTime.substring(0,11);
						int diff1 = Integer.valueOf(DateUtil.getResultOfTimes(time
								+ "23:59:59", startTime, "-"));
						if (diff1 > Integer.valueOf(POWER_ONOFF_INTERVAL_TIME)) {
							String[] offTime1 = { startTime,DateUtil.timeCaculation(startTime,
									POWER_OFF_POSTPOSTIVE_TIME, "+") };
							String last = addEvent(line, offTime1,
									biz, "");
							tmpMap.put(line, last);
							String[] offTime2={ DateUtil.timeCaculation(startTime,
									POWER_OFF_POSTPOSTIVE_TIME, "+"),DateUtil.timeCaculation(startTime,
									POWER_OFF_POSTPOSTIVE_TIME, "+") };
							String close = addEvent(line, offTime2,
									TypeConstans.LAST_OFF_CODE, "S");
							tmpQuene.add(close);
						} else {
									String[] offTime = {startTime, startTime.substring(0,11)+"23:59:59"};
									String last = addEvent(line, offTime,
											biz, "");
									tmpMap.put(line, last);
								}
					}
				 if(quene.size() < 2&&i == (index - 1)&& (biz.equals(TypeConstans.OFF_CODE))){
					 String[] times2 = new String[] { startTime, startTime };
					 String close = addEvent(line, times2,
							 biz, "");
						tmpMap.put(line, close);
				 }
			}			
			if (quene.size() == 2) {
				String e1 = quene.get(0);
				String e2 = quene.get(1);
				String ne1 = "";
				String ne2 = "";
				String s1Time = DateUtil.getStartTime(e1);
				String s2Time = DateUtil.getStartTime(e2);
				String[] times1 = { s1Time, s2Time };
				String[] times2 = { s2Time, s2Time };
				String split1[] = e1.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String split2[] = e2.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String bizType1 = split1[1];
				String bizType2 = split2[1];
				if (!(bizType1.equals(TypeConstans.OFF_CODE) && bizType2
						.equals(TypeConstans.ON_CODE))) {
					String diff = DateUtil
							.getResultOfTimes(s2Time, s1Time, "-");
					if (Integer.valueOf(diff) > Integer
							.valueOf(POWER_ONOFF_INTERVAL_TIME)) {
						// 间隔时间大于超时时间
						String closeTime = DateUtil.timeCaculation(s1Time,
								POWER_OFF_POSTPOSTIVE_TIME, "+");
						// 事件1的开始时间+配置的关机时间（5h）的一半作为关机事件的开始时间和结束时间
						String openTime = DateUtil.timeCaculation(s2Time,
								POWER_ON_SUB_TIME, "-");
						// 事件2的开始时间-配置的开机持续时间（15s）作为开机事件的开始时间
						String[] cTime = { closeTime, closeTime };
						String[] oTime = { openTime, s2Time };
						String closeEvent = addEvent(e1, cTime,
								TypeConstans.OFF_CODE, "S");
						String openEvent = addEvent(e2, oTime,
								TypeConstans.ON_CODE, "S");
						if (bizType1.equals(TypeConstans.OFF_CODE)) {
							times1 = new String[] { s1Time, s1Time };
							ne1 = addEvent(e1, times1, bizType1, "");
							tmpQuene.add(openEvent);
						}
						if (bizType1.equals(TypeConstans.ON_CODE)) {
							times1 = new String[] { s1Time, s1Time };
							ne1 = addEvent(e1, times1, bizType1, "");
							if (!bizType2.equals(TypeConstans.ON_CODE)) {
								tmpQuene.add(closeEvent);
								tmpQuene.add(openEvent);
							} else {
								tmpQuene.add(closeEvent);
							}
						}
						if (!isOCEvent(bizType1)) {
							times1 = new String[] { s1Time, closeTime };
							ne1 = addEvent(e1, times1, bizType1, "");
							if (bizType2.equals(TypeConstans.ON_CODE)) {
								tmpQuene.add(closeEvent);
							} else {
								tmpQuene.add(closeEvent);
								tmpQuene.add(openEvent);
							}
						}
					} else {
						// 间隔时间小于等于超时时间
						int diffTime = Integer.valueOf(diff);
						String closeTime = DateUtil.timeCaculation(s1Time,
								String.valueOf(Integer.valueOf(diffTime / 2)),
								"+");
						String openTime = "";
						if (diffTime > Integer.valueOf(POWER_ON_SUB_TIME)) {
							openTime = DateUtil.timeCaculation(s2Time,
									POWER_ON_SUB_TIME, "-");
						} else {
							openTime = s2Time;
						}
						String[] cTime = { closeTime, closeTime };
						String[] oTime = { openTime, s2Time };
						String closeEvent = addEvent(e1, cTime,
								TypeConstans.OFF_CODE, "S");
						String openEvent = addEvent(e2, oTime,
								TypeConstans.ON_CODE, "S");
						if (bizType2.equals(TypeConstans.ON_CODE)) {
							if (!isOCEvent(bizType1)) {
								times1 = new String[] { s1Time, closeTime };
							}
							if (bizType1.equals(TypeConstans.ON_CODE)) {
								times1 = new String[] { s1Time, s1Time };
							}
							ne1 = addEvent(e1, times1, bizType1, "");
							tmpQuene.add(closeEvent);
						}
						if (bizType1.equals(TypeConstans.OFF_CODE)
								&& !bizType2.equals(TypeConstans.ON_CODE)) {
							times1 = new String[] { s1Time, s1Time };
							ne1 = addEvent(e1, times1, bizType1, "");
							tmpQuene.add(openEvent);
						}
						ne1 = addEvent(e1, times1, bizType1, "");
					}
					tmpMap.put(e1, ne1);
					 if(i == (index - 1)&& (!bizType2.equals(TypeConstans.OFF_CODE))){		
						String[] times={s2Time,s2Time.substring(0,11)+"23:59:59"};
						ne2 = addEvent(e2, times, bizType2, "");
						tmpMap.put(e2, ne2);
						String time = s2Time.substring(0,11);
						int diff1 = Integer.valueOf(DateUtil.getResultOfTimes(time
								+ "23:59:59", s2Time, "-"));
						if (diff1 > Integer.valueOf(POWER_ONOFF_INTERVAL_TIME)) {
							String[] offTime1 = { s2Time,DateUtil.timeCaculation(s2Time,
									POWER_OFF_POSTPOSTIVE_TIME, "+") };
							String last = addEvent(line, offTime1,
									bizType2, "");
							tmpMap.put(e2, last);
							String[] offTime2={ DateUtil.timeCaculation(s2Time,
									POWER_OFF_POSTPOSTIVE_TIME, "+"),DateUtil.timeCaculation(s2Time,
									POWER_OFF_POSTPOSTIVE_TIME, "+") };
							String close = addEvent(line, offTime2,
									TypeConstans.LAST_OFF_CODE, "S");
							tmpQuene.add(close);
						} else {
									String[] offTime = {s2Time, s2Time.substring(0,11)+"23:59:59"};
									String last = addEvent(line, offTime,
											bizType2, "");
									tmpMap.put(e2, last);
								}
					}
					 if(i == (index - 1)&& (bizType2.equals(TypeConstans.OFF_CODE))){
						 times2 = new String[] { s2Time, s2Time };
						 String close = addEvent(line, times2,
									bizType2, "");
							tmpMap.put(e2, close);
					 }
					quene.remove(0);
				} else {
					times1 = new String[] { s1Time, s1Time };
					times2 = new String[] { s2Time, s2Time };
					ne1 = addEvent(e1, times1, bizType1, "");
					ne2 = addEvent(e2, times2, bizType2, "");
					tmpMap.put(e1, ne1);
					tmpMap.put(e2, ne2);
					quene.remove(0);
				}
			}
			i++;
			}
		}
		Set<String> keys = tmpMap.keySet();
		for (String s : keys) {
			records.remove(s);
			records.add(tmpMap.get(s));
		}
		for (String s : tmpQuene) {
			records.add(s);
			 reduceOC.increment(1);
		}
		return records;
	}

	// 根据时间，事件类型添加事件
	public String addEvent(String record, String[] time, String bizType,
			String type) {
		String split[] = record.split(OtherConstants.VERTICAL_DELIM_REGEX,20);
		String newRecord = "";
		for (int i = 2; i < split.length; i++) {
			newRecord = newRecord + split[i] + OtherConstants.VERTICAL_DELIM;
		}
		int last = newRecord.lastIndexOf(OtherConstants.VERTICAL_DELIM);
		newRecord=newRecord.substring(0, last);
		if (time != null && time.length > 0) {
			if (bizType.equals(TypeConstans.OFF_CODE)
					|| bizType.equals(TypeConstans.ON_CODE)|| bizType.equals(TypeConstans.LAST_OFF_CODE)) {
				if (time.length < 2) {
					// 添加开始时间
					newRecord = time[0] + OtherConstants.VERTICAL_DELIM
							+ bizType + OtherConstants.VERTICAL_DELIM +  "S";
				} else {
					if (type.equals("S") || record.endsWith("S")) {
						newRecord = time[0] + OtherConstants.VERTICAL_DELIM
								+ time[1] + OtherConstants.VERTICAL_DELIM
								+ bizType + OtherConstants.VERTICAL_DELIM +"S";
					} else {
						newRecord = time[0] + OtherConstants.VERTICAL_DELIM
								+ time[1] + OtherConstants.VERTICAL_DELIM
								+ bizType + OtherConstants.VERTICAL_DELIM
								+newRecord;
					}
				}
			} else {
				if (time.length < 2) {
					// 添加开始时间
					newRecord = time[0] + OtherConstants.VERTICAL_DELIM
							+ bizType + OtherConstants.VERTICAL_DELIM
							+ newRecord;
				} else {
					newRecord = time[0] + OtherConstants.VERTICAL_DELIM
							+ time[1] + OtherConstants.VERTICAL_DELIM + bizType
							+ OtherConstants.VERTICAL_DELIM + newRecord;

				}
			}
			return newRecord;
		} else {
			log.warn("添加事件的输入时间不正确，返回原始记录");
			return record;
		}
	}

	public Map<String, String> addNonAffectTimeEvent(String line,
			Set<String> records) {
		Map<String, String> map = new HashMap<String, String>();

		return map;
	}

	public boolean isOverZeroTime(String firstStartTime) {
		String time = firstStartTime.substring(11);
		long seconds = DateUtil.getSecondsInDay(time);

		long intervalTime = Integer.parseInt(POWER_ON_SUB_TIME);
		if (seconds > intervalTime) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isOCEvent(String bizType) {
		if (bizType.equals(TypeConstans.OFF_CODE)
				|| bizType.equals(TypeConstans.ON_CODE)) {
			return true;
		} else {
			return false;
		}
	}

	

	public static void main(String[] args) {
		 set.add("2014-11-18 12:56:12|257|6054084|UN");
//		 set.add("2014-11-18 13:28:31|3|5");
		set.add("2014-11-18 23:00:00|3|");
		set.add("2014-11-18 22:35:45|257|||0|0|015752A1-0000-0000-0000-000000000000||635519181453580000");
//		set.add("2014-11-18 22:35:52|257|2||0|0|02179561-0000-0000-0000-000000000000||635519181525010000");
		set.add("2014-11-18 23:44:59|3|2");
//		set.add("2014-11-18 23:56:59|3|1");
		AddOCEvent add = new AddOCEvent();
		Set<String> s = add.executeRule(null,set, null, null,null,null);
		Iterator<String> it = s.iterator();
		while (it.hasNext()) {
			String line = it.next();
			System.out.println(line);
		}
//		AddOCEvent a= new AddOCEvent();
//		String[] b={"16:35:45","19:35:45"};
//		String c =a.addEvent("2014-11-18 16:35:45|1|2", b, "1", "");
//		System.out.println(c);
//		String line ="2015-05-05 12:00:00|dvxzdfbvxdf|sdfrgs|";
//		String afterToday=DateUtil.getLaterDate("2015-05-05", DateUtil.DATE_FORMATER);
//		int firstDelim=line.indexOf(OtherConstants.VERTICAL_DELIM);
//		line=afterToday+" 00:00:00"+line.substring(firstDelim);
//	System.out.println(line);	
	}
		


}
