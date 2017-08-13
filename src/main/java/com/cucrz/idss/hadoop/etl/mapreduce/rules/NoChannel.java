package com.cucrz.idss.hadoop.etl.mapreduce.rules;

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
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.IETLRules;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleUtil;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class NoChannel implements IETLRules {

	static Map<String, String> conf = new HashMap<String, String>();
	static Logger log = Logger.getLogger(AddOCEvent.class);

	@Override
	public Set<String> executeRule(String user, Set<String> records,
			Context context, MultipleOutputs<Text, Text> mos,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap) {
		conf = RuleUtil.getConf(context);
		Counter reduceEmptyChannel = counterMap.get("EmptyChannel");
		Counter reduceLoseEmptyChannel = counterMap.get("LoseEmptyChannel");
		String NOCHANNEL_INTERVAL_TIME = conf
				.get("idss_ETL_NOCHANNEL_INTERVAL_TIME");
		int index = records.size();
		int i = 0;
		Map<String, String> tmpMap = new HashMap<String, String>();
		List<String> quene = new ArrayList<String>();
		Iterator<String> it = records.iterator();
		while (it.hasNext()) {
			String line = it.next();
			if (FormCheck.checkLine(line)) {
				String split[] = line
						.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String bizType = split[2];
				String sTime = DateUtil.getStartTime(line);
				String eTime = DateUtil.getEndTime(line);
				if (bizType.equals(TypeConstans.EVENT_LIVE__DATA_BIZ)
						|| bizType
								.equals(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ)
						|| bizType.equals(TypeConstans.EVENT_LIVE_DEFAULTTV)
						|| bizType.equals(TypeConstans.EVENT_LIVE_BIZ)) {
					String diffTime = DateUtil.getResultOfTimes(eTime, sTime,
							"-");
					if (Integer.valueOf(diffTime) < Integer
							.valueOf(NOCHANNEL_INTERVAL_TIME)) {
						String newECEvent = addEmptyChannel(sTime, eTime, line);
						tmpMap.put(line, newECEvent);
					}
				}
			}
		}
		Set<String> keys = tmpMap.keySet();
		for (String k : keys) {
			records.remove(k);
			reduceLoseEmptyChannel.increment(1);
			String newrecord = tmpMap.get(k);
			if (!newrecord.equals("")) {
				records.add(newrecord);
				reduceEmptyChannel.increment(1);
			}
		}

		records = removeEmpty(records);
		return records;
	}

	// 添加空频道事件
	public String addEmptyChannel(String startTime, String endTime,
			String record) {
		String newrecord = "";
		String[] split = record.split(OtherConstants.VERTICAL_DELIM_REGEX);
		newrecord = startTime + OtherConstants.VERTICAL_DELIM + endTime
				+ OtherConstants.VERTICAL_DELIM + TypeConstans.EMPTY_CHANNEL
				+ OtherConstants.VERTICAL_DELIM + "E";
		return newrecord;
	}

	// 去出重复空频道
	public Set<String> removeEmpty(Set<String> records) {
		List<String> tmpList1 = new ArrayList<String>();
		List<String> quene = new ArrayList<String>();
		List<String> tmpList2 = new ArrayList<String>();
		int index = records.size();
		int i = 0;
		Iterator<String> it = records.iterator();
		while (it.hasNext()) {
			String line = it.next();
			if (quene.size() < 2) {
				quene.add(line);
			}
			if (quene.size() == 2) {
				String e1 = quene.get(0);
				String e2 = quene.get(1);
				String split1[] = e1.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String split2[] = e2.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String bizType1 = split1[2];
				String bizType2 = split2[2];
				String s1Time = DateUtil.getStartTime(e1);
				String e2Time = DateUtil.getEndTime(e2);
				String newECEvent = "";
				if (!bizType1.equals(TypeConstans.EMPTY_CHANNEL)) {
					quene.remove(0);
				} else {
					if (bizType2.equals(TypeConstans.EMPTY_CHANNEL)) {
						newECEvent = addEmptyChannel(s1Time, e2Time, e1);
						tmpList1.add(e1);
						tmpList1.add(e2);
						quene.clear();
						quene.add(newECEvent);
					} else {
						tmpList2.add(quene.get(0));
						quene.clear();
					}
				}
			}
			if (i == (index - 1)) {
				if (quene.size() == 1) {
					String e1 = quene.get(0);
					String split1[] = e1
							.split(OtherConstants.VERTICAL_DELIM_REGEX);
					String bizType1 = split1[2];
					if (bizType1.equals(TypeConstans.EMPTY_CHANNEL)) {
						tmpList2.add(e1);
					}
				}
			}
			i++;
		}
		for (String k : tmpList2) {
			records.add(k);
		}
		for (String k : tmpList1) {
			records.remove(k);
		}

		return records;
	}

	// 判断是否为收视事件类型
	public boolean isProgramEvent(String bizType) {
		if (bizType.equals(TypeConstans.OFF_CODE)
				|| bizType.equals(TypeConstans.ON_CODE)
				|| bizType.equals(TypeConstans.HEART_BEAT)) {
			return false;
		} else {
			return true;
		}
	}

	// public static void main(String[] args) {
	// NoChannel nc = new NoChannel();
	// Set<String> set = new TreeSet<String>();
	// // set.add("2014-11-19 05:21:40|2014-11-19 05:21:41|1|S");
	// // set.add("2014-11-19 05:21:41|2014-11-19 05:21:45|257|1");
	// set.add("2014-11-19 05:21:45|2014-11-19 05:21:46|257|2");
	// set.add("2014-11-19 05:21:46|2014-11-19 05:21:50|771|3");
	// // // set.add("2014-11-19 05:21:50|2014-11-19 05:21:51|771|4");
	// // set.add("2014-11-19 05:21:51|2014-11-19 05:21:52|257|5");
	// // // set.add("2014-11-19 05:21:52|2014-11-19 05:21:53|257|6");
	// // // set.add("2014-11-19 05:21:53|2014-11-19 05:21:54|257|7");
	// // set.add("2014-11-19 05:21:54|2014-11-19 05:21:55|771|8");
	// // // set.add("2014-11-19 05:21:55|2014-11-19 05:21:55|771|9");
	// set = nc.executeRule(null,set, null, null);
	// Iterator<String> it = set.iterator();
	// while (it.hasNext()) {
	// String line = it.next();
	// System.out.println(line);
	// }
	// }
}
