package com.cucrz.idss.hadoop.etl.mapreduce.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class MergeSameEvent  implements IETLRules {
	static Map<String, String> conf = new HashMap<String, String>();
	static Logger log = Logger.getLogger(AddOCEvent.class);
	private static String SAME_EVENT_MERGE_TIME = null;
	@Override
	public Set<String> executeRule(String user,Set<String> records, Context context,MultipleOutputs<Text, Text> mos,Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap) {
		conf = RuleUtil.getConf(context);
		 Counter MergeLosedRows=counterMap.get("MergeLosedRows");
		SAME_EVENT_MERGE_TIME = conf.get("idss_ETL_SAME_EVENT_MERGE_TIME");
		List<String> quene = new ArrayList<String>();
		Map<String, String> tmpMap = new HashMap<String, String>();
		Iterator<String> it = records.iterator();
		while (it.hasNext()) {
			String line = it.next();
			if (quene.size() < 2) {
				if (RuleUtil.isAffectTime(line,1)) {
					quene.add(line);
				}
			}
			if (quene.size() == 2) {
				String e1 = quene.get(0);
				String e2 = quene.get(1);
				String s1Time = DateUtil.getStartTime(e1);
				String s2Time = DateUtil.getStartTime(e2);
				String event1 = e1.substring(e1.indexOf(OtherConstants.VERTICAL_DELIM));
				String event2 = e2.substring(e2.indexOf(OtherConstants.VERTICAL_DELIM));
				if(event1.equals(event2)){
					String diff = DateUtil
							.getResultOfTimes(s2Time, s1Time, "-");
					if(Integer.valueOf(diff)<Integer.valueOf(SAME_EVENT_MERGE_TIME)){
						String heartEvent=s2Time+OtherConstants.VERTICAL_DELIM+TypeConstans.HEART_BEAT+OtherConstants.VERTICAL_DELIM+"1";
						quene.remove(0);
						tmpMap.put(e2, heartEvent);
					}else{
						quene.remove(0);
					}
				}else{
					quene.remove(0);
				}
			}
		}
		Set<String> keys = tmpMap.keySet();
		for (String s : keys) {
			records.remove(s);
			MergeLosedRows.increment(1);
			records.add(tmpMap.get(s));
		}
		
		return records;
	}
}
