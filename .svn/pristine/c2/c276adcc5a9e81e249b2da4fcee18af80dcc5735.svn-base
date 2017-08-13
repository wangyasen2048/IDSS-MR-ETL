package com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public interface IETLRules {

	Set<String> executeRule(String user, Set<String> records, Context context,
			MultipleOutputs<Text, Text> mos,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap);

}
