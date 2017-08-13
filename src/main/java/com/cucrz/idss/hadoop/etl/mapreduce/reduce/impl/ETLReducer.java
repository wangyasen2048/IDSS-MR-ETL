package com.cucrz.idss.hadoop.etl.mapreduce.reduce.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.out.IETLOutput;
import com.cucrz.idss.hadoop.etl.mapreduce.out.FormFactory;
import com.cucrz.idss.hadoop.etl.mapreduce.reduce.IETLReducer;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.IETLRules;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleFactory;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class ETLReducer implements IETLReducer{
	private static Logger log = Logger.getLogger(ETLReducer.class);
	private static Map<String,IETLRules> etlRules = null;
	private static Map<String,IETLOutput> etlOutput = null;
	private static Map<String, Map<String, String>> resultMap=null; 
	//reduce计数器放入map
	@Override
	public  Map<String, Counter> etlRedSetupGetCounter(Context context) {
		Map<String, Counter> map = new HashMap<String, Counter>();
		Counter reduceEmptyChannel = (Counter) context.getCounter("reduce", "EmptyChannel");
		Counter reduceLoseEmptyChannel = (Counter) context.getCounter("reduce", "LoseEmptyChannel");
		Counter reduceOC = (Counter) context.getCounter("reduce", "OCEvent");
		Counter TotalUser = (Counter) context.getCounter("reduce", "TotalUser");
		Counter SampleUser = (Counter) context.getCounter("reduce", "SampleUser");
		Counter reduceInput = (Counter) context.getCounter("reduce", "reduceInput");
		Counter WrongRows = (Counter) context.getCounter("reduce", "WrongRows");
		Counter addOCInput = (Counter) context.getCounter("reduce", "addOCInput");
		Counter newChannel = (Counter) context.getCounter("reduce", "newChannel");
		Counter newCloumn = (Counter) context.getCounter("reduce", "newCloumn");
		Counter newUser = (Counter) context.getCounter("reduce", "newUser");
		Counter MergeLosedRows = (Counter) context.getCounter("reduce", "MergeLosedRows");
		Counter LoseHeartBeat = (Counter) context.getCounter("reduce", "LoseHeartBeat");
		map.put("newCloumn", newCloumn);
		map.put("newChannel", newChannel);
		map.put("newUser", newUser);
		map.put("LoseEmptyChannel", reduceLoseEmptyChannel);
		map.put("EmptyChannel", reduceEmptyChannel);
		map.put("OCEvent", reduceOC);
		map.put("TotalUser", TotalUser);
		map.put("SampleUser", SampleUser);
		map.put("reduceInput", reduceInput);
		map.put("WrongRows", WrongRows);
		map.put("addOCInput", addOCInput);
		map.put("MergeLosedRows", MergeLosedRows);
		map.put("LoseHeartBeat", LoseHeartBeat);
		return map;
	}
	//加载分布式缓存
	@Override
	public Map<String, Map<String, String>> etlReduceSetupDisCache(Context context,
			Map<String, Map<String, String>> storage) {
		resultMap = new HashMap<String, Map<String, String>>();
		Set<String> cacheFileNameList=ReadCacheUtil.getPropertiesByRegex("^idss_ETL_Cache", context);
		for(String cacheFileName:cacheFileNameList){
		try {
		BufferedReader reader = new BufferedReader(new FileReader(cacheFileName));
			resultMap.put(cacheFileName, ReadCacheUtil.readCache(reader, cacheFileName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
		return resultMap;
	}

	//reduce
	@Override
	public void etlReducer(String user, Set<String> records,Context context, MultipleOutputs<Text, Text> mos, String inputDateID,
			Map<String, Counter> counterMap) {
		Set<String>  ruleSet=new TreeSet<String>();
		Set<String>  sourceSet=new TreeSet<String>(new RuleUtil.RecordCompartor());
		sourceSet.addAll(records);
		ruleSet=ReadCacheUtil.getPropertiesByRegex("^idss_ETL_Rule_", context);
		etlRules= RuleFactory.getInstance(ruleSet);
		Set<String> rulekeyset = etlRules.keySet();
		for(String name:rulekeyset){
			IETLRules clazz=etlRules.get(name);
			records=clazz.executeRule(user,records, context,mos,resultMap,counterMap);
		}

		Set<String> formset=new TreeSet<String>();
		formset=ReadCacheUtil.getPropertiesByRegex("^idss_ETL_Form_", context);
		etlOutput=FormFactory.getInstance(formset);
		Set<String> formkeyset=etlOutput.keySet();
		for(String name:formkeyset){
			if(name.equals("0|true|com.cucrz.idss.hadoop.etl.mapreduce.out.form.source")){
				IETLOutput clazz=(IETLOutput)etlOutput.get(name);
				clazz.outputRecords(sourceSet,user, mos,context);
			}else{
			IETLOutput clazz=(IETLOutput)etlOutput.get(name);
			clazz.outputRecords(records,user, mos,context);
		}
		}
	}
}
