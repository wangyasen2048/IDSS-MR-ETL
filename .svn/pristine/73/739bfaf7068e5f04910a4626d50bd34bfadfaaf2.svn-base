package com.cucrz.idss.hadoop.etl.mapreduce.job.baseETL;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.reduce.IETLReducer;
import com.cucrz.idss.hadoop.etl.mapreduce.reduce.impl.ETLReducer;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class DataETLReducer extends Reducer<Text, Text, Text, Text> {
	private static Logger log = Logger.getLogger(DataEtlJob.class);
	private static Map<String, Map<String, String>> storage = new HashMap<String, Map<String, String>>();
	private static Map<String, String> sampleList = new HashMap<String, String>();
	private static Map<String, String> userList = new HashMap<String, String>();
	private static Map<String, Counter> counterMap = new HashMap<String, Counter>();
	private static MultipleOutputs<Text, Text> mos;
	private static Set<String> records = null;
	private static IETLReducer etlReducer = null;

	// 创建MultipleOutputs对象
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		try {
			etlReducer = new ETLReducer();
		} catch (Exception e1) {
			log.warn("实例化解析类错误！");
		}
		// 创建计数器
		counterMap = etlReducer.etlRedSetupGetCounter(context);
		// 加载分布式缓存
		if (context.getConfiguration().get("dcOpen").equals("true")) {
			log.info("开始加载reduce分布式缓存！");
			storage = etlReducer.etlReduceSetupDisCache(context, storage);
			log.info("reduce分布式缓存加载完成！");
		}
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Counter TotalUser = counterMap.get("TotalUser");
		Counter WrongRows = counterMap.get("WrongRows");
		Counter reduceInput = counterMap.get("reduceInput");
		Counter newChannel = counterMap.get("newChannel");
		Counter newCloumn = counterMap.get("newCloumn");
		Counter newUser = counterMap.get("newUser");
		records = new TreeSet<String>(new RuleUtil.RecordCompartor());
		String inputDateID = context.getConfiguration().get("inputDateID");	
		String isOutputNewUser = context.getConfiguration().get("idss_ETL_NewUserList");	
		String isOutputSample = context.getConfiguration().get(
				"idss_ETL_Rule_SampleUser");
		String dcOpen = context.getConfiguration().get("dcOpen");
		if (key.toString().equals("WrongDataRow")) {
			Iterator<Text> ite = values.iterator();
			while (ite.hasNext()) {
				Text row = ite.next();
				mos.write(OutputPath.WRONG, null, row, 
						 OutputPath.WRONG_PREFIX
						+ OutputPath.WRONG);
				WrongRows.increment(1);
			}			
		} else if (key.toString().equals("newChannel")) {
			Iterator<Text> ite = values.iterator();
			while (ite.hasNext()) {
				Text row = ite.next();
				mos.write(OutputPath.NEW_CHANNLE, null, row, OutputPath.NEW_CHANNLE_PREFIX
						+ OutputPath.NEW_CHANNLE);
				newChannel.increment(1);
			}
		} else if (key.toString().equals("newCloumn")) {
			Iterator<Text> ite = values.iterator();
			while (ite.hasNext()) {
				Text row = ite.next();
				mos.write(OutputPath.NEWCLOUMN, null, row,  OutputPath.NEWCLOUMN_PREFIX
						+ OutputPath.NEWCLOUMN);
				newCloumn.increment(1);
			}
		} else {	
			String[] split = key.toString().split(OtherConstants.VERTICAL_DELIM_REGEX);	
				if (isOutputSample.equals("true") && dcOpen.equals("true")) {
					Counter SampleUser = counterMap.get("SampleUser");
					context.getConfiguration().set("inputDateID", "sample");
					sampleList = storage.get(context.getConfiguration().get(
							"idss_ETL_Cache_SampleListFilePath"));
					if(sampleList!=null){
						Set<String> set=sampleList.keySet();
						Iterator<String> it = set.iterator();
						if(it.hasNext()){
						String one=it.next();
						String sampleSplit[]=one.split(OtherConstants.EXCLAMATION_DELIM_REGEX);
						if(sampleSplit.length<2){
							if (sampleList.containsKey(split[0])) {
								Set<String> sampleRecords = new TreeSet<String>(
										new RuleUtil.RecordCompartor());
								Iterator<Text> ite = values.iterator();
								while (ite.hasNext()) {
									String sr = ite.next().toString();
									sampleRecords.add(sr);
									records.add(sr);
									reduceInput.increment(1);
								}
								etlReducer.etlReducer(key.toString(), sampleRecords,
										context, mos, inputDateID, counterMap);
								SampleUser.increment(1);
							}
						}else {
							if (sampleList.containsKey(split[0]+"!"+split[1])) {
								Set<String> sampleRecords = new TreeSet<String>(
										new RuleUtil.RecordCompartor());
								Iterator<Text> ite = values.iterator();
								while (ite.hasNext()) {
									String sr = ite.next().toString();
									sampleRecords.add(sr);
									records.add(sr);
									reduceInput.increment(1);
								}
								etlReducer.etlReducer(key.toString(), sampleRecords,
										context, mos, inputDateID, counterMap);
								SampleUser.increment(1);
							}
						}
						}
				}
				}
				context.getConfiguration().set("inputDateID", "whole");
				Iterator<Text> ite = values.iterator();
				while (ite.hasNext()) {
					String sr = ite.next().toString();
					records.add(sr);
					reduceInput.increment(1);
				}
				etlReducer.etlReducer(key.toString(), records, context, mos,
						inputDateID, counterMap);
				TotalUser.increment(1);
				if(isOutputNewUser.equals("true")){
					userList = storage.get(context.getConfiguration().get(
							"idss_ETL_Cache_UserListFilePath"));
				if(ReadCacheUtil.isNewUser(userList, split[0], split[1])){
					mos.write(OutputPath.NEW_USER, null, new Text(split[0]+"!"+split[1]),  OutputPath.NEW_USER_PREFIX
							+ OutputPath.NEW_USER);
					newUser.increment(1);
				}
				}
				
		}
	}

	// 关闭MultipleOutputs对象
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
