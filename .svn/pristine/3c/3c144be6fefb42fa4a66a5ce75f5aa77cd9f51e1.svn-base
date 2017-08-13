package com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleUtil.RecordCompartor;

public class RuleFactory {
	
	private static Logger log = Logger.getLogger(RuleFactory.class);
	private static Map<String,IETLRules> rule = new TreeMap<String,IETLRules>(new RecordCompartor());
	private static  final Set<String> defaultRule = 
			    new TreeSet<String>(){{
				add("2|true|com.cucrz.idss.hadoop.etl.mapreduce.rules.AddOCEvent");
	}};
	
	public static Map<String,IETLRules> getInstance(Set<String> set){
		if(null==set||set.isEmpty()){
			rule=createRule(defaultRule);
		}else{
			rule=createRule(set);
		}
		return rule;
	}
	
	public static Map<String,IETLRules> createRule(Set<String> ruleSet){
		

			for(String ruleName:ruleSet){
				String[] split=ruleName.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String name="";
				if(split.length>1){
				if(split[1].equals("true")){
					name=split[2];				
			try {
				Class<IETLRules> clazz = (Class<IETLRules>)Class.forName(name);
				try {
					rule.put(ruleName, (IETLRules)clazz.newInstance());
				} catch (InstantiationException e) {
					
					e.printStackTrace();
				} catch (IllegalAccessException e) {				
					e.printStackTrace();
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}	
				}
				}
				}
			
			return rule;
	}

}
