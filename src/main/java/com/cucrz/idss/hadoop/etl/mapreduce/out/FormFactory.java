package com.cucrz.idss.hadoop.etl.mapreduce.out;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.RuleUtil.RecordCompartor;

public class FormFactory {
	
	private static Logger log = Logger.getLogger(FormFactory.class);
	private static Map<String,IETLOutput> form = new TreeMap<String,IETLOutput>(new RecordCompartor());
	private static  final Set<String> defaultForm = 
		    new TreeSet<String>(){{
				add("1|true|com.cucrz.idss.hadoop.etl.mapreduce.out.form.hiveEvent");
		    }};	
	
	public static Map<String,IETLOutput> getInstance(Set<String> set){
		if(null==set||set.isEmpty()){
			form=createform(defaultForm);
		}else{
			form=createform(set);
		}
		return form;
	}
	
	public static Map<String,IETLOutput> createform(Set<String> formSet){
			for(String formName:formSet){
				String[] split=formName.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String name="";
				
				if(split.length>2&&split[1].equals("true")){
			
					name=split[2];
			try {
				Class clazz = Class.forName(name);
				try {
					form.put(formName, (IETLOutput)clazz.newInstance());
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				log.warn("The output form is not right!");
			}	
			}
			}
			return form;
	}
}