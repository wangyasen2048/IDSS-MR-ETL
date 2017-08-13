package com.cucrz.idss.hadoop.etl.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class ReadProperty {
	 public static Map<String, String> properties = new TreeMap<String, String>(new Comparator<String>() {

		@Override
		public int compare(String o1, String o2) {
			return o1.compareTo(o2);
		}
	});
	 public static Logger log = Logger.getLogger(ReadProperty.class);
	
	/**
	 * 读取本地的property配置文件，用作调试使用<br/>
	 * 在部署服务器，应该使用readProperty这个方法
	 * @return
	 */
	public static Map<String, String> readPropertyLocal(String fileName) {
		File file = new File(fileName);
		try {
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file));
			BufferedReader br = new BufferedReader(isr);
			String line = br.readLine();

			while (line != null) {
				if (line.indexOf("=") > 0&&!line.startsWith("#")) {
					String[] splits = line.split("=",2);
					getWholeFormName(splits[0], splits[1]);
				}
				line = br.readLine();
			}
			Set<String> set = properties.keySet();
			Iterator<String> iter = set.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				log.info(key + "===" + properties.get(key));
			}
		} catch (IOException e) {
			log.warn("=====配置文件不正确！=====");
			e.printStackTrace();
		}
		return properties;
	}
//读取指定路径下的文件，并写成keyvalue形式并存入getWholeFormName的properties
	public static Map<String,String> readProperty(String path,Configuration conf){	
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fis =fs.open(new Path(path));
			InputStreamReader isr  = new InputStreamReader(fis); 
			BufferedReader br = new BufferedReader(isr);
			String line=br.readLine();
			
		    while(line!=null){
		    	if(line.indexOf("=")>0&&!line.startsWith("#")){
		    	String[] splits = line.split("=",2);
		    	getWholeFormName(splits[0], splits[1]);	
		    	
		    }
		    	line=br.readLine();		    	
			}
		    
		    
			Set<String> set =properties.keySet();		
			Iterator<String> iter =set.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				log.info(key + "===" + properties.get(key));
			}
		} catch (IOException e) { 
			log.warn("=====配置文件不正确！=====");
			e.printStackTrace();
		}
		return properties;
	}
	 //补全类名，以便调用，并且放入properties
	public static void getWholeFormName(String name,String value){
		if(name.equals("idss_ETL_Form_Hive")){
			value="1|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.out.form.hiveEvent";			
		}
		if(name.equals("idss_ETL_Form_Event1")){
			value="2|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.out.form.event1";
			
		}
		if(name.equals("idss_ETL_Form_Event2")){
			value="3|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.out.form.event2";
		}
		if(name.equals("idss_ETL_Form_Source")){
			value="0|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.out.form.source";
		}
		if(name.equals("idss_ETL_Rule_MergeSameEvent")){
			value="1|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.rules.MergeSameEvent";
		}
		if(name.equals("idss_ETL_Rule_OC")){
			value="2|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.rules.AddOCEvent";
		}
		if(name.equals("idss_ETL_Rule_LoseHeartBeat")){
			value="3|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.rules.LoseHeartBeat";
		}
		if(name.equals("idss_ETL_Rule_NoChannel")){
			value="4|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.rules.NoChannel";
		}
		if(name.equals("idss_ETL_Rule_FindNewBaseData")){
			value="9|"+value.trim()+"|com.cucrz.idss.hadoop.etl.mapreduce.rules.NewBaseData";
		}
		properties.put(name, value);
	}
	//核对操作运营商信息
	public static boolean checkOperatorINFO(String operatorID,String operatorName){
		Map<String,String> operatorMap= new HashMap<String, String>();
		try {
			Class operator=Class.forName("com.cucrz.idss.hadoop.etl.operator.Operator");
			Field[] fields=operator.getFields();
			for(Field field:fields){
				String fieldName=field.getName();
				String value=(String) field.get(operator);
				operatorMap.put(fieldName, value);
			}
		} catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException e) {
			System.err.println("运营商对照类不存在！！");
			e.printStackTrace();
		}
		if(operatorMap.containsKey(operatorName)){
				String value=operatorMap.get(operatorName);
				if(value.equals(operatorID)){
					return true;
				}
			
		}else{
			System.out.println("-------输入的运营商名称--"+operatorName+"不存在！---------");
			
		}
		System.out.println("--------运营商ID与运营商名称不匹配----------");
		System.out.println("--------请输入以下运营商和ID之一：----------");
		Iterator<String> it=operatorMap.keySet().iterator();
		while(it.hasNext()){
			String key=it.next();
			System.out.println(key+"===="+operatorMap.get(key));
		}
		return false;
	}
}