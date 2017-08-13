package com.cucrz.idss.hadoop.etl.mapreduce.map;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ETLMapperFactory {
	private static final String defaultAction = "com.cucrz.idss.hadoop.etl.mapreduce.map.gehua.EtlDataAction";
	private static Logger log = Logger.getLogger(ETLMapperFactory.class);
	private Map<String, Object> operMapEtls = new HashMap<String, Object>();
	
	//将实例库放在Map<String, Object> operMapEtls中
	public  ETLMapperFactory(String operatorName,String operatorID){
		if(operMapEtls.get(operatorName)==null){
			Class<IETLMapper> clazz;
			try {
				clazz = (Class<IETLMapper>) Class.forName(operatorName);
				operMapEtls.put(operatorID, clazz);
			} catch (ClassNotFoundException e) {
			log.warn("operator Action Class not found!");
				e.printStackTrace();
			}
			
		}
	}
	//判断输入是否合法，合法返回ETLMapperFactory实例，不合法返回默认值
	public static ETLMapperFactory getInstance(String operatorName,String operatorID) throws Exception {
		if(null == operatorName || "".equals(operatorName.trim())){
			return new ETLMapperFactory(defaultAction , operatorID);
		}else{
			return new ETLMapperFactory(operatorName , operatorID);
		}
	}
	//创建实例，返回类型IETLMapper
	public IETLMapper createETL(String operatorID) {
		Class<?> clazz = (Class<?>) operMapEtls.get(operatorID);
		try {
			return (IETLMapper) clazz.newInstance();
		} catch (Exception e) {
			log.warn("operator name is not right!");
			throw new RuntimeException(e);
		}
	}
}
