package com.cucrz.idss.hadoop.etl.mapreduce.in.path;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.in.IOperPath;

public class OperatorPath {
	private static Logger log=Logger.getLogger(OperatorPath.class);
	
	@SuppressWarnings("unchecked")
	public static Path[] getPath(String operatorName,String operID,String dateID){
		Class<IOperPath> clazz;
		try {
			clazz=(Class<IOperPath>) Class.forName(operatorName);
			IOperPath op=clazz.newInstance();
			return op.getPath(operatorName,operID,dateID);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
		log.warn("运营商名称不正确，无法获得自用路径！");
		return null;
		}
	}
}
