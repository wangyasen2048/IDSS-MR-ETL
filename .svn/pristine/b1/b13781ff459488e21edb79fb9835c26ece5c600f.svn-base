package com.cucrz.idss.hadoop.etl.mapreduce.in.path.guangdong;

import java.io.File;

import org.apache.hadoop.fs.Path;

import com.cucrz.idss.hadoop.etl.mapreduce.in.IOperPath;

public class guangdong implements IOperPath {
	private static String basePath="cucrz/data";

	@Override
	public Path[] getPath(String operatorName, String operID, String dateID) {
		String look=basePath+File.separator+"look"+File.separator+operID+File.separator+dateID.substring(0,4)+File.separator+dateID.substring(4,6)+File.separator+dateID.substring(6,8);
		String close=basePath+File.separator+"close"+File.separator+operID+File.separator+dateID.substring(0,4)+File.separator+dateID.substring(4,6)+File.separator+dateID.substring(6,8);
		Path[] paths=new Path[]{new Path(look),new Path(close)};
		return paths;
	}

}
