package com.cucrz.idss.hadoop.etl.mapreduce.in;

import org.apache.hadoop.fs.Path;

public interface IOperPath {
	Path[] getPath(String operatorName,String operID,String dateID);
}
