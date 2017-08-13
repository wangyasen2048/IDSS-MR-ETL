package com.cucrz.idss.hadoop.etl.mapreduce.out;

import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public interface IETLOutput {
	void outputRecords(Set<String> records ,String userID, MultipleOutputs<Text, Text> mos,Context context);
	

}
