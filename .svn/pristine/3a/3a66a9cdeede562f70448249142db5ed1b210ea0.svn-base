package com.cucrz.idss.hadoop.etl.mapreduce.job.program;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ProgramReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context){
		
		for(Text value : values){
			try {
				context.write(null, value);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
