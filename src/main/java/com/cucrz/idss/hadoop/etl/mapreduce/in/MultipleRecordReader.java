package com.cucrz.idss.hadoop.etl.mapreduce.in;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MultipleRecordReader extends RecordReader<Text ,Text>{

	private Text key = new Text();
	private Text value = new Text();
	private String line ="";
	private BufferedReader br = null;
	
	@Override
	public void close() throws IOException {
		br.close();
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		return 0;
	}

	@Override
	public void initialize(InputSplit inputsplit,TaskAttemptContext taskattemptcontext) throws IOException,InterruptedException {
		
		String delimiter = taskattemptcontext.getConfiguration().get("file.delimiter", "\r\n");
		FileSplit split = (FileSplit) inputsplit;
		Configuration conf = taskattemptcontext.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream is = fs.open(path);
		InputStreamReader isr = new InputStreamReader(is);
		br = new BufferedReader(isr);
		
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while ((line=br.readLine())!=null){
			value.set(line);			
		return true;
	}
		return false;
	}

}
