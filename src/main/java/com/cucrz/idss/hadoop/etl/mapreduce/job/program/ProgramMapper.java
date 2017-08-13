package com.cucrz.idss.hadoop.etl.mapreduce.job.program;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.mortbay.io.Buffer;
import org.mortbay.jetty.HttpParser.Input;
import org.omg.CORBA.portable.InputStream;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.job.baseETL.DataETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.map.ETLMapperFactory;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.FormCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class ProgramMapper extends Mapper<Object, Text, Text, Text> {
	private static Logger log = Logger.getLogger(DataETLMapper.class);
	private Map<String, Map<String, String>> storage = new HashMap<String, Map<String, String>>();
	private static Map<String, Counter> counterMap = new HashMap<String, Counter>();
	private static MultipleOutputs<Text, Text> mos;
	private String splitPath = "";
	private IETLMapper etlMapper;
	private String operId;
	private String dateId;
	static Text k = new Text();
	static Text v = new Text();
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String etlActionName = "com.cucrz.idss.hadoop.etl.mapreduce.map.program.LiveProgram";
		operId = conf.get("operatorID");
		dateId = conf.get("inputDateID");
		FileSplit inFilename = getFilePath(context);
		splitPath = inFilename.getPath().toString();
		mos = new MultipleOutputs<Text, Text>(context);
		try {
			etlMapper = ETLMapperFactory.getInstance(etlActionName, operId).createETL(operId);
		} catch (Exception e) {
			log.warn("实例化解析类错误！");
			e.printStackTrace();
			return;
		}
		counterMap = etlMapper.etlMapSetupGetCounter(context);
		boolean isOpen = Boolean.parseBoolean(conf.get("dcOpen"));
		if (isOpen) {
			log.info("开始加载map分布式缓存！");
			
			storage = etlMapper.etlMapSetupDisCache(context, storage);
			log.info("map分布式缓存加载完成！");
		}
	}
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Counter mapFmtError = counterMap.get("mapFmtError");
		Counter TotleRows = counterMap.get("TotleRows");
		String inputValue = value.toString().trim();
		TotleRows.increment(1);
		
		boolean flag=true;
		if (FormCheck.checkLine(inputValue)) {
			 String ln=etlMapper.etlMapper(inputValue, splitPath, operId, dateId, context, storage, counterMap, null,null);
				if(ln!=null){
					String[] lines = ln.split(OtherConstants.ENTER_DELIM);
					for(String s : lines){
//			 mos.write(OutputPath.PROGRAM_LIVE, null, new Text(s), OutputPath.PROGRAM_LIVE_PREFIX + OutputPath.PROGRAM_LIVE);
						context.write(new Text("program"), new Text(s));
						flag=false;
					}
				}
		}
		if(flag){
//			mos.write(OutputPath.PROGRAM_LIVE, null, new Text(inputValue),OutputPath.PROGRAM_WRONG_PREFIX + OutputPath.PROGRAM_WRONG);
		mapFmtError.increment(1);
		}
	}
	
	
	private FileSplit getFilePath(Context context) throws IOException {
		// FileSplit fileSplit = (FileSplit) context.getInputSplit();
		InputSplit split = context.getInputSplit();
		Class<? extends InputSplit> splitClass = split.getClass();
		FileSplit fileSplit = null;
		if (splitClass.equals(FileSplit.class)) {
			fileSplit = (FileSplit) split;
		} else if (splitClass.getName().equals(
				"org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
			// begin reflection hackery...
			try {
				Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
				getInputSplitMethod.setAccessible(true);
				fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
			} catch (Exception e) {
				// wrap and re-throw error
				throw new IOException(e);
			}
			// end reflection hackery
		}
		return fileSplit;
	}
}
