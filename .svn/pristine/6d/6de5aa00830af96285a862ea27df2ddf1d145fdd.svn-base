package com.cucrz.idss.hadoop.etl.mapreduce.job.baseETL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.map.ETLMapperFactory;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.FormCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class DataETLMapper extends Mapper<Object, Text, Text, Text> {
	private static Logger log = Logger.getLogger(DataETLMapper.class);

	private Map<String, Map<String, String>> storage = new HashMap<String, Map<String, String>>();
	private static Map<String, Counter> counterMap = new HashMap<String, Counter>();
	private static Map<String, String> newChannel =  new HashMap<String, String>();
	private static Map<String, String> newCloumn = new HashMap<String, String>();
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
		String etlActionName = conf.get("operatorName");
		operId = conf.get("operatorID");
		dateId = conf.get("inputDateID");
		FileSplit inFilename = getFilePath(context);
		splitPath = inFilename.getPath().toString();
		mos = new MultipleOutputs<Text, Text>(context);
		try {
			etlMapper = ETLMapperFactory.getInstance(etlActionName, operId)
					.createETL(operId);
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
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Counter mapFmtError = counterMap.get("mapFmtError");
		Counter TotleRows = counterMap.get("TotleRows");
		Counter mapOutputRows = counterMap.get("mapOutputRows");
		String inputValue = value.toString().trim();
		TotleRows.increment(1);
		String laterDate=DateUtil.getLaterDate(dateId,DateUtil.DATEFORMATER);
		if (FormCheck.checkLine(inputValue)) {
			String ln = etlMapper.etlMapper(inputValue, splitPath, operId, dateId, context,storage, counterMap, newChannel, newCloumn);
			if(ln!=null){
			String[] lines = ln.split(OtherConstants.ENTER_DELIM);
			for (String line : lines) {
				if (FormCheck.formCheckRule(line)) {
					String[] headline = line.split(OtherConstants.TAB_DELIM);
					String[] strs = headline[1]
							.split(OtherConstants.VERTICAL_DELIM_REGEX);
					if (strs.length > 2) {
						String date="";
						try {
							date = DateUtil.DATEFORMATER.format(DateUtil.DATE_TIME_FORMATER.parse(strs[0]));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						if(date.equals(dateId)){
						k.set(headline[0]);// 设备ID
						v.set(headline[1]);// 剩余其他数据
						context.write(k, v);
						mapOutputRows.increment(1);
					}else if(date.equals(laterDate)){
						mos.write(OutputPath.LAST_EVENT, null, new Text(line),  OutputPath.LAST_EVENT_PREFIX+ OutputPath.LAST_EVENT);
					}
					}
				} else {
					k.set("WrongDataRow");
					v.set(new Text(inputValue));
					context.write(k, v);
					mapFmtError.increment(1);
				}
			}
		}
		} else {
			k.set("WrongDataRow");
			v.set(new Text(inputValue));
			context.write(k, v);
			mapFmtError.increment(1);
		}
	}

	@Override
	protected void cleanup(Context context) {
		// 输出新频道，新栏目等
		String isOutputNewChannel = context.getConfiguration().get(
				"idss_ETL_NewChannel");
		String isOutputNewCloumn = context.getConfiguration().get(
				"idss_ETL_NewCloumn");
		Set<String> channelSet = newChannel.keySet();
		Set<String> cloumnSet = newCloumn.keySet();
		if (isOutputNewChannel.equals("true")) {
			for (String s : channelSet) {
				try {
					context.write(new Text("newChannel"), new Text(s));
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		if (isOutputNewCloumn.equals("true")) {
			for (String s : cloumnSet) {
				try {
					context.write(new Text("newCloumn"), new Text(s));
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
//		 写前一天事件到reduce
		Configuration conf = context.getConfiguration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			getLastEvent(context,fs);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void getLastEvent(Context context,FileSystem fs) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Counter mapOutputRows = counterMap.get("mapOutputRows");
		Counter mapFmtError = counterMap.get("mapFmtError");
		String lastEvent = conf.get("lastEventPath");
		log.info("前一天文件路径---------------" + lastEvent);
		Path path = new Path(lastEvent);
		try {if(fs.exists(path)){
			
				FileStatus[] files = fs.listStatus(path);
				for (FileStatus file:files) {
					if (!file.isDir()) {
						InputStream is = fs.open(file.getPath());
						InputStreamReader isr = new InputStreamReader(is);
						BufferedReader br = new BufferedReader(isr);
						String line = br.readLine();
						while (line != null) {
							if (FormCheck.checkLine(line)) {
								String[] lines = line
										.split(OtherConstants.ENTER_DELIM);
								for (String line1 : lines) {
									if (FormCheck.formCheckRule(line1)) {
										String[] headline = line1
												.split(OtherConstants.TAB_DELIM);
										String[] strs = headline[1]
												.split(OtherConstants.VERTICAL_DELIM_REGEX);
										if (strs.length > 2) {
											k.set(headline[0]);// 设备ID
											v.set(headline[1]);// 剩余其他数据
											context.write(k, v);
											mapOutputRows.increment(1);
										}
									} else {
										k.set("WrongDataRow");
										v.set(new Text(line));
										context.write(k, v);
										mapFmtError.increment(1);
									}
								}
							} else {
								k.set("WrongDataRow");
								v.set(new Text(line));
								context.write(k, v);
								mapFmtError.increment(1);
							}
							line = br.readLine();
						}
						is.close();
					}
				}
		}
		} catch (IOException e) {
			log.warn("前一天事件文件路径错误！");
			e.printStackTrace();
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
