package com.cucrz.idss.hadoop.etl.mapreduce.job.program;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.exception.IdsshvException;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.in.ETLInputFormat;
import com.cucrz.idss.hadoop.etl.mapreduce.in.path.OperatorPath;
import com.cucrz.idss.hadoop.etl.mapreduce.job.baseETL.DataEtlJob;
import com.cucrz.idss.hadoop.etl.util.ReadProperty;

public class ProgramJob extends ControlledJob {
	private static Logger log = Logger.getLogger(DataEtlJob.class);
	private static final String propertyFileName = "idss_etl.property";
	private static  String JOB_PROPERTY = "/cucrz/data/cache/";
	public ProgramJob(Configuration conf) throws IOException {
		super(conf);
		try {
			super.setJob(createJob(conf));
		} catch (IdsshvException e) {
			log.warn("job创建失败！");
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public static Job createJob(Configuration conf) throws IOException, IdsshvException, URISyntaxException {
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "ProgramJob_" + conf.get("operatorID") + "_" + conf.get("inputDateID") + "_"
				+ DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMddHHmmssSSS"));
		boolean isOpen = Boolean.parseBoolean(conf.get("dcOpen"));
		String isOutputSampleUser = conf.get("idss_ETL_Rule_SampleUser");
		
			// 添加distributedcache
			FileStatus[] cache_files = fs.listStatus(
					new Path(conf.get("preCachePath") + conf.get("operatorID") + OtherConstants.FILE_SEPARATOR));
			for (int i = 0; i < cache_files.length; i++) {
				if (!cache_files[i].isDir()) {
					String path = cache_files[i].getPath().toString();
					int idx = path.lastIndexOf(OtherConstants.FILE_SEPARATOR);
					String symlink = path.substring(idx + 1);
					DistributedCache.addCacheFile(new URI(path + "#" + symlink), job.getConfiguration());				
				}
			}
//			Path programSchedule=new Path(conf.get("fs.defaultFS")+ OtherConstants.FILE_SEPARATOR
//					+ conf.get("dataSourcePath")+ OtherConstants.FILE_SEPARATOR + conf.get("operatorID")
//					+ OtherConstants.FILE_SEPARATOR + conf.get("inputDateID")+"/ProgramSchedule.txt");
//			DistributedCache.addCacheFile(new URI(programSchedule + "#" + conf.get("idss_ETL_Cache_ProgramSchedule")), job.getConfiguration());			
			DistributedCache.createSymlink(conf);

		job.setJarByClass(ProgramJob.class);
		job.setMapperClass(ProgramMapper.class);
		job.setReducerClass(ProgramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(ETLInputFormat.class);
//		输入路径---hive格式中的live数据
		 Path in = new Path(conf.get("fs.defaultFS") +File.separator + conf.get("outputPath") + File.separator + conf.get("operatorID")
			     +File.separator+ conf.get("inputDateID")+File.separator +OutputPath.WHOLE+"/hive/");
		 if(isOutputSampleUser.equals("true")){
			 in= new Path(conf.get("fs.defaultFS") +File.separator + conf.get("outputPath") + File.separator + conf.get("operatorID")
		     +File.separator+ conf.get("inputDateID")+File.separator +OutputPath.SAMPLE+"/hive/");
		 }
//		 输出路径
		 Path out =  new Path(conf.get("fs.defaultFS") +File.separator + 
				 conf.get("outputPath") + File.separator+ conf.get("operatorID")+File.separator + conf.get("inputDateID")+File.separator+"program");
			FileStatus[] files = fs.listStatus(in);
			for (int i = 0; i < files.length; i++) {
				if (files[i].isFile()&&files[i].getPath().toString().contains("evt_live")) {	
					MultipleInputs.addInputPath(job, files[i].getPath(), ETLInputFormat.class);
				}
			}	
//		 FileInputFormat.addInputPath(job,in);
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
		FileOutputFormat.setOutputPath(job, out);
		// 定义输出父路径
		MultipleOutputs.addNamedOutput(job, OutputPath.PROGRAM_LIVE, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, OutputPath.PROGRAM_WRONG, TextOutputFormat.class, Text.class, Text.class);
		return job;
	}

	public static boolean run(Map<String, String> mapArgs) throws Exception {
		Configuration conf = new Configuration();
		if(mapArgs.get("propertyPath")!=null&&!mapArgs.get("propertyPath").equals("")){
			JOB_PROPERTY=mapArgs.get("propertyPath");
		}
		String propertyPath = conf.get("fs.defaultFS") + JOB_PROPERTY + mapArgs.get("operatorID") + File.separator
				+ propertyFileName;mapArgs.putAll(ReadProperty.readProperty(propertyPath, conf));
		conf.set("dcOpen", mapArgs.get("dcOpen"));
		Set<String> set = mapArgs.keySet();
		for (String key : set) {
			conf.set(key, mapArgs.get(key));
		}
		conf.set("outputPath", mapArgs.get("prefixOutputPath"));
		conf.set("dataSourcePath", mapArgs.get("prefixDataPath"));
		conf.set("mapred.reduce.tasks", "5");
		Job job = null;
		String baseData = conf.get("idss_ETL_Rule_BaseData");
		try {
			job = createJob(conf);// 创建job
			if (baseData != null && baseData.equals("true")) {
			
			}
			job.submit();// 提交job
		} catch (Exception e) {
			e.printStackTrace();
		}
		boolean isFinished = job.waitForCompletion(true);
		// boolean isFinished =true;
		if (isFinished) {
			if (baseData != null && baseData.equals("true")) {
	
			}
			System.out.println("==========》 日期id为 " + mapArgs.get("inputDateID") + "----Program处理成功！《==========");
			return true;
		} else {
			System.out.println("==========》 日期id为 " + mapArgs.get("inputDateID") + "----Program处理失败！《==========");
			return false;
		}
	}
}
