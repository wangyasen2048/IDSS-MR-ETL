
package com.cucrz.idss.hadoop.etl.start;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.job.baseETL.DataEtlJob;
import com.cucrz.idss.hadoop.etl.mapreduce.job.program.ProgramJob;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadProperty;


public class DataETLMain {
	private static Logger log = Logger.getLogger(DataETLMain.class);
	private static Map<String, String> map = new HashMap<String, String>();
	public static void main(String[] args) {// operId operName dateId	
		args=new String[]{"-o","1302","-n","fujiandianxin","-d","20151003","-p","cucrz/data/cache/"};
		mapRed(args);

	}

	public static void mapRed(String[] args){
		long startTime=System.currentTimeMillis();
		
		
		//判断输出信息，是否有误，
		if (args.length < 6) {
			System.out.println("Usage:  -o opertorId -n operatorName -d dateId -p propertyPath ");
		}
		String operId = "";
		String dateId = "";
		String actionName = "";
		String iscache = "true";
		String proPath = "";
		int len = args.length;
		
		if(len>=6){
		for (int k = 0; k < len; k++) {
			if (args[k].equals("-o")) {
				operId = args[k + 1];	
			} else if (args[k].equals("-d")) {
				dateId = args[k + 1];
			} else if (args[k].equals("-n")) {
				actionName = args[k + 1];
			}else if (args[k].equals("-p")){
				proPath=args[k + 1];
			}
		}
		boolean is=ReadProperty.checkOperatorINFO(operId, actionName);
		if(is!=true){
			System.exit(1);
		}
		if (dateId.length() == 8) {
				try {
					DateUtil.DATEFORMATER.parse(dateId);
				} catch (java.text.ParseException e) {
					System.out.println("----------输入日期参数不正确！-------------");
					return;
				}	
		} else {
			System.out.println("----------------输入日期参数不正确！-------------");
			return;
		}
		}else{
			log.error("输入参数有误！\n" + "Usage:  -o opertorId -n operatorName -d dateId -p propertyPath");
			return;
		}
		
		if (len % 2 != 0) {
			log.error("输入参数有误！\n" + "Usage:  -o opertorId -n operatorName -d dateId -p propertyPath");
			System.out.println("Usage:  -o opertorId -n operatorName -d dateId -p propertyPath");
			return;
		}
		
		//指定特定频道的map类名
		String etlActionName = "com.cucrz.idss.hadoop.etl.mapreduce.map.impl." + actionName + ".ETLMapper";
		//指定特定频道的路径
		String operatorPath="com.cucrz.idss.hadoop.etl.mapreduce.in.path."+actionName+"."+actionName;
		//特定频道的路径
		map.put("pathName", operatorPath);
		//判断是否是缓存
		map.put("dcOpen", iscache);	
		//频道id
		map.put("operatorID", operId);
		//特定频道的路径
		map.put("operatorName", etlActionName);
		//输出日期id
		map.put("inputDateID", dateId);
		//数据路径
		map.put("propertyPath", proPath);
		
		boolean etl = false;
		boolean program=false;
			Set<String> set =map.keySet();		
			Iterator<String> iter =set.iterator();
			while (iter.hasNext()) {
				String key = iter.next();
//				System.out.println(key + "===" + map.get(key));
				log.info(key + "===" + map.get(key));
			}
			
			
			
			//核心代码部分
			try {
				
				etl = DataEtlJob.run(map);
				String isProgram=map.get("idss_ETL_Form_Program");
				if(etl&&isProgram!=null&&isProgram.equals("true")){
				program=ProgramJob.run(map);
				}
			} catch (Exception e) {
				log.error(e);
				System.exit(1);
			}
			
			
			//获取任务持续时间
			long endTime = System.currentTimeMillis();
			String time = DateUtil.getDuration(startTime, endTime);
			if(etl&&program){	
				System.out.println("==========》 用时== " +time+ "《==========");
			}else{
				System.out.println("==========》 用时== " +time+ "《==========");
			}
		}

}
