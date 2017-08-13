package com.cucrz.idss.hadoop.etl.mapreduce.out.form;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.out.IETLOutput;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.DateCheck;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class event2 implements IETLOutput {
	private static Logger log=Logger.getLogger(event2.class);

	@Override
	public void outputRecords(Set<String> records, String userID,
			MultipleOutputs<Text, Text> mos, Context context) {
			Iterator<String> it = records.iterator();
			String inputDateID = context.getConfiguration().get("inputDateID");
			while(it.hasNext()){
				String line =  it.next();
				String[] uSplit=userID.split(OtherConstants.VERTICAL_DELIM_REGEX);
				String[] split=line.split(OtherConstants.VERTICAL_DELIM_REGEX,20);
				if(split.length>2&&uSplit.length==2){
				String bizType=split[2];
				if(bizType.equals(TypeConstans.LAST_OFF_CODE)){
					bizType=TypeConstans.OFF_CODE;
				}
				if(isProgramEvent(bizType)){
				try {
					String regionID=uSplit[1];
					String operID= context.getConfiguration().get("operatorID");
					String time =DateUtil.TIME_FORMATER.format(DateUtil.DATE_TIME_FORMATER.parse(split[0]));
					String recordDate=DateUtil.DATE_FORMATER.format(DateUtil.DATE_TIME_FORMATER.parse(split[0]));
					
						String duration = DateUtil.getResultOfTimes(split[1], split[0], "-");
						String newrecord=operID+OtherConstants.VERTICAL_DELIM+uSplit[1]+
								OtherConstants.VERTICAL_DELIM+uSplit[0]+OtherConstants.VERTICAL_DELIM+
								split[0]+OtherConstants.VERTICAL_DELIM+split[1]+OtherConstants.VERTICAL_DELIM+
								duration+OtherConstants.VERTICAL_DELIM+bizType+OtherConstants.VERTICAL_DELIM;
						for(int i=3;i<split.length;i++){
							newrecord=newrecord+split[i]+OtherConstants.VERTICAL_DELIM;
						}
						if(newrecord.endsWith("|")){
						int last = newrecord.lastIndexOf(OtherConstants.VERTICAL_DELIM);
						newrecord=newrecord.substring(0, last);
						}
						try {
							mos.write(inputDateID, null, new Text(newrecord), inputDateID+OtherConstants.FILE_SEPARATOR+OutputPath.EVENT2_PREFIX+OutputPath.EVENT2+regionID);
						} catch (IOException e) {
							
							e.printStackTrace();
						} catch (InterruptedException e) {
							
							e.printStackTrace();
						}
					
					
				} catch (ParseException e) {
					log.warn("记录日期不正确，无法解析");
					e.printStackTrace();
				}				
			}		
			}
	}
	}
	
	
	//判断是否为收视事件类型
		public boolean isProgramEvent(String bizType){
			if(bizType.equals(TypeConstans.HEART_BEAT)){
				return false;
			}else{
				return true;
			}	
		}
}
