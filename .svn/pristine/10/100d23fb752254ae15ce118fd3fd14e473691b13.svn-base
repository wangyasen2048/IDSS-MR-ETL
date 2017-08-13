package com.cucrz.idss.hadoop.etl.mapreduce.rules;

import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;

public class FormCheck {
	private static Logger log = Logger.getLogger(FormCheck.class);
	
	//验证数据格式及数据头是否合法
	public static boolean formCheckRule(String line){
		if(!line.equals("")){
			int index =  line.indexOf(OtherConstants.VERTICAL_DELIM);
			if (index!=-1){
				if(line.indexOf(OtherConstants.TAB_DELIM)!=-1){
					String[] split = line.split(OtherConstants.TAB_DELIM);
					String[] user1 = split[0].split(OtherConstants.VERTICAL_DELIM_REGEX);
					String[] user2 = split[1].split(OtherConstants.VERTICAL_DELIM_REGEX);
					if(user2.length>1&&user1.length==2){
						if (checkLine(user1[0]) &&checkLine(user1[1])&&checkLine(user2[0])&&checkLine(user2[1])){							
						return true;
						}else {
						return false;
						}
					}else{
						log.warn("user information isn't right!----"+line);
					}
				}
			}
		}		
		return false;
	}

	
	/**
	 * 验证数据合法性
	 * 
	 * @return
	 */
	public static boolean checkLine(String record) {
		if (null == record || record.trim().isEmpty()||record.equals("")) {
			return false;
		}
		return true;
	}
	
}
