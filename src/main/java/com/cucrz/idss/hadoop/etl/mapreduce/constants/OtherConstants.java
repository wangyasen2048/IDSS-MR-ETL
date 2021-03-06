package com.cucrz.idss.hadoop.etl.mapreduce.constants;

import java.io.File;
import java.util.regex.Pattern;

public class OtherConstants {

	// 文件分隔符
	public static final String FILE_SEPARATOR = "/";
	public static final String COMMA_DELIM = ",";
	public static final String COLON_DELIM = ":";
	public static final String TAB_DELIM = "\t";
	public static final String ENTER_DELIM = "\n";
	public static final String VERTICAL_DELIM = "|";
	public static final String CARET_DELIM="^";
	public static final String CARET_DELIM_REGEX = Pattern
			.quote(CARET_DELIM);
	public static final String VERTICAL_DELIM_REGEX = Pattern
			.quote(VERTICAL_DELIM);
	public static final String EXCLAMATION_DELIM = "!";
	public static final String EXCLAMATION_DELIM_REGEX = Pattern
			.quote(EXCLAMATION_DELIM);
	public static final String MR_CONF = "mapred.conf.xml";
	public static final String USER_JOB_CONF = "userJob.conf.xml";
	
	public static final String ESCAPE = "&";
}
