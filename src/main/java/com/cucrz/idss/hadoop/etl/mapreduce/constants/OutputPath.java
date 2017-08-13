package com.cucrz.idss.hadoop.etl.mapreduce.constants;

public class OutputPath {

	// hive输出路径参数
	public static final String HIVE_PREFIX = "hive/";
	public static final String HIVE_LIVE = "evt_live";
	public static final String HIVE_LOOKBACKPAGE = "evt_lookbackpage";
	public static final String HIVE_LOOKBACK = "evt_lookbackprogram";
	public static final String HIVE_VODPAGE = "evt_vodpage";
	public static final String HIVE_VODPROGRAMME = "evt_video";
	public static final String HIVE_TIMESHIFT = "evt_timeshift";
	public static final String HIVE_TVAPP = "evt_teleapp";
	public static final String HIVE_ADS = "evt_ads";
	public static final String HIVE_OTHER = "evt_tv";
	public static final String HIVE_OC = "evt_oc";
	public static final String HIVE_UBUSE = "evt_unuse";

	// event1输出路径参数
	public static final String EVENT1_PREFIX = "event1/";
	public static final String EVENT1 = "event1_";
	// event2输出路径参数
	public static final String EVENT2_PREFIX = "event2/";
	public static final String EVENT2 = "event2_";
	//原始记录输出路径参数
	public static final String SOURCE_PREFIX = "source/";
	public static final String SOURCE = "source_";
	//错误记录输出路径参数
	public static final String WRONG_PREFIX = "wrong/";
	public static final String WRONG = "wrong";
	//新频道记录输出路径参数
	public static final String NEW_CHANNLE_PREFIX = "newChannel/";
	public static final String NEW_CHANNLE = "newChannel";
	//新栏目信息记录输出路径参数
	public static final String NEWCLOUMN_PREFIX = "newCloumn/";
	public static final String NEWCLOUMN = "newCloumn";
	//新用户记录输出路径参数
	public static final String NEW_USER_PREFIX = "newUser/";
	public static final String NEW_USER = "newUser";
	//完整用户
	public static final String WHOLE = "whole";
	//样本户
	public static final String SAMPLE = "sample";
	//直播节目
	public static final String PROGRAM_LIVE = "liveprogram";
	public static final String PROGRAM_LIVE_PREFIX= "liveprogram/";
	//错误记录输出路径参数
	public static final String PROGRAM_WRONG_PREFIX = "programwrong/";
	public static final String PROGRAM_WRONG = "programwrong";
	public static final String LAST_EVENT_PREFIX = "lastEvent/";
	public static final String LAST_EVENT= "lastEvent";
}