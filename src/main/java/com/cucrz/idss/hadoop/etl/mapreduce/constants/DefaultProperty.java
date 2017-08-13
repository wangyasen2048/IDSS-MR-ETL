package com.cucrz.idss.hadoop.etl.mapreduce.constants;

public class DefaultProperty {
	//时间参数  单位为秒
	public static final String POWER_ONOFF_INTERVAL_TIME="18000";//开关机超时时间
	public static final String POWER_ON_INTERVAL_TIME="15";//开机超时时间
	public static final String POWER_ON_SUB_TIME="15";//开机前置时间
	public static final String POWER_OFF_POSTPOSTIVE_TIME="9000";
	public static final String NOCHANNEL_INTERVAL_TIME="5";//空频道间隔时间
	public static final String HEART_BEAT_INTERVAL_TIME="5";//心跳间隔时间
	public static final String MERGE_INTERVAL_TIME="900";//合并间隔时间

	//规则参数
	public static final String idss_ETL_Rule_Merge_Same_Event="1|true|com.cucrz.idss.hadoop.etl.mapreduce.rules.MergeSameEvent";
	public static final String idss_ETL_Rule_OC="2|true|com.cucrz.idss.hadoop.etl.mapreduce.rules.AddOCEvent";
	public static final String idss_ETL_Rule_LoseHeartBeat="3|true|com.cucrz.idss.hadoop.etl.mapreduce.rules.LoseHeartBeat";
	public static final String idss_ETL_Rule_NoChannel="4|false|com.cucrz.idss.hadoop.etl.mapreduce.rules.NoChannel";
	public static final String idss_ETL_Rule_NewBaseData="9|false|com.cucrz.idss.hadoop.etl.mapreduce.rules.NewBaseData";
	public static final String idss_ETL_Rule_SampleUser="false";
	
	//输出格式参数
	public static final String idss_ETL_Form_Hive="1|true|com.cucrz.idss.hadoop.etl.mapreduce.out.form.hiveEvent";
	public static final String idss_ETL_Form_Event1="2|false|com.cucrz.idss.hadoop.etl.mapreduce.out.form.event1";
	public static final String idss_ETL_Form_Event2="3|false|com.cucrz.idss.hadoop.etl.mapreduce.out.form.event2";
	public static final String idss_ETL_Form_Source="4|false|com.cucrz.idss.hadoop.etl.mapreduce.out.form.source";
	
	
}
