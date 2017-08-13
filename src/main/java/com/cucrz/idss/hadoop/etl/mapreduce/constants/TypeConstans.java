package com.cucrz.idss.hadoop.etl.mapreduce.constants;

public class TypeConstans {

	// 系统类型
	public static final String OFF_CODE = "2";// 关机
	public static final String LAST_OFF_CODE = "999999";// 关机
	public static final String ON_CODE = "1";// 开机
	public static final String HEART_BEAT = "3";
	// 业务类型

	// 直播类
	public static final String EMPTY_CHANNEL = "256";//空频道
	public static final String EVENT_LIVE__DATA_BIZ = "256";// 空频道
	public static final String EVENT_LIVE_BIZ = "257";// 直播
	public static final String EVENT_LIVE__DATAVOICE_BIZ = "258";// 数字音频直播
	public static final String EVENT_LIVE_DEFAULTTV = "511";// 未知频道
	public static final String EVENT_UNUSE="000";//其他类型
	public static final String EVENT_LIVE_PROGRAM="259";//直播节目
	// 状态类
	public static final String STATUS_AUDIENCE_INFORMATION = "513";
	public static final String STATUS_COMMENT_INFORMATION = "514";
	public static final String STATUS_VOICE_BAR = "528";
	public static final String STATUS_CHANNEL_INFORMATION_BAR = "529";
	public static final String STATUS_HOT_RECOMMEND_LIST = "530";
	public static final String STATUS_PERSONAL_RECOMMEND_LIST = "531";
	public static final String STATUS_HOBBY_LIST = "532";
	public static final String STATUS_CHANNEL_LIST = "533";
	public static final String STATUS_QUESTIONNAIRE_PAGE = "534";
	public static final String STATUS_PROGRAM_GUIDE = "535";
	public static final String STATUS_MUTIPLE_VIEWS = "536";
	public static final String STATUS_SCHEDULING_PROGRAM = "537";
	public static final String STATUS_MAINMENU = "538";
	public static final String STATUS_DWAYMENU = "539";
	public static final String STATUS_EXCEPTION = "544";
	public static final String STATUS_LIKE_CHANNEL = "545";
	public static final String STATUS_RESERVE_PROGRAM = "546";
	
	// 双向类
	public static final String EVENT_VOD_TELEAPP = "769";// 电视应用
	public static final String EVENT_TIMESHIFT_BIZ = "770";// 时移
	public static final String EVENT_VOD_PAGE_BIZ = "771";// VOD页面
	public static final String EVENT_VOD_PROGRAM_BIZ = "772";// VOD节目
	public static final String EVENT_LOOKBACK_PAGE_BIZ = "773";// 回看页面
	public static final String EVENT_LOOKBACK_PRO_BIZ = "774"; // 回看节目
	public static final String EVENT_URL = "775";
	public static final String EVENT_UNKNOWN = "";
	// 通讯类
	public static final String CALL_DEFAULT = "";
	public static final String VIDEO_CALL = "";
	public static final String MAIL_MESSAGE = "1027";
	// 菜单类
	public static final String MENU_DEFAULT = "";
	public static final String CHANNEL_LIST = "1284";
	public static final String CHANNEL_STORE  = "1283";
	public static final String GUIDE_EPG  = "1286";
	public static final String MOSAIC  = "1287";
	
	// 广告类
	public static final String EVENT_AFFECT_ADS = "1538";// 影响业务的广告
	public static final String EVENT_NOT_AFFECT_ADS = "1537"; // 不影响业务的广告
	// 终端监控类
	public static final String DEVIECE_INFORMATION = "";
	
	//其他业务
	public static final String OTHER_UNDEFINED = "65535";
	
	

	
	//默认区域编码
	//珠江
	public static final String default_Zhujiang = "190200";
	//歌华
	public static final String default_Gehua = "110000";
	//广东
	public static final String default_Guangdong = "445400";
	//陕西
	public static final String default_Shanxi = "610000";
	//东方有线
	public static final String default_Dongfang  = "090100";
	//成都
	public static final String default_Chengdu  = "512000";
	//河北
	public static final String default_Hebei  = "130100";
	//天津
	public static final String default_Tianjin = "120000";
	//宁波
	public static final String default_Ningbo = "330200";
	//四川
	public static final String default_Sichuan = "510000";
	//河南
	public static final String default_Henan = "410000";
	//长沙国安
	public static final String default_Changsha = "430100";
	//湖南电信
	public static final String default_Hunandianxin = "430000";
	

}
