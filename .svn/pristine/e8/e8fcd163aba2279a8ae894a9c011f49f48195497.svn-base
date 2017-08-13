package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.tianjin;

import java.util.Map;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
/**
 * 
 * @author qianzhiqin
 * @creatTime  2015年5月5日 
 */
public class EVBean {
	private String E;
	private String E1;
	private String E2;
	private String E3;
	private String E4;
	private String E5;
	private String D1;
	private String D2;
	private String D3;
	private String T;
	
	public EVBean() {
		super();
	}
	
	public EVBean(String e, String e1, String e2, String e3, String e4,
			String e5, String d1, String d2, String d3, String t) {
		super();
		E = e;
		E1 = e1;
		E2 = e2;
		E3 = e3;
		E4 = e4;
		E5 = e5;
		D1 = d1;
		D2 = d2;
		D3 = d3;
		T = t;
	}
	
	public String getE() {
		return E;
	}
	public void setE(String e) {
		E = e;
	}
	public String getE1() {
		return E1;
	}
	public void setE1(String e1) {
		E1 = e1;
	}
	public String getE2() {
		return E2;
	}
	public void setE2(String e2) {
		E2 = e2;
	}
	public String getE3() {
		return E3;
	}
	public void setE3(String e3) {
		E3 = e3;
	}
	public String getE4() {
		return E4;
	}
	public void setE4(String e4) {
		E4 = e4;
	}
	public String getE5() {
		return E5;
	}
	public void setE5(String e5) {
		E5 = e5;
	}
	public String getD1() {
		return D1;
	}
	public void setD1(String d1) {
		D1 = d1;
	}
	public String getD2() {
		return D2;
	}
	public void setD2(String d2) {
		D2 = d2;
	}
	public String getD3() {
		return D3;
	}
	public void setD3(String d3) {
		D3 = d3;
	}
	public String getT() {
		return T;
	}
	public void setT(String t) {
		T = t;
	}
	
	/**
	 * 根据ev的T，判断事件类型
	 * @param date yyyyMMdd
	 * @return
	 */
	public String getActionType(String dateStr,Map<String, String> channelName){
		StringBuffer actionType = new StringBuffer();
		
		//直播 状态
		if("L".equals(T)){
			//开机启动    >>  开机
			if("0000".equals(D1)){
				actionType.append(TypeConstans.ON_CODE + OtherConstants.VERTICAL_DELIM + "1");
			//进入portal	>>  设置为心跳
			}else if("0001".equals(D1)){
				actionType.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
			//显示音量 >>	
			}else if("0002".equals(D1)){
				actionType.append(TypeConstans.STATUS_VOICE_BAR);
			//进入直播  >> 数字音频直播 
			}else if("0101".equals(D1)){
//				Network_ID
//				TS_ID
//				Service_ID
//				直播频道换台方式
//				直播频道播放状态
//				逻辑频道号
//				频道名称
//				当前统一频道编号
				String channelQG = channelName.get(E3)==null?"":channelName.get(E3);
				actionType.append(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ + OtherConstants.VERTICAL_DELIM);
				actionType.append(E1 + OtherConstants.VERTICAL_DELIM + E2 +OtherConstants.VERTICAL_DELIM + E3 + OtherConstants.VERTICAL_DELIM);
				actionType.append(OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM +E3+OtherConstants.VERTICAL_DELIM + D3 +OtherConstants.VERTICAL_DELIM + channelQG );
			//列表  >> 频道列表
			}else if(Integer.parseInt(D1)>=201 && Integer.parseInt(D1)<=222){
				if(Integer.parseInt(D1)%2 != 0){
					actionType.append(TypeConstans.STATUS_CHANNEL_LIST );
				}else {
					//退出列表 >> 心跳
					actionType.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
				}
			//节目预告 >>  电子节目指南EPG
			}else if(Integer.parseInt(D1)>=301 && Integer.parseInt(D1)<=304){
				if(Integer.parseInt(D1)%2 != 0){
					actionType.append(TypeConstans.STATUS_PROGRAM_GUIDE );
				}else {
					//退出预告 >> 心跳
					actionType.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
				}
			//显示pf条,退出直播状态 >> 心跳
			}else {
				actionType.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
			}
			
		//点播、时移、回放
		}else if("V".equals(T)){
			//VOD点播  VOD节目播放
			if("0003".equals(D1)){
				String contentId = "";
				try {
					String spilt = E1.split("/")[3];
					contentId = spilt.substring(0,spilt.indexOf("^"));
				} catch (Exception e) {
					return null;
				}
				actionType.append(TypeConstans.EVENT_VOD_PROGRAM_BIZ + OtherConstants.VERTICAL_DELIM);
				//影片ID， 影片名称，
				actionType.append(contentId + OtherConstants.VERTICAL_DELIM + D2 + OtherConstants.VERTICAL_DELIM);
				//影片分集数，当前节目播放时间，节目播放状态 ，
				actionType.append("0"+OtherConstants.VERTICAL_DELIM +"0" + OtherConstants.VERTICAL_DELIM +"1"+ OtherConstants.VERTICAL_DELIM);
				//栏目ID ，栏目名称
				actionType.append(E2 + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM);
				actionType.append(OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM);
				
			//互动时移	
			}else if("0001".equals(D1)){
//				时移节目Network_ID
//				时移节目TS_ID
//				时移节目Service_ID
//				原始播出时间
//				播放状态
//				时移频道名称
//				统一频道编号
				String channelQG = channelName.get(E3)==null?"":channelName.get(E3);
				actionType.append(TypeConstans.EVENT_TIMESHIFT_BIZ + OtherConstants.VERTICAL_DELIM);
				actionType.append(E1 + OtherConstants.VERTICAL_DELIM +E2 + OtherConstants.VERTICAL_DELIM + E3 + OtherConstants.VERTICAL_DELIM);
				actionType.append(OtherConstants.VERTICAL_DELIM + "1" +OtherConstants.VERTICAL_DELIM + D3 + OtherConstants.VERTICAL_DELIM + channelQG);
			// 电视回放
			}else if("0002".equals(D1)){
//				String spilt = E1.split("/")[3];
//				String channelId = spilt.substring(0,spilt.indexOf("^"));
				String startTime = "";
				try {
					startTime = E1.substring(E1.indexOf("startTime=")+10,E1.indexOf("endTime=")-5);
				} catch (Exception e) {
					return null;
				}
				String date = "";
				String time = "";
				if(startTime.length()==14){
					date = startTime.substring(0,8);
					time = startTime.substring(8,14);
				}else{
					return null;
				}
				actionType.append(TypeConstans.EVENT_LOOKBACK_PRO_BIZ + OtherConstants.VERTICAL_DELIM);
				//节目ID,回看节目名称,回看频道名称
				actionType.append(OtherConstants.VERTICAL_DELIM + D2 + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM);
				//节目播出日期,节目播出时间,
				actionType.append(date + OtherConstants.VERTICAL_DELIM +time + OtherConstants.VERTICAL_DELIM  );
				//当前节目播放时间,节目播放状态
				actionType.append(E + OtherConstants.VERTICAL_DELIM + "1");
			}
			
		//VOD
		}else if("P".equals(T)){
			if("0000".equals(D1)){
//				0x0303	VOD业务页面
//				页面ID
//				栏目ID
//				栏目名称
//				栏目页面当前页码
//				当前栏目页面的影片个数"
				actionType.append(TypeConstans.EVENT_VOD_PAGE_BIZ + OtherConstants.VERTICAL_DELIM);
				actionType.append(OtherConstants.VERTICAL_DELIM +E3 + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM);
				actionType.append(D3 + OtherConstants.VERTICAL_DELIM + E4);
			}else if("0001".equals(D1)){
//				0x0305	回看业务页面

//				页面ID  E1
//				节目播出日期
//				回看频道名称  E3
				actionType.append(TypeConstans.EVENT_LOOKBACK_PAGE_BIZ + OtherConstants.VERTICAL_DELIM);
				actionType.append(E1 + OtherConstants.VERTICAL_DELIM + dateStr + OtherConstants.VERTICAL_DELIM + E3 + OtherConstants.VERTICAL_DELIM);
				
			}
			
		//广告 业务
		}else if("A".equals(T)){
//			广告位置ID
//			显示秒时长
//			广告表现形式
//			广告ID
//			广告名称
			actionType.append(TypeConstans.EVENT_AFFECT_ADS + OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM);
			actionType.append(D3 + OtherConstants.VERTICAL_DELIM + E4 + OtherConstants.VERTICAL_DELIM + E5 + OtherConstants.VERTICAL_DELIM );
		
		//异常
		}else if("E".equals(T)){
			actionType.append(TypeConstans.STATUS_EXCEPTION + OtherConstants.VERTICAL_DELIM );
			actionType.append(OtherConstants.VERTICAL_DELIM + OtherConstants.VERTICAL_DELIM+ OtherConstants.VERTICAL_DELIM);
		
		// 心跳
		}else if("H".equals(T)){
			actionType.append(TypeConstans.HEART_BEAT + OtherConstants.VERTICAL_DELIM +"1");
		}
		
		return actionType.toString();
	}
}
