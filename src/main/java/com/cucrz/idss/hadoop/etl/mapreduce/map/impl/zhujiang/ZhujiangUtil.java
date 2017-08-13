package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.zhujiang;

import java.util.Map;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.PlayStatus;

public class ZhujiangUtil {
	
	public static String supplementTime(String time){
		if(time.length()<6){
			for(int i=0;time.length()<6;time="0"+time){				
			}
		}		
		return time;
	}
	
	public static String playStatusTran(String status) {
		status = status.toUpperCase();
		if (status.equals("PLAY")) {
			return PlayStatus.STATUS_PLAY;
		} else if (status.equals("PAUSE")) {
			return PlayStatus.STATUS_PAUSE;
		} else if (status.equals("REPLAY")) {
			return PlayStatus.STATUS_REPLAY;
		} else if (status.equals("REWIND")) {
			return PlayStatus.STATUS_REWIND;
		} else if (status.equals("SKIP")) {
			return PlayStatus.STATUS_SKIP;
		} else if (status.equals("STOP")) {
			return PlayStatus.STATUS_STOP;
		} else if (status.equals("FASTFORWARD")) {
			return PlayStatus.STATUS_FASTFORWARD;
		}  else if (status.equals("LIVE")) {
			return PlayStatus.STATUS_PLAY;
		} else {
			return status;
		}
	}
	public static String[] getNewVODInfo(Map<String, String> newCloumn,Map<String, String> VODInfoList,
			String programInfo,String cloumnInfo) {
		String vod=programInfo+", ,0, ";
		if (VODInfoList != null) {
			if (VODInfoList.containsKey(programInfo)) {			
				vod=programInfo+","+VODInfoList.get(programInfo);
			}else{
				newCloumn.put(programInfo,null);
			}
		}
		String[] vodInfo=(vod).split(OtherConstants.COMMA_DELIM,10);
		return vodInfo;
	}
	
	public static String[] getLookBackInfo(Map<String,String> lookbackList,String programId){
		String[] lookbackInfo=null;
		if(programId!=null&&lookbackList!=null){
			if(lookbackList.containsKey(programId)){
				lookbackInfo=lookbackList.get(programId).split(OtherConstants.EXCLAMATION_DELIM_REGEX,10);
			}
		}
		return lookbackInfo;
	}
}
