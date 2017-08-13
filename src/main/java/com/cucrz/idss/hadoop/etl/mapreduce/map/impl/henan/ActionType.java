package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.henan;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;
/**
 * 
 * @author qianzhiqin
 *	
 * @creatTime 2015年4月13日
 */
public class ActionType {
	
	/**
	 * 根据 actionType ，得到标准用户行为类型<br>
	 * 47项 到 73项  是从前端获取的，目前得不到,暂不处理
	 * @param plit
	 * @return
	 */
	protected static String getActionType(String[] split,
					Map<String, String> newChannel,
					Map<String, String> channelNameList,
					Map<String, String> channelCodeList,
					Map<String, String> VODInfoList,
					Map<String, String> newCloumn,Configuration conf ) {
		StringBuffer result = new StringBuffer();
		String actionType = "";
		if(split.length>=2){
			actionType = split[1];
		}
		//DVB(必选字段) >> 0x0101	数字电视
		if ("DVB".equals(actionType) && split.length >= 3){
			if(isInteger(split[2])){
				result.append(TypeConstans.EVENT_LIVE_BIZ + OtherConstants.VERTICAL_DELIM);
				result.append(OtherConstants.VERTICAL_DELIM);
				result.append(OtherConstants.VERTICAL_DELIM);
				result.append(split[2]+OtherConstants.VERTICAL_DELIM);
				result.append("0"+OtherConstants.VERTICAL_DELIM);
				result.append("0"+OtherConstants.VERTICAL_DELIM);
				result.append("0"+OtherConstants.VERTICAL_DELIM);
				result.append(OtherConstants.VERTICAL_DELIM);
				result.append(ReadCacheUtil.getUniqueChannel(newChannel, channelNameList, channelCodeList,"##"+ split[2], ""));
			}
		}
		//VOD >> 0x0304	VOD节目播放
		else if("VOD".equals(actionType) && split.length >= 5){
			result.append(TypeConstans.EVENT_VOD_PROGRAM_BIZ + OtherConstants.VERTICAL_DELIM);
			//影片ID
			result.append(split[2]+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append("0"+OtherConstants.VERTICAL_DELIM);
			//当前节目播放时间
			result.append("0"+OtherConstants.VERTICAL_DELIM);
			//节目播放状态
			result.append((split[4]=="0"?"1":"8") + OtherConstants.VERTICAL_DELIM);
			//栏目ID
			result.append(split[3] + OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
			ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList, split[2]+delim + "", split[3]+delim+"",delim,TypeConstans.default_Henan);
		}
		//3. MAINMENU(必选字段) >> 0x021A	主界面/主菜单
		else if("MAINMENU".equals(actionType)){
			result.append(TypeConstans.STATUS_MAINMENU + OtherConstants.VERTICAL_DELIM + "MAINMENU");
		}
		//4.DWAYMENU(必选字段) >> 0x021B	双向界面/双向菜单
		else if("DWAYMENU".equals(actionType)){
			result.append(TypeConstans.STATUS_DWAYMENU + OtherConstants.VERTICAL_DELIM + "DWAYMENU");
		}
		//5.STOCK >> 0x0301	图文应用
		else if("STOCK".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/STOCK" + OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		//6. MOSAIC >> 0x0212	热门推荐列表
		else if("MOSAIC".equals(actionType)){
			result.append(TypeConstans.STATUS_HOT_RECOMMEND_LIST + OtherConstants.VERTICAL_DELIM + "MOSAIC");
		}
		//7. EPG >> 0x0506	电子节目指南EPG 
		else if("EPG".equals(actionType)){
			result.append(TypeConstans.GUIDE_EPG+ OtherConstants.VERTICAL_DELIM + "EPG");
		}
		//8. NVOD
		else if("NVOD".equals(actionType)){
			
		}
		//9. BROADCAST(必选字段) 
		else if("BROADCAST".equals(actionType)){
			
		}
		//10. MAIL >> 0x0301	图文应用
		else if("MAIL".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/MAIL"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		//11. KTV >> 0x0301	图文应用
		else if("KTV".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/KTV"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		//12. UPDATE
		else if("UPDATE".equals(actionType)){

		}
		//13. GAME
		else if("GAME".equals(actionType)){
			
		}
		//14. EPOCKET >> 0x0301	图文应用
		else if("EPOCKET".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/EPOCKET"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		//15 BANK >> 0x0301	图文应用
		else if("BANK".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/BANK"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		//16. NETGAME >> 0x0301	图文应用
		else if("NETGAME".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/NETGAME"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		//32.POWEROFF >> 关机
		else if("POWEROFF".equals(actionType)){
			result.append(TypeConstans.LAST_OFF_CODE + OtherConstants.VERTICAL_DELIM + "1");
		}
		//33. VOLUME >> 0x0210	音量条
		else if("VOLUME".equals(actionType)){
			result.append(TypeConstans.STATUS_VOICE_BAR + OtherConstants.VERTICAL_DELIM + "VOLUME");
		}
		//34. LIST >> 0x0215	频道列表
		else if("LIST".equals(actionType)){
			result.append(TypeConstans.STATUS_CHANNEL_LIST + OtherConstants.VERTICAL_DELIM + "LIST");
		}
		//35. MINIEPG >> 0x0217	电子节目指南EPG
		else if("MINIEPG".equals(actionType)){
			result.append(TypeConstans.STATUS_PROGRAM_GUIDE + OtherConstants.VERTICAL_DELIM + "MINIEPG");
		}
		//36. GUIDE
		else if("GUIDE".equals(actionType)){
 
		}
		//37. ORDER >> 0x0219	节目预约
		else if("ORDER".equals(actionType)){
			result.append(TypeConstans.STATUS_SCHEDULING_PROGRAM + OtherConstants.VERTICAL_DELIM + "ORDER");
		}
		//38. ORDERINFO >> 0x0219	节目预约
		else if("ORDERINFO".equals(actionType)){
			result.append(TypeConstans.STATUS_SCHEDULING_PROGRAM + OtherConstants.VERTICAL_DELIM + "ORDERINFO");
		}
		//39. FAVORITEI >>  0x0214	喜好列表
		else if("FAVORITEI".equals(actionType)){
			result.append(TypeConstans.STATUS_HOBBY_LIST + OtherConstants.VERTICAL_DELIM + "FAVORITEI");
		}
		//40. FAVORITEB >>  0x0214	喜好列表
		else if("FAVORITEB".equals(actionType)){
			result.append(TypeConstans.STATUS_HOBBY_LIST + OtherConstants.VERTICAL_DELIM + "FAVORITEB");
		}
		//41. LB
		else if("LB".equals(actionType)){
		
		}
		//42. SS
		else if("SS".equals(actionType)){
		
		}
		//43. HELP
		else if("HELP".equals(actionType)){
		
		}
		//44. DVBAC
		else if("DVBAC".equals(actionType)){
		
		}
		//45. DVBMUTE
		else if("DVBMUTE".equals(actionType)){
		
		}
		//46. RADIO >> 0x0102	数字音频广播
		else if("RADIO".equals(actionType)){
			result.append(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ + OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append("0"+OtherConstants.VERTICAL_DELIM);
			result.append("0"+OtherConstants.VERTICAL_DELIM);
			result.append("0"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
			result.append(ReadCacheUtil.getUniqueChannel(newChannel, channelNameList, channelCodeList, "", ""));
		}
		//78. SHOWAD  >> 0x0301	图文应用
		else if("SHOWAD".equals(actionType)){
			result.append(TypeConstans.EVENT_VOD_TELEAPP + OtherConstants.VERTICAL_DELIM);
			result.append("/SHOWAD"+OtherConstants.VERTICAL_DELIM);
			result.append(OtherConstants.VERTICAL_DELIM);
		}
		else{
			System.out.println("actionType 没有匹配");
		}
		return result.toString();
	}

	/**
	 * 判断字符串是否是整数
	 */
	public static boolean isInteger(String value) {
		try {
			Integer.parseInt(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
}
