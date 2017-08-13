package com.cucrz.idss.hadoop.etl.mapreduce.out.form;

import java.text.ParseException;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.out.IETLOutput;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class hiveEvent implements IETLOutput {

	private static Logger log = Logger.getLogger(hiveEvent.class);
	private static final long TIME_PART_1M = 60;

	@Override
	public void outputRecords(Set<String> records, String userID,
			MultipleOutputs<Text, Text> mos, Context context) {
		String dateID = context.getConfiguration().get("inputDateID");
		Iterator<String> it = records.iterator();
		while (it.hasNext()) {
			String record = it.next();
			try {
				hiveRecord(record, userID, dateID, mos);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	// 转换HIVE事件
	public void hiveRecord(String record, String userID, String dateID,
			MultipleOutputs<Text, Text> mos) throws Exception {
		String newrecord = "";
		String[] split = record.split(OtherConstants.VERTICAL_DELIM_REGEX,30);
		String[] h = userID.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String bizType = split[2];
		// String
		// recordDate=DateUtil.DATE_FORMATER.format(DateUtil.DATE_TIME_FORMATER.parse(split[1]));
		UUID uuid = UUID.randomUUID();
		String eventID = uuid.toString();
		// 事件ID+业务类型+区域ID+用户ID
		String head = eventID + OtherConstants.VERTICAL_DELIM + bizType
				+ OtherConstants.VERTICAL_DELIM + h[1]
				+ OtherConstants.VERTICAL_DELIM + h[0]
				+ OtherConstants.VERTICAL_DELIM;
		// 开始时间+结束时间+当前时间+1分钟区间
		String tail = OtherConstants.VERTICAL_DELIM + split[0]
				+ OtherConstants.VERTICAL_DELIM + split[1]
				+ OtherConstants.VERTICAL_DELIM + DateUtil.getCurrentTime()
				+ OtherConstants.VERTICAL_DELIM
				+ getTimePart1M(split[0], split[1]);

		int buisness = Integer.valueOf(bizType);
		switch (buisness) {
		case 1:
			newrecord = getOCEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_OTHER);

			break;
		case 2:
			newrecord = getOCEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_OTHER);

			break;
		case 256:
			newrecord = getNoChannelEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_LIVE);

			break;
		case 257:
			newrecord = getLiveEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_LIVE);
			break;
		case 258:
			newrecord = getLiveEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_LIVE);
			break;
		case 511:
			newrecord = getLiveEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_LIVE);
			break;
		case 769:
			newrecord = getTVAppEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_TVAPP);
			break;
		case 770:
			newrecord = getTimeShiftEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_TIMESHIFT);
			break;
		case 771:
			newrecord = getVODPageEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_VODPAGE);
			break;
		case 772:
			newrecord = getVODVideoEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_VODPROGRAMME);
			break;
		case 773:
			newrecord = getLookBackPageEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_LOOKBACKPAGE);
			break;
		case 774:
			newrecord = getLookBackEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_LOOKBACK);
			break;
		case 000:
			newrecord = getOtherEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_OTHER);
			break;
		case 1537:
			newrecord = getAD1Event(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_ADS);
			break;
		case 1538:
			newrecord = getAD1Event(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_ADS);
			break;
		case 65535:
			newrecord = getUndefinedEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_OTHER);
			break;
		case 999999:
			head = eventID + OtherConstants.VERTICAL_DELIM
					+ TypeConstans.OFF_CODE + OtherConstants.VERTICAL_DELIM
					+ h[1] + OtherConstants.VERTICAL_DELIM + h[0]
					+ OtherConstants.VERTICAL_DELIM;
			newrecord = getOCEvent(head, tail, split);
			mos.write(dateID, null, new Text(newrecord), dateID
					+ OtherConstants.FILE_SEPARATOR + OutputPath.HIVE_PREFIX
					+ OutputPath.HIVE_OTHER);
			break;
		}
	}

	// 开关机事件
	public String getOCEvent(String head, String tail, String[] split) {
		String OCEvent = "";
			if (split[3].equals("") || split[3] == null) {
				OCEvent = head + "1"+ tail;
			} else {
				OCEvent = head + split[3] + tail;
			}		
		return OCEvent;
	}

	// 空频道事件
	public String getNoChannelEvent(String head, String tail, String[] split) {
		String noChannelEvent = "";
		noChannelEvent = head + split[3] + tail;
		return noChannelEvent;
	}

	// 直播事件
	public String getLiveEvent(String head, String tail, String[] split) {
		String liveEvent = "";
		liveEvent = head + split[10] + tail;
		return liveEvent;
	}

	// 回看页面事件
	public String getLookBackPageEvent(String head, String tail, String[] split) {
		String lookBackPageEvent = "";
		lookBackPageEvent = head + split[3] +OtherConstants.VERTICAL_DELIM+split[5]+ tail;
		return lookBackPageEvent;
	}

	// 回看事件
	public String getLookBackEvent(String head, String tail, String[] split) {
		String lookBackEvent = "";
		lookBackEvent = head + split[3] + OtherConstants.VERTICAL_DELIM
				+ split[6] + " " + split[7] + OtherConstants.VERTICAL_DELIM
				+ split[5] + OtherConstants.VERTICAL_DELIM + split[8]
				+ OtherConstants.VERTICAL_DELIM + split[9] + tail;
		return lookBackEvent;
	}

	// 电视应用事件
	public String getTVAppEvent(String head, String tail, String[] split) {
		String TVAppEvent = "";
		TVAppEvent = head + split[3] + tail;
		return TVAppEvent;
	}

	// 时移事件
	public String getTimeShiftEvent(String head, String tail, String[] split) {
		String TimeShiftEvent = "";
		TimeShiftEvent = head + split[9] + OtherConstants.VERTICAL_DELIM
				+ split[6] + OtherConstants.VERTICAL_DELIM + split[7] + tail;
		return TimeShiftEvent;
	}

	// VOD页面事件
	public String getVODPageEvent(String head, String tail, String[] split) {
		String VODPageEvent = "";
		VODPageEvent = head + split[4] + tail;
		return VODPageEvent;
	}

	// VOD影片事件
	public String getVODVideoEvent(String head, String tail, String[] split) {
		String VODVideoEvent = "";
		VODVideoEvent = head + split[8] + OtherConstants.VERTICAL_DELIM
				+ split[3] + OtherConstants.VERTICAL_DELIM + split[7] + tail;
		return VODVideoEvent;
	}

	// 其他业务事件
	public String getOtherEvent(String head, String tail, String[] split) {
		String OtherEvent = "";
		StringBuffer para=new StringBuffer();
		for(int i=3;i<split.length;i++){
			para.append(split[i]).append(OtherConstants.EXCLAMATION_DELIM);
		}
		OtherEvent=head+para.delete(para.length()-1, para.length()).toString()+tail;
		return OtherEvent;
	}

	// 未知业务事件
	public String getUndefinedEvent(String head, String tail, String[] split) {
		String OtherEvent = "";
		OtherEvent = head + tail;
		return OtherEvent;
	}

	// 广告事件
	public String getAD1Event(String head, String tail, String[] split) {
		String AD1Event = "";
		if(split.length>8){
			AD1Event=head+split[6]+OtherConstants.VERTICAL_DELIM
					+split[7]+OtherConstants.VERTICAL_DELIM
					+split[3]+OtherConstants.VERTICAL_DELIM
					+split[8]+OtherConstants.VERTICAL_DELIM
					+split[5]+OtherConstants.VERTICAL_DELIM
					+split[4]
					+tail;
		}else{
			StringBuffer para=new StringBuffer();
			for(int i=3;i<split.length;i++){
				para.append(split[i]).append(OtherConstants.VERTICAL_DELIM);
			}
			AD1Event=head+para.delete(para.length()-1, para.length()).toString()+tail;
		}
		return AD1Event;
	}

	// 获取事件一分钟区间
	public static String getTimePart1M(String startTime, String endTime) {
		long partTime = TIME_PART_1M;
		String result = "";
		long start = 0;
		long end = 0;
		try {
			start = DateUtil.getSecondsInDay(DateUtil.TIME_FORMATER
					.format(DateUtil.DATE_TIME_FORMATER.parse(startTime)));
			end = DateUtil.getSecondsInDay(DateUtil.TIME_FORMATER
					.format(DateUtil.DATE_TIME_FORMATER.parse(endTime)));
		} catch (ParseException e) {
			log.warn("记录的开始时间和结束时间不正确");
			e.printStackTrace();
		}

		long iStart = (start - start % partTime);
		for (; iStart < end; iStart += partTime) {
			long watch = (iStart + partTime) > end ? end - start : iStart
					+ partTime - start;
			result += iStart + ":" + watch + ",";
			start += watch;
		}
		if (result.length() == 0) {
			result += (start - (start % partTime)) + ":0,";
		}
		return result.substring(0, result.length() - 1);
	}

	public static void main(String[] args) throws Exception {
		String a=",|1|||asd|||||||";
		StringTokenizer c= new StringTokenizer(a, "|");
		if(c.hasMoreElements()){
			System.out.println(c.nextToken());			
		}
		String[] b=a.split(OtherConstants.VERTICAL_DELIM_REGEX,100);
		System.out.println(b);
	}

}
