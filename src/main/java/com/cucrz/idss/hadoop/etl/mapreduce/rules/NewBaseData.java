package com.cucrz.idss.hadoop.etl.mapreduce.rules;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.mortbay.log.Log;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.rules.ruleUtil.IETLRules;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class NewBaseData implements IETLRules {

	@Override
	public Set<String> executeRule(String user, Set<String> records,
			Context context, MultipleOutputs<Text, Text> mos,
			Map<String, Map<String, String>> storage,
			Map<String, Counter> counterMap) {
		Configuration conf = context.getConfiguration();
		Counter Channel = counterMap.get("newChannel");
		Counter Cloumn = counterMap.get("newCloumn");
		Map<String, String> VODInfoList = storage.get(conf
				.get("idss_ETL_Cache_VODInfoList"));
		Map<String, String> channelNameList = storage.get(conf
				.get("idss_ETL_Cache_NameToUniqueChannel"));
		Map<String, String> channelCodeList = storage.get(conf
				.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Map<String, String> newChannel = new HashMap<String, String>();
		Map<String, String> newCloumn = new HashMap<String, String>();
		Map<String, String> tmpRecord = new HashMap<String, String>();
		String delim=conf.get("idss_ETL_VODInfoList_Separator").trim();
		String regionID=user.split(OtherConstants.VERTICAL_DELIM_REGEX)[1];
		for (String record : records) {
			String[] split = record.split(OtherConstants.VERTICAL_DELIM_REGEX,
					30);
			if (split.length > 2) {
				String bizType = split[2];
				if (split.length > 10) {
					if (bizType.equals(TypeConstans.EVENT_LIVE__DATA_BIZ)
							|| bizType
									.equals(TypeConstans.EVENT_LIVE__DATAVOICE_BIZ)
							|| bizType.equals(TypeConstans.EVENT_LIVE_BIZ)
							|| bizType
									.equals(TypeConstans.EVENT_LIVE_DEFAULTTV)) {
						split[10] = ReadCacheUtil.getUniqueChannel(newChannel,
								channelNameList, channelCodeList, split[3]+"#"+split[4]+"#"+split[5],
								split[9]);
						String newRecord = tranSplitToEvent(split);
						tmpRecord.put(record, newRecord);
					}
				}
				if (split.length > 9) {
					if (bizType.equals(TypeConstans.EVENT_VOD_PROGRAM_BIZ)) {
						ReadCacheUtil.getNewVODInfo(newCloumn, VODInfoList,
								split[3] + delim + split[4], split[8] + delim
										+ split[9],delim,regionID);
						String newRecord = tranSplitToEvent(split);
						tmpRecord.put(record, newRecord);
					}
				}
				if (split.length > 9) {
					if (bizType.equals(TypeConstans.EVENT_TIMESHIFT_BIZ)) {
						split[9] = ReadCacheUtil.getUniqueChannel(newChannel,
								channelNameList, channelCodeList, split[3]+"#"
										+ split[4]+"#" + split[5], split[8]);
						String newRecord = tranSplitToEvent(split);
						tmpRecord.put(record, newRecord);
					}
				}
				if (split.length > 10) {
					if (bizType.equals(TypeConstans.EVENT_LOOKBACK_PRO_BIZ)) {
						split[10] = ReadCacheUtil.getUniqueChannel(newChannel,
								channelNameList, channelCodeList, "##", split[5]);
						String newRecord = tranSplitToEvent(split);
						tmpRecord.put(record, newRecord);
					}
				}
			}

		}
		Set<String> keyset = tmpRecord.keySet();
		for (String key : keyset) {
			if (records.contains(key)) {
				records.remove(key);
				records.add(tmpRecord.get(key));
			}
		}
		String isOutputNewChannel = context.getConfiguration().get(
				"idss_ETL_NewChannel");
		String isOutputNewCloumn = context.getConfiguration().get(
				"idss_ETL_NewCloumn");
		Set<String> channelSet = newChannel.keySet();
		Set<String> cloumnSet = newCloumn.keySet();
		
		if (isOutputNewChannel.equals("true")) {
			for (String row : channelSet) {
//				Log.info("新影片======="+row);
				try {
					mos.write(OutputPath.NEW_CHANNLE, null, new Text(row),
							OutputPath.NEW_CHANNLE_PREFIX
									+ OutputPath.NEW_CHANNLE);
					Channel.increment(1);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		if (isOutputNewCloumn.equals("true")) {
			for (String row : cloumnSet) {
//				Log.info("新栏目======="+row);
				try {
					mos.write(OutputPath.NEWCLOUMN, null, new Text(row),
							OutputPath.NEWCLOUMN_PREFIX + OutputPath.NEWCLOUMN);
					Cloumn.increment(1);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return records;
	}

	public static String tranSplitToEvent(String[] split) {
		String newrecord = "";
		if (split != null) {
			for (int i = 0; i < split.length; i++) {
				newrecord = newrecord + split[i]
						+ OtherConstants.VERTICAL_DELIM;
			}
			int index = newrecord.lastIndexOf(OtherConstants.VERTICAL_DELIM);
			newrecord = newrecord.substring(0, index);
		}
		return newrecord;
	}
}
