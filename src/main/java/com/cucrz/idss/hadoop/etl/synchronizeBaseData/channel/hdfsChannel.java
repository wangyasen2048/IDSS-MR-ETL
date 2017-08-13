package com.cucrz.idss.hadoop.etl.synchronizeBaseData.channel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.channelBean;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class hdfsChannel {
	private static Logger log = Logger.getLogger(hdfsChannel.class);

	public static Set<channelBean> getChannelFromHDFS(Configuration conf) {
		Path newChannel = new Path(conf.get("fs.defaultFS") + File.separator
				+ conf.get("outputPath") + File.separator
				+ conf.get("operatorID") + File.separator
				+ conf.get("inputDateID") + File.separator + "newChannel"
				+ File.separator);
		Path CacheCodeChannel = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		Path CacheNameChannel = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_NameToUniqueChannel"));
		String fieldDelim=conf.get("idss_ETL_ChannelList_Separator");
		String columnDelim=conf.get("idss_ETL_ChannelList_Separator");
		Set<channelBean> channelSet = new HashSet<channelBean>();
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(newChannel)){
			FileStatus[] files = fs.listStatus(newChannel);
			for (FileStatus f : files) {
				if (!f.isDir()) {
					Set<channelBean> tmpSet1 = readNewChannelFile(f.getPath(),
							fs, conf.get("operatorID"),
							fieldDelim,columnDelim);
					if (tmpSet1 != null && tmpSet1.size() > 0) {
						channelSet.addAll(tmpSet1);
					}
				}
			}
			}
			Set<channelBean> tmpSet2 = readCacheChannelHDFSFile(
					CacheCodeChannel, CacheNameChannel, fs,
					conf.get("operatorID"));
			if (tmpSet2 != null && tmpSet2.size() > 0) {
				channelSet.addAll(tmpSet2);
			}
			return channelSet;
		} catch (IOException e) {
			log.warn("频道对照表路径错误！");
			e.printStackTrace();
			return null;
		}
	}

	public static Set<channelBean> readNewChannelFile(Path path, FileSystem fs,
			String operID, String fieldDelim,String columnDelim) {

		try {
			if (fs.exists(path)) {
				InputStream fsis = fs.open(path);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fsis));
				Set<channelBean> channelSet = new HashSet<channelBean>();
				String line = br.readLine();
				while (line != null) {
					line = new String(line.getBytes(),"UTF-8");
					String[] split = line.split(Pattern.quote(fieldDelim));
					String[] cids=split[0].split(Pattern.quote(columnDelim),5) ;
					if(split[0].isEmpty()){
						cids=new String[] {"","",""};
					}else if(cids.length<=2){
						cids=new String[] {"","",split[0]};
					}
					channelBean channel = null;
					if (split.length == 1) {
						channel = new channelBean(split[0], "", operID,
								cids[2], cids[1], cids[0],
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					} else if (split.length > 1) {
						channel = new channelBean(split[0], split[1], operID,
								cids[2], cids[1], cids[0],
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					}
					channelSet.add(channel);
					line = br.readLine();
				}
				return channelSet;
			} else {
				return null;
			}
		} catch (IOException e) {
			log.warn("新频道文件路径有误！");
			e.printStackTrace();
			return null;
		}
	}

	public static Set<channelBean> readCacheChannelHDFSFile(Path codePath,
			Path namePath, FileSystem fs, String operID) {
		Set<channelBean> codeSet = new HashSet<channelBean>();
		Set<channelBean> nameSet = new HashSet<channelBean>();
		Set<channelBean> channelSet = new HashSet<channelBean>();
		codeSet = readCodeChannel(codePath, fs, operID,
				OtherConstants.VERTICAL_DELIM_REGEX);
		nameSet = readNameChannel(namePath, fs, operID,
				OtherConstants.VERTICAL_DELIM_REGEX);
		if (codeSet != null && codeSet.size() > 1) {
			channelSet.addAll(codeSet);
			if (nameSet != null && nameSet.size() > 1) {
				for (channelBean Name : nameSet) {
					String uniqChannel = Name.getChannel_code();
					if (uniqChannel != null) {
						for (channelBean Code : codeSet) {
							if (Code.getChannel_code() != null) {
								if (uniqChannel.equals(Code.getChannel_code())) {
									if (Name.getRegion_channelname() != null) {
										Code.setRegion_channelname(Name
												.getRegion_channelname());
										channelSet.add(Code);
									}
								}
							}
						}
					}
				}
				return channelSet;
			} else {
				return codeSet;
			}
		} else if (nameSet != null && nameSet.size() > 1) {
			return nameSet;
		}
		return null;
	}

	public static Set<channelBean> readNameChannel(Path namePath,
			FileSystem fs, String operID, String delim) {
		try {
			if (fs.exists(namePath)) {
				InputStream fsis = fs.open(namePath);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fsis));
				Set<channelBean> channelSet = new HashSet<channelBean>();
				String line =br.readLine();
				while (line != null) {
					line = new String(line.getBytes(),"UTF-8");
					String[] split = line.split(delim);
					channelBean channel = null;
					if (split.length == 1) {
						channel = new channelBean("", split[0], operID, "", "",
								"",
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					} else if (split.length > 1) {
						channel = new channelBean(split[1], split[0], operID,
								"", "", "",
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					}
					channelSet.add(channel);
					line = br.readLine();
				}
				channelSet.remove(null);
				return channelSet;
			} else {
				return null;
			}
		} catch (IOException e) {
			log.warn("------------频道名称对照表路径有误！----------");
			e.printStackTrace();
			return null;
		}
	}

	public static Set<channelBean> readCodeChannel(Path codePath,
			FileSystem fs, String operID, String delim) {
		try {
			if (fs.exists(codePath)) {
				InputStream fsis = fs.open(codePath);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fsis));
				Set<channelBean> channelSet = new HashSet<channelBean>();
				String line =br.readLine();
				while (line != null) {
					 line = new String(line.getBytes(),"UTF-8");
					String[] split = line.split(delim);
					String[] cids=split[0].split("#",5) ;
					if(split[0].isEmpty()){
						cids=new String[] {"","",""};
					}else if(cids.length<=2){
						cids=new String[] {"","",split[0]};
					}
					channelBean channel = null;
					if (split.length == 1) {
						channel = new channelBean("", "", operID, 
								cids[2], cids[1], cids[0],
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					} else if (split.length > 1) {
						channel = new channelBean(split[1], "", operID,
								cids[2], cids[1], cids[0],
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					}
					channelSet.add(channel);
					line = br.readLine();
				}
				channelSet.remove(null);
				return channelSet;
			} else {
				return null;
			}
		} catch (IOException e) {
			log.warn("-----------频道编号对照表路径有误！-----------");
			e.printStackTrace();
			return null;
		}
	}

	public static void updateHDFSFile(Configuration conf,
			Set<channelBean> channelSet) {
		Path Name2Channel = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_NameToUniqueChannel"));
		Path channel2Channel = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_OriginalChannelToUniqueChannel"));
		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(Name2Channel)) {
				fs.delete(Name2Channel);
			}
			if (fs.exists(channel2Channel)) {
				fs.delete(channel2Channel);
			}
			OutputStream osName = fs.create(Name2Channel);
			OutputStream osCode = fs.create(channel2Channel);
			OutputStreamWriter oswName = new OutputStreamWriter(osName);
			OutputStreamWriter oswCode = new OutputStreamWriter(osCode);
			BufferedWriter bwName = new BufferedWriter(oswName);
			BufferedWriter bwCode = new BufferedWriter(oswCode);

			for (channelBean channel : channelSet) {
				bwName.write(channel.getRegion_channelname()
						+ OtherConstants.VERTICAL_DELIM
						+ channel.getChannel_code());
				bwName.newLine();
				bwCode.write(channel.getNet_id()+"#"+channel.getTs_id()+"#"+channel.getService_id()
						+ OtherConstants.VERTICAL_DELIM
						+ channel.getChannel_code());
				bwCode.newLine();
			}
			bwName.flush();
			bwCode.flush();
			osName.close();
			osCode.close();
			fs.close();
			log.info("-------------频道对照表更新完成！--------------");
		} catch (IOException e) {
			log.warn("-------------频道对照列表路径错误！------------");
			e.printStackTrace();	
		}
	}

}
