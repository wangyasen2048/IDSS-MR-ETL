package com.cucrz.idss.hadoop.etl.synchronizeBaseData.vodInfo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;
import org.jaxen.pattern.PatternHandler;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.channelBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.columnBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.video2columnBean;
import com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean.videoBean;
import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class hdfsVOD {
	private static Logger log = Logger.getLogger(hdfsVOD.class);
	private static String regionID;


	public static List getVODInfoFromHDFS(Configuration conf) {
		regionID = synchronizeVOD.getDefaultRegion(conf.get("operatorID"));
		Path newCloumn = new Path(conf.get("fs.defaultFS") + File.separator
				+ conf.get("outputPath") + File.separator
				+ conf.get("operatorID") + File.separator
				+ conf.get("inputDateID") + File.separator + "newCloumn"
				+ File.separator);
		Path CacheVOD = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_VODInfoList"));
		String delim=Pattern.quote(conf.get("idss_ETL_VODInfoList_Separator").trim());
		
		Set<columnBean> cloumnSet = new HashSet<columnBean>();
		Set<videoBean> videoSet = new HashSet<videoBean>();
		Set<video2columnBean> video2cloumnSet = new HashSet<video2columnBean>();
		List vodList = new ArrayList();
		List tmpList = new ArrayList();
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(newCloumn)){
			FileStatus[] files = fs.listStatus(newCloumn);
			for (FileStatus f : files) {
				if (!f.isDir()) {
					tmpList = readNewCloumnFile(f.getPath(), fs,
							conf.get("operatorID"), delim);
					cloumnSet.addAll((Set<columnBean>) tmpList.get(0));
					videoSet.addAll((Set<videoBean>) tmpList.get(1));
					video2cloumnSet.addAll((Set<video2columnBean>) tmpList
							.get(2));
				}
			}
			}
//			tmpList = readCacheCloumnHDFSFile(CacheVOD, fs,
//					conf.get("operatorID"),delim);
			if(tmpList!=null&&tmpList.size()>0){
 			cloumnSet.addAll((Set<columnBean>) tmpList.get(0));
			videoSet.addAll((Set<videoBean>) tmpList.get(1));
			video2cloumnSet.addAll((Set<video2columnBean>) tmpList.get(2));
		}
			vodList.add(0, cloumnSet);
			vodList.add(1, videoSet);
			vodList.add(2, video2cloumnSet);
			return vodList;
		} catch (IOException e) {
			log.warn("栏目对照表路径错误！");
			e.printStackTrace();
			return null;
		}
	}

	public static List readNewCloumnFile(Path path, FileSystem fs,
			String operID, String delim) {
		
		try {
			if(fs.exists(path)){
			InputStream fsis = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
			List vodLIst = new ArrayList();
			Set<columnBean> cloumnSet = new HashSet<columnBean>();
			Set<videoBean> videoSet = new HashSet<videoBean>();
			Set<video2columnBean> video2cloumnSet = new HashSet<video2columnBean>();
			String line =br.readLine();
			while (line != null) {
				line=new String(line.getBytes(),"UTF-8");
				String[] split = line.split(delim,20);
				columnBean cloumn = null;
				video2columnBean video2cloumn = null;
				videoBean video = null;
//				if (split.length == 1) {
//					video = new videoBean(split[0], split[0], "", "", operID,
//							DateUtil.DATE_TIME_FORMATER.format(new Date()),
//							DateUtil.DATE_TIME_FORMATER.format(new Date()));
//				} else if (split.length == 2) {
//					video = new videoBean(split[0], split[0], split[1], "", operID,
//							DateUtil.DATE_TIME_FORMATER.format(new Date()),
//							DateUtil.DATE_TIME_FORMATER.format(new Date()));
//				} else if (split.length == 3) {
//					video2cloumn = new video2columnBean(split[2], "", split[0],
//							split[1], operID,
//							DateUtil.DATE_TIME_FORMATER.format(new Date()),
//							DateUtil.DATE_TIME_FORMATER.format(new Date()));
//					video = new videoBean(split[0],split[0], split[1], "", operID,
//							DateUtil.DATE_TIME_FORMATER.format(new Date()),
//							DateUtil.DATE_TIME_FORMATER.format(new Date()));
//					cloumn = new columnBean(split[2], operID, split[2], "", "",
//							0, "", 0, regionID,
//							DateUtil.DATE_TIME_FORMATER.format(new Date()),
//							DateUtil.DATE_TIME_FORMATER.format(new Date()));
//				} else 
				if (split.length >4) {
					video2cloumn = new video2columnBean(split[2], split[3],
							split[0], split[1], operID,
							DateUtil.DATE_TIME_FORMATER.format(new Date()),
							DateUtil.DATE_TIME_FORMATER.format(new Date()));
					video = new videoBean(split[0], split[0], split[1], "", operID,
							DateUtil.DATE_TIME_FORMATER.format(new Date()),
							DateUtil.DATE_TIME_FORMATER.format(new Date()));
					cloumn = new columnBean(split[2], operID, split[2], "",
							split[3], 3, "", 0, split[4],
							DateUtil.DATE_TIME_FORMATER.format(new Date()),
							DateUtil.DATE_TIME_FORMATER.format(new Date()));
				}
				cloumnSet.add(cloumn);
				videoSet.add(video);
				video2cloumnSet.add(video2cloumn);
				line = br.readLine();
			}
			cloumnSet.remove(null);
			videoSet.remove(null);
			video2cloumnSet.remove(null);
			vodLIst.add(0, cloumnSet);
			vodLIst.add(1, videoSet);
			vodLIst.add(2, video2cloumnSet);
			return vodLIst;
			}else{
				return null;
			}
		} catch (IOException e) {
			log.warn("栏目对照表路径有误！");
			e.printStackTrace();
			return null;
		}
		
	}

	public static List readCacheCloumnHDFSFile(Path vodPath, FileSystem fs,
			String operID,String delim) {
		try {
			if(fs.exists(vodPath)){
			InputStream fsis = fs.open(vodPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
			List vodLIst = new ArrayList();
			Set<columnBean> cloumnSet = new HashSet<columnBean>();
			Set<videoBean> videoSet = new HashSet<videoBean>();
			Set<video2columnBean> video2cloumnSet = new HashSet<video2columnBean>();
			String line =br.readLine();
			while (line != null) {
				line=new String(line.getBytes(),"UTF-8");
				String[] split1 = line.split(delim,30);
				if (split1.length > 2) {
					String[] split2 = split1[0]
							.split(OtherConstants.VERTICAL_DELIM_REGEX,10);
					columnBean cloumn = null;
					video2columnBean video2cloumn = null;
					videoBean video = null;
//					if (split1.length == 1) {
//						if (split2.length == 1) {
//							video = new videoBean(split2[0], split2[0], "", "",
//									operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//						} else if (split2.length == 2) {
//							video = new videoBean(split2[0], split2[0], split2[1], "",
//									operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//						}
//					} else if (split1.length == 2) {
//						if (split2.length == 1) {
//							video2cloumn = new video2columnBean(split1[1], "",
//									split2[0], "", operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//							video = new videoBean(split2[0],split2[0], "", "",
//									operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//						} else if (split2.length == 2) {
//							video2cloumn = new video2columnBean(split1[1], "",
//									split2[0], split2[1], operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//							video = new videoBean(split2[0],split2[0], split2[1], "",
//									operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//						}
//						cloumn = new columnBean(split1[1], operID, split1[1], "", "",
//								0, "", 0, "", "",
//								DateUtil.DATE_TIME_FORMATER.format(new Date()));
//					} else
//						if (split1.length == 3) {
//						if (split2.length == 1) {
//							video2cloumn = new video2columnBean(split1[1], "",
//									split2[0], "", operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//							video = new videoBean(split2[0], split2[0], "", "",
//									operID,
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()),
//									DateUtil.DATE_TIME_FORMATER
//											.format(new Date()));
//						} else 
							if (split2.length >2) {
							video2cloumn = new video2columnBean(split1[1], "",
									split2[0], split2[1], operID,
									DateUtil.DATE_TIME_FORMATER
											.format(new Date()),
									DateUtil.DATE_TIME_FORMATER
											.format(new Date()));
							video = new videoBean(split2[0],split2[0], split2[1], "",
									operID,
									DateUtil.DATE_TIME_FORMATER
											.format(new Date()),
									DateUtil.DATE_TIME_FORMATER
											.format(new Date()));
//						}
						cloumn = new columnBean(split1[1], operID, split1[1], "",
								split1[2], 0, "", 0, "",
								DateUtil.DATE_TIME_FORMATER.format(new Date()),
								DateUtil.DATE_TIME_FORMATER.format(new Date()));
					};
					cloumnSet.add(cloumn);
					videoSet.add(video);
					video2cloumnSet.add(video2cloumn);
					line = br.readLine();
				}
			}
			cloumnSet.remove(null);
			videoSet.remove(null);
			video2cloumnSet.remove(null);
			vodLIst.add(0, cloumnSet);
			vodLIst.add(1, videoSet);
			vodLIst.add(2, video2cloumnSet);
			return vodLIst;
			}else{
				return null;
			}
		} catch (IOException e) {
			log.warn("VOD对照表路径有误！");
			e.printStackTrace();
			return null;
		}
	}

	public static void updateVODHDFSFile(Configuration conf,
			Set<video2columnBean> video2cloumnSet,long times) {
		Path CacheVODInfo = new Path(conf.get("preCachePath")
				+ conf.get("operatorID") + OtherConstants.FILE_SEPARATOR
				+ conf.get("idss_ETL_Cache_VODInfoList"));
		String delim=Pattern.quote(conf.get("idss_ETL_VODInfoList_Separator").trim());
		try {
			DistributedFileSystem fs =(DistributedFileSystem) FileSystem.get(conf);
			FSDataOutputStream osVOD;
			if(times==0L){
				FileSystem fs1 = FileSystem.newInstance(conf);
			if (fs.exists(CacheVODInfo)) {
				fs.delete(CacheVODInfo);
			}
			osVOD= fs1.create(CacheVODInfo); 
			fs1.close();
			}
			 osVOD = fs.append(CacheVODInfo);
			OutputStreamWriter oswVOD = new OutputStreamWriter(osVOD);
			BufferedWriter bwVOD = new BufferedWriter(oswVOD);
			for (video2columnBean video2cloumn : video2cloumnSet) {
				bwVOD.append(video2cloumn.getVideo_id()
						+ OtherConstants.VERTICAL_DELIM
						+ video2cloumn.getVideo_name()
						+ delim
						+ video2cloumn.getColumn_id()
						+ delim
						+ video2cloumn.getColumn_name());
				bwVOD.newLine();
			}
			bwVOD.flush();
			bwVOD.close();

			log.info("-------------VOD对照表第"+(times+1L)+"次更新完成！--------------");
		} catch (IOException e) {
			log.warn("-------------VOD对照列表路径错误！------------");
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		String a= null;
		try {
			byte[] b= a.getBytes("UTF-8");
			String c =new String(b,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
