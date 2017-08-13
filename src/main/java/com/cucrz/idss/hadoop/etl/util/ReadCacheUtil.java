package com.cucrz.idss.hadoop.etl.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;

public class ReadCacheUtil {
	private static Logger log = Logger.getLogger(ReadCacheUtil.class);

	// 读取分布式缓存
	public static Map<String, String> readCache(BufferedReader reader,
			String cacheFileName) throws Exception {
		Map<String, String> result = new HashMap<String, String>();
		String line = reader.readLine();
		while (null != line) {
			String[] split = line.split(OtherConstants.VERTICAL_DELIM_REGEX,2);
			if (split.length >= 2) {
				result.put(split[0], split[1]);
			} else if (split.length == 1) {
				result.put(split[0], "");
			}
			line = reader.readLine();

		}
		return result;
	}
	// 根据频道名称和逻辑频道号得到统一频道编号
	public static String getUniqueChannel(Map<String, String> newChannel,
			Map<String, String> channelNameList,
			Map<String, String> channelCodeList, String channelCode,
			String channelName) {
		String originalCode = channelCode;
		String channelCode1 = "";
		String channelCode2 = "";
		if (channelNameList != null && channelName != null&&!channelName.equals("")) {
			if (channelNameList.containsKey(channelName)) {
				channelCode1 = channelNameList.get(channelName);
			}
		}
		if (channelCodeList != null && originalCode != null&&!originalCode.equals("")) {
			if (channelCodeList.containsKey(originalCode)) {
				channelCode2 = channelCodeList.get(originalCode);
			}
		}
		if (!channelCode1.equals("")) {
			if (!channelCode2.equals("")) {
				channelCode = channelCode2;
			} else {
				channelCode = channelCode1;
			}
		} else {
			if (!channelCode2.equals("")) {
				channelCode = channelCode2;
			} else {
				if(originalCode == null || originalCode.equals("")){
				channelCode = "1";
			}
			}
		}
		// 输出未知的ID
		if (originalCode == null || originalCode.equals("")) {
			if (channelCode == null || channelCode.equals("")) {
				channelCode = "1";
			}
		}
		if (newChannel != null) {
			if (originalCode.equals(channelCode)) {
				newChannel.put(originalCode + "," + channelName, null);
			}
		}
		return channelCode;
	}

	// 根据区域对照表转换统一区域ID
	public static String getUniqueRegion(Map<String, String> regionCodeList,
			String regionCode, String defaultRegion) {
		if (regionCodeList != null && regionCode != null) {
			if (regionCodeList.containsKey(regionCode)) {
				regionCode = regionCodeList.get(regionCode);
			} else {
				regionCode = defaultRegion;
			}
		}
		return regionCode;
	}

	// 是否新VOD信息
	public static void getNewVODInfo(Map<String, String> newCloumn,
			Map<String, String> VODInfoList, String programInfo,
			String cloumnInfo,String delim,String regionID) {
		if (VODInfoList != null) {
			programInfo=programInfo.trim();
			if(programInfo!=null&&!programInfo.equals("")&&!programInfo.equals(delim)){
				String[] split=programInfo.split(Pattern.quote(delim));
			if (!VODInfoList.containsKey(split[0])) {
				newCloumn.put(programInfo + delim + cloumnInfo, null);
			}
			}
		}else{
			newCloumn.put(programInfo + delim + cloumnInfo+delim+regionID, null);
		}
	}
	//使用MD5转化字符串
	final static char[] hexChar = { '0', '1', '2', '3', '4', '5', '6', '7','8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };   
	public static String getMD5String(String str){
		String cryptographicString=null;
		try {
			byte[] strByte=str.getBytes();
			MessageDigest mdInst = MessageDigest.getInstance("MD5");
			mdInst.update(strByte);
			byte[] code=mdInst.digest();
			cryptographicString=toHexString(code);
		} catch (NoSuchAlgorithmException e) {
			log.error("没有MD5加密算法，点播ID生成失败");
			e.printStackTrace();
		}	
		return cryptographicString;
	}
	//转化字节数组为十六进制字符串
	public static String toHexString(byte[] b) {    
        StringBuilder sb = new StringBuilder(b.length * 2);    
        for (int i = 0; i < b.length; i++) {    
            sb.append(hexChar[(b[i] & 0xf0) >>> 4]);    
            sb.append(hexChar[b[i] & 0x0f]);    
        }    
        return sb.toString();    
    }    
	// 是否新用户
	public static boolean isNewUser(Map<String, String> userList,
			String userID, String userRegion) {
		if (userList != null && userID != null) {
			if (!userList.containsKey(userID + "!" + userRegion)) {
				return true;
			}
		}
		return false;
	}

	// 根据正则表达式获取配置
	public static Set<String> getPropertiesByRegex(String regex, Context context) {
		Set<String> set = new TreeSet<String>();
		Map<String, String> map = context.getConfiguration().getValByRegex(regex);
		Set<String> keys = map.keySet();
		for (String name : keys) {
			set.add(map.get(name));
		}
		return set;
	}
	//获取UUID
	public static String getUUID(){
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

	public static void convertChannelList() throws IOException {
		FileInputStream fis = new FileInputStream("D:/result.csv");
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		FileInputStream fis2 = new FileInputStream("D:/col.txt");
		InputStreamReader isr2 = new InputStreamReader(fis2);
		BufferedReader br2 = new BufferedReader(isr2);
		Map<String, String> channelNameList;
		try {
			channelNameList = readCache(br2, null);
			Set<String> tmp = new TreeSet<String>();
			FileOutputStream fos1 = new FileOutputStream(
					"C:/Users/wei/Desktop/lookback.txt");
			FileOutputStream fos2 = new FileOutputStream(
					"C:/Users/wei/Desktop/cn.txt");
			OutputStreamWriter osw1 = new OutputStreamWriter(fos1);
			OutputStreamWriter osw2 = new OutputStreamWriter(fos2);
			BufferedWriter bw1 = new BufferedWriter(osw1);
			BufferedWriter bw2 = new BufferedWriter(osw2);
			String line = br.readLine();
			while (line != null) {

				String[] split = line.split(",");
				String[] s = split[2].split("/");
				if (s[0].equals("42029")) {
					String[] s1 = split[1].split(" ");
					if (s1.length > 1) {
						String[] s2 = split[3].split("/");
						if (s2.length > 3) {
							String date = DateUtil.DATEFORMATER
									.format(DateUtil.DATE_FORMATER
											.parse(s2[s2.length - 1]));
							if (date.compareTo("20140901") >= 0) {
								String code = getUniqueChannel(null,
										channelNameList, null, null,
										s2[s2.length - 2]);
								bw1.write(split[0] + "|" + s1[1] + "!"
										+ s2[s2.length - 2] + "!"
										+ s2[s2.length - 1] + "!" + s1[0]
										+ ":00" + "!" + code);
								bw1.newLine();
							}
						} else {
							// System.out.println(s2);
						}
					} else {
						// System.out.println(s1);
					}
				}
				line = br.readLine();
			}

			// for(String s:tmp){
			// bw2.write(s);
			// bw2.newLine();
			// }
			bw1.flush();
			bw2.flush();
			fos1.close();
			fos2.close();
			fis.close();
		} catch (Exception e) {

			e.printStackTrace();
		}
		System.out.println("格式转换完成");
	}
	 public static void main(String[] args) {
		 System.out.println(ReadCacheUtil.getMD5String("天天向上"));
		 System.out.println("asdad");
	 }
}
