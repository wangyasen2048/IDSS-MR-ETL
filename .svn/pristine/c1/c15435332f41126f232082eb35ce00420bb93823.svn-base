package com.cucrz.idss.hadoop.etl.mapreduce.map.program;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.TypeConstans;
import com.cucrz.idss.hadoop.etl.mapreduce.map.IETLMapper;
import com.cucrz.idss.hadoop.etl.mapreduce.out.form.hiveEvent;
import com.cucrz.idss.hadoop.etl.util.DateUtil;
import com.cucrz.idss.hadoop.etl.util.ReadCacheUtil;

public class LiveProgram  implements IETLMapper{
	private static Configuration conf = null;
	private static Logger log =Logger.getLogger(LiveProgram.class);
	private static Map<String, List<String[]>> ProgramSchedule = null;
	
	@Override
	public Map<String, Map<String, String>> etlMapSetupDisCache(Context context,
			Map<String, Map<String, String>> storage) {
		Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
		Map<String, String> cacheFileNameMap = context.getConfiguration()
				.getValByRegex("^idss_ETL_Cache");
		Set<String> cacheFileNameSet = cacheFileNameMap.keySet();
		for (String cacheFileName : cacheFileNameSet) {
			cacheFileName = cacheFileNameMap.get(cacheFileName);
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						cacheFileName));
				resultMap.put(cacheFileName,ReadCacheUtil.readCache(reader, cacheFileName));			
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		ProgramSchedule=getProgramSchedule(context);
		return resultMap;
	}

	@Override
	public Map<String, Counter> etlMapSetupGetCounter(Context context) {
		Map<String, Counter> map = new HashMap<String, Counter>();
		Counter mapTotleRows = (Counter) context.getCounter("programMap", "TotleRows");
		Counter mapOutputRows = (Counter) context.getCounter("programMap",
				"mapOutputRows");
		Counter mapFmtError = (Counter) context
				.getCounter("programMap", "mapFmtError");
		Counter mapDateIdError = (Counter) context.getCounter("programMap",
				"mapDateIdError");
		map.put("TotleRows", mapTotleRows);
		map.put("mapOutputRows", mapOutputRows);
		map.put("mapFmtError", mapFmtError);
		map.put("mapDateIdError", mapDateIdError);
		return map;
	}

	@Override
	public String etlMapper(String inputLine, String inFileName, String operId, String inputDateID, Context context,
			Map<String, Map<String, String>> storage, Map<String, Counter> counterMap, Map<String, String> newChannel,
			Map<String, String> newCloumn) {
		conf = context.getConfiguration();
		Counter mapParamError = counterMap.get("mapParamError");
		Counter mapOutputRows = counterMap.get("mapOutputRows");
	
		String[] split = inputLine.split(OtherConstants.VERTICAL_DELIM_REGEX);
		String head=split[3]+OtherConstants.VERTICAL_DELIM+split[2];
		String channelID=split[4];
		String sTime=split[5];
		String eTime=split[6];
		StringBuilder sb=new StringBuilder();
		if(ProgramSchedule!=null){
		if(ProgramSchedule.containsKey(channelID)){
			List<String[]> programs= ProgramSchedule.get(channelID);
			List<String> pList = getProgramEvent(head, sTime, eTime, channelID, programs);
			Collections.sort(pList, new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					String[] p1=o1.split(OtherConstants.VERTICAL_DELIM_REGEX);
					String[] p2=o2.split(OtherConstants.VERTICAL_DELIM_REGEX);
					return p1[6].compareTo(p2[6]);
				}	
			});		
			for (String s : pList){				
					sb.append(s).append(OtherConstants.ENTER_DELIM);	
			}
		}}
		return sb.toString();
	}
	
	@SuppressWarnings( { "rawtypes", "unchecked" } )
	public List<String> getProgramEvent(String head,String sTime,String  eTime,String channelID,List<String[]> programs){
		List<String> programEvent= new ArrayList<>();
		try {
			String st=DateUtil.DATE_TIME_FULL.format(DateUtil.DATE_TIME_FORMATER.parse(sTime));
			String et=DateUtil.DATE_TIME_FULL.format(DateUtil.DATE_TIME_FORMATER.parse(eTime));
			long s=Long.valueOf(st);
			long e=Long.valueOf(et);
			int size=programs.size();
			int index=0;
			Set pros=new HashSet<>();
		for(String[] p:programs){
				long s1=Long.valueOf(p[3]);
				long e1=Long.valueOf(p[4]);
				if(s>=s1&&e<e1){
					pros.add(index);
				}
			index++;
			}
			Iterator it = pros.iterator();
		while (it.hasNext()){
			String[] program=programs.get((int)it.next());
			long s1=Long.valueOf(program[3]);
			long e1=Long.valueOf(program[4]);
			long start= s>=s1 ? s :s1;
			long end= e<=e1 ? e :e1;
			sTime=DateUtil.DATE_TIME_FORMATER.format(DateUtil.DATE_TIME_FULL.parse(String.valueOf(start)));
			eTime=DateUtil.DATE_TIME_FORMATER.format(DateUtil.DATE_TIME_FULL.parse(String.valueOf(end)));
			programEvent=createEvent(programEvent, sTime, eTime, program[5], head, channelID);
		}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return programEvent;
	}
	
	public List<String> createEvent(List<String> programEvent,String sTime,String eTime,String programID,String head,String channelID){
		String event = UUID.randomUUID().toString() 
				+ OtherConstants.VERTICAL_DELIM + head
				+ OtherConstants.VERTICAL_DELIM + TypeConstans.EVENT_LIVE_PROGRAM
				+ OtherConstants.VERTICAL_DELIM + channelID 
				+ OtherConstants.VERTICAL_DELIM +programID
				+ OtherConstants.VERTICAL_DELIM + sTime 
				+ OtherConstants.VERTICAL_DELIM + eTime
				+ OtherConstants.VERTICAL_DELIM + DateUtil.getCurrentTime() 
				+ OtherConstants.VERTICAL_DELIM+ hiveEvent.getTimePart1M(sTime, eTime);
		programEvent.add(event);
		return programEvent;
	}
	 
	public Map<String,List<String[]>> getProgramSchedule(Context context){
      	Configuration conf=context.getConfiguration();
      	try {
			FileSystem fs=FileSystem.get(conf);
			FSDataInputStream fdis =  fs.open(new Path(conf.get("fs.defaultFS")+ OtherConstants.FILE_SEPARATOR
					+ conf.get("dataSourcePath")+ OtherConstants.FILE_SEPARATOR + conf.get("operatorID")
					+ OtherConstants.FILE_SEPARATOR + conf.get("inputDateID")+ OtherConstants.FILE_SEPARATOR +conf.get("idss_ETL_ProgramSchedule")));
			BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
			 return getProgramCache(reader);
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
      
}
	
	
	public Map<String,List<String[]>> getProgramCache(BufferedReader reader ){
		Map<String,List<String[]>> programCache= new HashMap<>();
		Set<String> programset = new TreeSet<>();
		String Line;
		try {
			Line = reader.readLine();
		while(Line!=null){
			programset.add(Line);
			Line = reader.readLine();
		}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(programset!=null&&programset.size()>0){
			Iterator<String> iterator= programset.iterator();
			String firstChannel=null;
			String afterChannel=null;
			changeChannel : while (iterator.hasNext()){
				String l=iterator.next();
				String[] param1=l.split(OtherConstants.VERTICAL_DELIM_REGEX);
				if(param1.length==6){
					firstChannel=param1[0];
					List<String[]> pList= new ArrayList<>();					
					pList.add(param1);
					nextProgram : while(iterator.hasNext()){
					String l2=iterator.next();
					String[] param2=l2.split(OtherConstants.VERTICAL_DELIM_REGEX);
					if(param2.length==6){
						afterChannel=param2[0];
						if (firstChannel.equals(afterChannel)&&!firstChannel.equals("")){
							pList.add(param2);
							continue nextProgram;
						}else {
							Collections.sort(pList, new ProgramComparator());
							programCache.put("##"+firstChannel, pList);
							continue changeChannel;
						}
					}
					}
					Collections.sort(pList, new ProgramComparator());
					programCache.put("##"+firstChannel, pList);
			}
			}
		}
		return programCache;
	}

	public static class ProgramComparator implements Comparator<String[]>{

		@Override
		public int compare(String[] o1, String[] o2) {
			
			return o1[3].compareTo(o2[3]);
		}
		
	}
	

}
