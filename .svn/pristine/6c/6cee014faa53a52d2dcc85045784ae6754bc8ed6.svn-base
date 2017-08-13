package com.cucrz.idss.hadoop.etl.mapreduce.out.form;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cucrz.idss.hadoop.etl.mapreduce.constants.OtherConstants;
import com.cucrz.idss.hadoop.etl.mapreduce.constants.OutputPath;
import com.cucrz.idss.hadoop.etl.mapreduce.out.IETLOutput;

public class source implements IETLOutput {

	@Override
	public void outputRecords(Set<String> records, String userID,
			MultipleOutputs<Text, Text> mos, Context context) {
		String operID = context.getConfiguration().get("operatorID");
		String inputDateID = context.getConfiguration().get("inputDateID");
		Iterator<String> it = records.iterator();
		while (it.hasNext()) {
			String line = it.next();
			String[] uSplit = userID.split(OtherConstants.VERTICAL_DELIM_REGEX);
			String[] split = line.split(OtherConstants.VERTICAL_DELIM_REGEX,20);
			if (split.length > 2&&uSplit.length==2) {
				String regionID = uSplit[1];
				String newrecord = operID + OtherConstants.VERTICAL_DELIM
						+ regionID + OtherConstants.VERTICAL_DELIM + uSplit[0]
						+ OtherConstants.VERTICAL_DELIM;
				newrecord = newrecord + line;
				try {
					mos.write(inputDateID, null, new Text(newrecord),
							inputDateID + OtherConstants.FILE_SEPARATOR
									+ OutputPath.SOURCE_PREFIX
									+ OutputPath.SOURCE + regionID);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
