/*jadclipse*/// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) radix(10) lradix(10) 
// Source File Name:   TextInputFormat.java

package com.cucrz.idss.hadoop.etl.mapreduce.in;

import com.google.common.base.Charsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

// Referenced classes of package org.apache.hadoop.mapreduce.lib.input:
//            FileInputFormat, LineRecordReader
//自定义inputformat
public class ETLInputFormat extends FileInputFormat
{


    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
    {
        String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        String delim=context.getConfiguration().get("idss_ETL_File_Separator");
        if(delim!=null&&!delim.equals("")){
        	return new LineRecordReader(delim.getBytes(Charsets.UTF_8));
        }
        byte recordDelimiterBytes[] = null;
        if(null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        return new LineRecordReader(recordDelimiterBytes);
    }

    protected boolean isSplitable(JobContext context, Path file)
    {
        CompressionCodec codec = (new CompressionCodecFactory(context.getConfiguration())).getCodec(file);
        if(null == codec)
            return true;
        else
            return codec instanceof SplittableCompressionCodec;
    }
}
