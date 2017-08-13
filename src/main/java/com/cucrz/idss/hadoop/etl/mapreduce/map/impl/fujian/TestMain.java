package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.fujian;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cucrz.idss.hadoop.etl.mapreduce.in.ETLInputFormat;

public class TestMain {
	 
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
 
        public void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            System.out.println("key:\t " + key);
            System.out.println("value:\t " + value);
            System.out.println("-------------------------");
        }
    }
 
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
         Path outPath = new Path("/hive/11");
         FileSystem.get(conf).delete(outPath, true);
        Job job = new Job(conf, "TestMyInputFormat");
        job.setInputFormatClass(ETLInputFormat.class);
        job.setJarByClass(TestMain.class);
        job.setMapperClass(TestMain.MapperClass.class);
       // job.setJarByClass(DataEtlJob.class);
		//job.setMapperClass(MapperClass.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path("/cucrz/data/1301/20150512"));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outPath);
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}