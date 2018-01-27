package com.sq.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author sq
 *1分析具体的业务逻辑逻辑， 确定输入输出数据的样式
 *2.自定义一个类，这个类要继承Mapper，重写map方法，实现具体业务逻辑，讲新的k，v输出
 *3.自定义一个类，这个类要继承Reducer,重写reduce方法，实现具体业务逻辑，讲新的k，v输出
 *将自定义Mapper和Reducer通过Job对象组装起来
 */
public class WordCount {
	static {
	    try {
	    	System.load("f:/hadoop2.7.3/eclipse/workspace/hadoop2.7.3-a/bin/hadoop-common-2.2.0-bin-master/bin/hadoop.dll");
	    } catch (UnsatisfiedLinkError e) {
	      System.err.println("Native code library failed to load./n" + e);
	      System.exit(1);
	    }
	  }

	public static void main(String[] args) throws Exception {
		
		System.setProperty("hadoop.home.dir", "f:/hadoop2.7.3/eclipse/workspace/hadoop2.7.3-a/bin/hadoop-common-2.2.0-bin-master");
		
		//构建Job对象
		Job job=Job.getInstance(new Configuration());
		
		//注意：main方法所在的类
		job.setJarByClass(WordCount.class);
		
		//设置Mapper相关属性
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop1:9000/words.txt"));
		
		//设置Reducer相关属性
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/wcout16-11021"));
		
		//提交任务
		job.waitForCompletion(true);
	}

}
