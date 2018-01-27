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
 *1���������ҵ���߼��߼��� ȷ������������ݵ���ʽ
 *2.�Զ���һ���࣬�����Ҫ�̳�Mapper����дmap������ʵ�־���ҵ���߼������µ�k��v���
 *3.�Զ���һ���࣬�����Ҫ�̳�Reducer,��дreduce������ʵ�־���ҵ���߼������µ�k��v���
 *���Զ���Mapper��Reducerͨ��Job������װ����
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
		
		//����Job����
		Job job=Job.getInstance(new Configuration());
		
		//ע�⣺main�������ڵ���
		job.setJarByClass(WordCount.class);
		
		//����Mapper�������
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop1:9000/words.txt"));
		
		//����Reducer�������
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/wcout16-11021"));
		
		//�ύ����
		job.waitForCompletion(true);
	}

}
