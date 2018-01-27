package com.sq.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		//��������v1
		String line=value.toString();
		//�з�����
		String[] words=line.split("\t");
		//ѭ��
		for (String w : words) {
			//����һ�Σ���һ��һ�����
			context.write(new Text(w), new LongWritable(1));
		}
		// super.map(key, value, context);
	}


}
