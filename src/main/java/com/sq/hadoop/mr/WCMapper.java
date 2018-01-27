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
		
		//接受数据v1
		String line=value.toString();
		//切分数据
		String[] words=line.split("\t");
		//循环
		for (String w : words) {
			//出现一次，记一个一，输出
			context.write(new Text(w), new LongWritable(1));
		}
		// super.map(key, value, context);
	}


}
