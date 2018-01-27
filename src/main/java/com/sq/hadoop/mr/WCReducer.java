package com.sq.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text k2, Iterable<LongWritable> v2s,
			Context context)
			throws IOException, InterruptedException {
		
		//��������
		//Text k3=k2;
		//����һ��������
		long counter=0;
		//ѭ��v2s
		for (LongWritable i : v2s) {
			counter+=i.get();
		}
		//���
		context.write(k2, new LongWritable(counter));
		//super.reduce(k2, v2s, context);
	}

}
