package com.sq.exercise;

/**
 * Created by sq on 2018/1/27.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * MapReduce实现Join操作 
 */
public class MapRedJoin {
    public static final String DELIMITER = "\u0009"; // 字段分隔符  

    // map过程  
    public static class MapClass extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        public void configure(JobConf job) {
            super.configure(job);
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException, ClassCastException {
            // 获取输入文件的全路径和名称  
            String filePath = ((FileSplit) reporter.getInputSplit()).getPath().toString();
            // 获取记录字符串  
            String line = value.toString();
            // 抛弃空记录  
            if (line == null || line.equals("")) return;

            // 处理来自表A的记录  
            if (filePath.contains("m_ys_lab_jointest_a")) {
                String[] values = line.split(DELIMITER); // 按分隔符分割出字段  
                if (values.length < 2) return;

                String id = values[0]; // id  
                String name = values[1]; // name  

                output.collect(new Text(id), new Text("a#" + name));
            }
            // 处理来自表B的记录  
            else if (filePath.contains("m_ys_lab_jointest_b")) {
                String[] values = line.split(DELIMITER); // 按分隔符分割出字段  
                if (values.length < 3) return;

                String id = values[0]; // id  
                String statyear = values[1]; // statyear  
                String num = values[2]; //num  

                output.collect(new Text(id), new Text("b#" + statyear + DELIMITER + num));
            }
        }
    }}
