package com.lg.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// �õ�һ������ת��Ϊstring
		String line = value.toString();
		// ����һ���зֳ���������
		String[] words = line.split(" ");
		// �������飬���<���ʣ�1>
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}

	}

}
