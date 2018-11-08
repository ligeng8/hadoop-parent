package com.lg.mapperReducerRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lg.mapper.WordCountMapper;
import com.lg.reducer.WorkCountReducer;

public class WordCountRunner {
	// ��ҵ���߼���ص���Ϣ���ĸ���mapper���ĸ���reducer��Ҫ������������������Ľ�������������������������һ��job����
	// ����������õ�job�ύ����Ⱥȥ����
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// ָ�������job���ڵ�jar��
		job.setJarByClass(WordCountRunner.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WorkCountReducer.class);
		// �������ǵ�ҵ���߼�Mapper������key��value����������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// �������ǵ�ҵ���߼�Reducer������key��value����������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// ָ��Ҫ������������ڵ�λ��
		FileInputFormat.setInputPaths(job, "hdfs://server121:9000/wordcount/data/big.txt");
		// ָ���������֮��Ľ���������λ��
		FileOutputFormat.setOutputPath(job, new Path("hdfs://server121:9000/wordcount/output/"));
		//��yarn��Ⱥ�ύ���job
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
