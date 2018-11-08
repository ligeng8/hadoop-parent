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
	// 把业务逻辑相关的信息（哪个是mapper，哪个是reducer，要处理的数据在哪里，输出的结果放哪里。。。。。。）描述成一个job对象
	// 把这个描述好的job提交给集群去运行
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 指定我这个job所在的jar包
		job.setJarByClass(WordCountRunner.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WorkCountReducer.class);
		// 设置我们的业务逻辑Mapper类的输出key和value的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 设置我们的业务逻辑Reducer类的输出key和value的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 指定要处理的数据所在的位置
		FileInputFormat.setInputPaths(job, "hdfs://server121:9000/wordcount/data/big.txt");
		// 指定处理完成之后的结果所保存的位置
		FileOutputFormat.setOutputPath(job, new Path("hdfs://server121:9000/wordcount/output/"));
		//向yarn集群提交这个job
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
