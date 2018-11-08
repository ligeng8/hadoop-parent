package com.lg.hdfs;
/**
 * 相对那些封装好的方法而言的更底层一些的操作方式
 * 上层那些mapreduce   spark等运算框架，去hdfs中获取数据的时候，就是调的这种底层的api
 * @author
 *
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class StreamAccess {
	private FileSystem fs = null;

	/**
	 * 初始化客户端
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Before
	public void init() throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		fs = FileSystem.get(new URI("hdfs://hdp-node01:9000"), configuration);
	}

	/**
	 * 上传文件
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		FSDataOutputStream outputStream = fs.create(new Path("/angelabady.love"), true);
		FileInputStream inputStream = new FileInputStream("qwqw");
		org.apache.commons.io.IOUtils.copy(inputStream, outputStream);
	}

	/**
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDown() throws IllegalArgumentException, IOException {
		FSDataInputStream inputStream = fs.open(new Path("/angelabady.love"));
		FileOutputStream outputStream = new FileOutputStream(new File("qwqw"));
		IOUtils.copyBytes(inputStream, outputStream, 4096);
	}

	/**
	 * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度 用于上层分布式运算框架并发处理数据
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRandomAccess() throws IllegalArgumentException, IOException {
		// 先获取一个文件的输入流----针对hdfs上的
		FSDataInputStream in = fs.open(new Path("/iloveyou.txt"));

		// 可以将流的起始偏移量进行自定义
		in.seek(22);

		// 再构造一个文件的输出流----针对本地的
		FileOutputStream out = new FileOutputStream(new File("c:/iloveyou.line.2.txt"));

		IOUtils.copyBytes(in, out, 19L, true);

	}

	/**
	 * 测试查看文件内容
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testCat() throws IllegalArgumentException, IOException {
		FSDataInputStream inputStream = fs.open(new Path("/iloveyou.txt"));
		IOUtils.copyBytes(inputStream, System.out, 1024);
	}

	/**
	 * 在mapreduce、spark等运算框架中，有一个核心思想就是将运算移往数据， 或者说，就是要在并发计算中尽可能让运算本地化，
	 * 这就需要获取数据所在位置的信息并进行相应范围读取 以下模拟实现：获取一个文件的所有block位置信息， 然后读取指定block中的内容
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testCats() throws IllegalArgumentException, IOException {
		Path path = new Path("/weblog/input/access.log.10");
		//
		FSDataInputStream inputStream = fs.open(path);
		// 拿到文件信息
		FileStatus[] listStatus = fs.listStatus(path);
//          获取这个文件的所有block的信息
		BlockLocation[] blockLocations = fs.getFileBlockLocations(listStatus[0], 0, listStatus[0].getLen());
		//第一个block的长度
		long length = blockLocations[0].getLength();
		//第一个block的起始偏移量
		long offset = blockLocations[0].getOffset();
		System.out.println(length);
		System.out.println(offset);
		//获取第一个block写入输出流
//		IOUtils.copyBytes(inputStream, System.out, (int) length);
		FileOutputStream outputStream = new FileOutputStream("d:/block0");
		byte[] b = new byte[4096];
		while(inputStream.read(offset, b, 0, 4096) != -1) {
			outputStream.write(b);
			offset += 4096;
			if(offset>=length) return;

		}
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
