package com.lg.hdfs;
/**
 * �����Щ��װ�õķ������Եĸ��ײ�һЩ�Ĳ�����ʽ
 * �ϲ���Щmapreduce   spark�������ܣ�ȥhdfs�л�ȡ���ݵ�ʱ�򣬾��ǵ������ֵײ��api
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
	 * ��ʼ���ͻ���
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
	 * �ϴ��ļ�
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
	 * hdfs֧�������λ�����ļ���ȡ�����ҿ��Է���ض�ȡָ������ �����ϲ�ֲ�ʽ�����ܲ�����������
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRandomAccess() throws IllegalArgumentException, IOException {
		// �Ȼ�ȡһ���ļ���������----���hdfs�ϵ�
		FSDataInputStream in = fs.open(new Path("/iloveyou.txt"));

		// ���Խ�������ʼƫ���������Զ���
		in.seek(22);

		// �ٹ���һ���ļ��������----��Ա��ص�
		FileOutputStream out = new FileOutputStream(new File("c:/iloveyou.line.2.txt"));

		IOUtils.copyBytes(in, out, 19L, true);

	}

	/**
	 * ���Բ鿴�ļ�����
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
	 * ��mapreduce��spark���������У���һ������˼����ǽ������������ݣ� ����˵������Ҫ�ڲ��������о����������㱾�ػ���
	 * �����Ҫ��ȡ��������λ�õ���Ϣ��������Ӧ��Χ��ȡ ����ģ��ʵ�֣���ȡһ���ļ�������blockλ����Ϣ�� Ȼ���ȡָ��block�е�����
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testCats() throws IllegalArgumentException, IOException {
		Path path = new Path("/weblog/input/access.log.10");
		//
		FSDataInputStream inputStream = fs.open(path);
		// �õ��ļ���Ϣ
		FileStatus[] listStatus = fs.listStatus(path);
//          ��ȡ����ļ�������block����Ϣ
		BlockLocation[] blockLocations = fs.getFileBlockLocations(listStatus[0], 0, listStatus[0].getLen());
		//��һ��block�ĳ���
		long length = blockLocations[0].getLength();
		//��һ��block����ʼƫ����
		long offset = blockLocations[0].getOffset();
		System.out.println(length);
		System.out.println(offset);
		//��ȡ��һ��blockд�������
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
