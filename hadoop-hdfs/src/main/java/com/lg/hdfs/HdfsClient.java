package com.lg.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient {

	FileSystem fs = null;

	/**
	 * ����FileSystem�ͻ���
	 * 
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		// ����һ�����ò�����������һ������������Ҫ���ʵ�hdfs��URI
		// �Ӷ�FileSystem.get()������֪��Ӧ����ȥ����һ������hdfs�ļ�ϵͳ�Ŀͻ��ˣ��Լ�hdfs�ķ��ʵ�ַ
		// new Configuration();��ʱ�����ͻ�ȥ����jar���е�hadoop-hdfs-2.6.1.jar hdfs-default.xml
		// Ȼ���ټ���classpath�µ�hdfs-site.xml
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://hdp-node01:9000");
		// �������ȼ��� 1���ͻ��˴��������õ�ֵ
		// 2��classpath�µ��û��Զ��������ļ�
		// 3��Ȼ���Ƿ�������Ĭ������
		configuration.set("dfs.replication", "3");
		// ��ȡһ��hdfs�ķ��ʿͻ��ˣ����ݲ��������ʵ��Ӧ����DistributedFileSystem��ʵ��
		fs = FileSystem.get(configuration);

		// �������ȥ��ȡ����conf����Ϳ��Բ�Ҫ��"fs.defaultFS"���������ң�����ͻ��˵���ݱ�ʶ�Ѿ���hadoop�û�
//		fs = FileSystem.get(new URI("hdfs://hdp-node01:9000"), configuration, "hadoop");
	}

	/**
	 * ���б����ļ���hdfs��
	 * 
	 * @throws IOException
	 */
	@Test
	public void moveFromLocalFile() throws IOException {
		Path src = new Path("qwqw");
		Path dst = new Path("/aa/bb");
		fs.moveFromLocalFile(src, dst);
	}

	/**
	 * ����hdfs�ļ��������ļ�ϵͳ
	 * 
	 * @throws IOException
	 */
	@Test
	public void moveToLocalFile() throws IOException {

		Path src = new Path("/aa/bb");
		Path dst = new Path("qwqw");
		fs.moveToLocalFile(src, dst);
	}

	/**
	 * ���Ʊ����ļ���hdfs
	 * 
	 * @throws IOException
	 */
	@Test
	public void copyFromLocalFile() throws IOException {
		Path src = new Path("qwqw");
		Path dst = new Path("/aa/bb");
		fs.copyFromLocalFile(src, dst);
	}
	/**
	 * ���Ʊ����ļ���hdfs
	 * 
	 * @throws IOException
	 */
	@Test
	public void copyFromLocalFile1() throws IOException {
		Path src = new Path("qwqw");
		Path dst = new Path("/aa/bb");
		fs.copyFromLocalFile(src, dst);
	}
	/**
	 * ����hdfs�ļ��������ļ�ϵͳ
	 * 
	 * @throws IOException
	 */
	@Test
	public void copyToLocalFile() throws IOException {
		Path dst = new Path("qwqw");
		Path src = new Path("/aa/bb");
		fs.copyToLocalFile(src, dst);
	}

	/**
	 * ���� ɾ�� ��������Ŀ¼
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {
		// ����Ŀ¼
		Path path = new Path("aa/bb/cc");
		fs.mkdirs(path);
		// ɾ���ļ���
		fs.delete(path, true);
		// �������ļ�
		Path dst;
		fs.rename(path, dst = new Path("/tomcat/sss"));
	}

	/**
	 * �鿴Ŀ¼��Ϣ��ֻ��ʾ�ļ�
	 * 
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			System.out.println(status.getPath().getName());
			System.out.println(status.getBlockSize());
			System.out.println(status.getPermission());
			System.out.println(status.getLen());
			BlockLocation[] locations = status.getBlockLocations();
			for (BlockLocation blockLocation : locations) {
				System.out.println(
						"bk-Length" + blockLocation.getLength() + "--" + "block-offset" + blockLocation.getOffset());
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("#########################################################################");
			System.out.println("#########################################################################");
			System.out.println("#########################################################################");
			System.out.println("#########################################################################");
		}
	}

	/**
	 * �鿴�ļ����ļ�����Ϣ
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {

		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		String flag = "d--             ";
		for (FileStatus fstatus : listStatus) {
			if (fstatus.isFile())
				flag = "f--         ";
			System.out.println(flag + fstatus.getPath().getName());
		}
	}

	@After
	public void after() {
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
