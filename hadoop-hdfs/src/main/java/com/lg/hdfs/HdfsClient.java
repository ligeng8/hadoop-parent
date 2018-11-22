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
	 * 连接FileSystem客户端
	 * 
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		// 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
		// 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
		// new Configuration();的时候，它就会去加载jar包中的hadoop-hdfs-2.6.1.jar hdfs-default.xml
		// 然后再加载classpath下的hdfs-site.xml
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://hdp-node01:9000");
		// 参数优先级： 1、客户端代码中设置的值
		// 2、classpath下的用户自定义配置文件
		// 3、然后是服务器的默认配置
		configuration.set("dfs.replication", "3");
		// 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
		fs = FileSystem.get(configuration);

		// 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
//		fs = FileSystem.get(new URI("hdfs://hdp-node01:9000"), configuration, "hadoop");
	}

	/**
	 * 剪切本地文件到hdfs上
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
	 * 剪切hdfs文件到本地文件系统
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
	 * 复制本地文件到hdfs
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
	 * 复制本地文件到hdfs
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
	 * 复制hdfs文件到本地文件系统
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
	 * 创建 删除 和重名令目录
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {
		// 创建目录
		Path path = new Path("aa/bb/cc");
		fs.mkdirs(path);
		// 删除文件夹
		fs.delete(path, true);
		// 重名名文件
		Path dst;
		fs.rename(path, dst = new Path("/tomcat/sss"));
	}

	/**
	 * 查看目录信息，只显示文件
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
	 * 查看文件及文件夹信息
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
