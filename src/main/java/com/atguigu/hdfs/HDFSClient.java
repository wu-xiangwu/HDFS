package com.atguigu.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HDFSClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://hadoop102:9000");
		//获取hdfs客户端对象
		//FileSystem fs = FileSystem.get(conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//在hdfs上创建路径
		fs.mkdirs(new Path("/0830/dashen/banzhang.txt"));
		//
		//关闭资源
		fs.close();
		
		System.out.println("over");
	}
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		//获取fs对象
		//获取fs对象
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf , "atguigu");
		//执行上传API
		fs.copyFromLocalFile(new Path("d:/0830/2.png"), new Path("/3.png"));
		
		//关闭资源
		fs.close();
	}
	
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		//获取fs对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//执行下载操作
		fs.copyToLocalFile(new Path("/2.png"), new Path("d:/"));
		//关闭资源
		fs.close();
	}
	
	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		//获取fs对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//执行删除操作
		fs.delete(new Path("/3.png"), true);
		//关闭资源
		fs.close();
	}
	
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		//获取fs对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//执行删除操作
		fs.rename(new Path("/wc.input"), new Path("/ww.input"));
		//关闭资源
		fs.close();
		//关闭资源
		
	}
}	
