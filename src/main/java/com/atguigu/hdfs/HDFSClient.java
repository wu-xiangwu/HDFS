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
		//��ȡhdfs�ͻ��˶���
		//FileSystem fs = FileSystem.get(conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//��hdfs�ϴ���·��
		fs.mkdirs(new Path("/0830/dashen/banzhang.txt"));
		
		//�ر���Դ
		fs.close();
		
		System.out.println("over");
	}
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		//��ȡfs����
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf , "atguigu");
		//ִ���ϴ�API
		fs.copyFromLocalFile(new Path("d:/0830/2.png"), new Path("/3.png"));
		
		//�ر���Դ
		fs.close();
	}
	
	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		//��ȡfs����
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//ִ�����ز���
		fs.copyToLocalFile(new Path("/2.png"), new Path("d:/"));
		//�ر���Դ
		fs.close();
	}
	
	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		//��ȡfs����
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//ִ��ɾ������
		fs.delete(new Path("/3.png"), true);
		//�ر���Դ
		fs.close();
	}
	
	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		//��ȡfs����
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
		
		//ִ��ɾ������
		fs.rename(new Path("/wc.input"), new Path("/ww.input"));
		//�ر���Դ
		fs.close();
	}
}	
