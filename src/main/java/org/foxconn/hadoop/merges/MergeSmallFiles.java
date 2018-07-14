package org.foxconn.hadoop.merges;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.bind.JAXBElement.GlobalScope;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MergeSmallFiles {
	
	private static FileSystem fs =null;
	private static FileSystem local =null;
	
	private static final String HDFS_SERVER="hdfs://192.168.198.128:9000";
	
	public static void mergerFiles() throws URISyntaxException, IOException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf = new Configuration();
		URI uri = new URI(HDFS_SERVER);
		fs = FileSystem.get(uri, conf);
		local = FileSystem.get(conf);
		FileStatus[] globalStatus  = local.globStatus(new Path("d:/pdata/*"),new RegexUncluderFilter("^.*svn$"));
		for(FileStatus fileStatus:globalStatus) {
			System.out.println(fileStatus.getPath().toString());
		}
		Path[] dirs = FileUtil.stat2Paths(globalStatus);
		FSDataOutputStream out =null;
		FSDataInputStream in =null;
		
		for(Path dir :dirs) {
			String fileName = dir.getName().replaceAll("-", "");
			FileStatus[] globStatus = local.globStatus(new Path(dir+"/*"),new RegexAcceptFilter("^.*txt$"));
			Path hdfFilePath = new Path(HDFS_SERVER+"/dongvip/"+fileName+".txt");
			out=fs.create(hdfFilePath);
			for(FileStatus fileStatus:globStatus) {
				System.out.println(fileStatus.getPath().toString());
				in = local.open(dir);
				IOUtils.copyBytes(in, out, 1024);
			}
			
			
		}
		if(null!=out) {
			out.close();
		}
	}
	
	public static void main(String[] args) {
//		FileSplit
//		InputFormat<K, V>
//		Partitioner<KEY, VALUE>
		try {
			try {
				mergerFiles();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
