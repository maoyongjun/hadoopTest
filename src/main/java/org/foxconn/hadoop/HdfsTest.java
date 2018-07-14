package org.foxconn.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class HdfsTest {
	
	Logger logger = Logger.getLogger(HdfsTest.class);
	private Set<String> set = new HashSet<String>();
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		HdfsTest test = new HdfsTest();
//		test.test1();
//		test.test2();
//		test.test3();
//		test.test4();
		test.test5();
	}
	
	public void test5(){
		  try {
	            Configuration conf = new Configuration();

	            // 不设置该代码会出现错误：java.io.IOException: No FileSystem for scheme: hdfs
	            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

	            String filePath = "hdfs://192.168.146.158:9000/wordcount/srcdata/";
	            Path path = new Path(filePath);

	            // 这里需要设置URI，否则出现错误：java.lang.IllegalArgumentException: Wrong FS: hdfs://127.0.0.1:9000/test/test.txt, expected: file:///
	            FileSystem fs = FileSystem.get(new URI(filePath), conf);

	            System.out.println( "READING ============================" );
	            FSDataInputStream is = fs.open(path);
	            BufferedReader br = new BufferedReader(new InputStreamReader(is));
	            // 示例仅读取一行
	            String content = br.readLine();
	            System.out.println(content);
	            br.close();


	            System.out.println("WRITING ============================");
	            byte[] buff = "this is helloworld from java api!\n".getBytes();
	            FSDataOutputStream os = fs.create(path);
	            os.write(buff, 0, buff.length);
	            os.close();
	            fs.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		
	}
	
	public  void test1() throws Exception {
		// 1）构建Configuration对象，读取并解析相关配置文件
		Configuration conf = new Configuration();
		// 2）设置相关属性
//		conf.set("fs.defaultFS", "hdfs://192.168.146.158:9000");
//		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//		 conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		// 3）获取特定文件系统实例fs（以HDFS文件系统实例）
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.146.158:9000"), conf, "hdfs");
		// 4）通过文件系统实例fs进行文件操作(以删除文件实例)
		fs.delete(new Path("/user/liuhl/someWords.txt"));
	}
	
	public void test2() throws Exception {
		FileSystem fileSystem=getHadoopFileSystem();
		FSDataInputStream in=null;
		in=fileSystem.open(new Path("file:///a.txt"));
//		FileStatus fileStatus=fileSystem.getFileStatus(new Path(uri));
//		byte[] buffer=new byte[1024];
//		in.read(4096, buffer, 0, 1024);
		IOUtils.copyBytes(in, System.out, 4096, false);
		IOUtils.closeStream(in);
	}
	
	
	public void test3() throws IOException{
		FileSystem fileSystem=getHadoopFileSystem();
		 Path path = new Path("test5");
		 boolean b = false;
	     b =fileSystem.mkdirs(path);
	     System.out.println(b);
		
	}
	public void test4() throws IOException{
		FileSystem fileSystem=getHadoopFileSystem();
		 recursiveHdfsPath(fileSystem,new Path("hdfs://192.168.146.158:9000/"));
		 System.out.println(set);
	}
	public Set<String> recursiveHdfsPath(FileSystem hdfs,Path listPath){
		 FileStatus[] files = null;
         try {
             files = hdfs.listStatus(listPath);
             System.out.println(files);
             // 实际上并不是每个文件夹都会有文件的。
             if(files.length == 0){
                 // 如果不使用toUri()，获取的路径带URL。
                 set.add(listPath.toUri().getPath());
             }else {
                 // 判断是否为文件
                 for (FileStatus f : files) {
                     if (files.length == 0 || f.isFile()) {
                         set.add(f.getPath().toUri().getPath());
                     } else {
                    	 if(set.size()>40){
                    		 return set;
                    	 }
                         // 是文件夹，且非空，就继续遍历
                         recursiveHdfsPath(hdfs, f.getPath());
                     }
                 }
             }
         } catch (IOException e) {
             e.printStackTrace();
             logger.error(e);
         }
         return set;
		
	}
	
	 public FileSystem getHadoopFileSystem() throws IOException {
         
         
         FileSystem fs = null;
         Configuration conf = null;
 
         // 方法一，本地有配置文件，直接获取配置文件（core-site.xml，hdfs-site.xml）
         // 根据配置文件创建HDFS对象
         // 此时必须指定hdsf的访问路径。
//         conf = new Configuration();
//         // 文件系统为必须设置的内容。其他配置参数可以自行设置，且优先级最高
//         conf.set("fs.defaultFS", "hdfs://192.168.146.158:9000");
// 
//         try {
//             // 根据配置文件创建HDFS对象
//             fs = FileSystem.get(conf);
//         } catch (IOException e) {
//             e.printStackTrace();
//             logger.error("",e);
//         }
 
         // 方法二：本地没有hadoop系统，但是可以远程访问。根据给定的URI和用户名，访问hdfs的配置参数
         // 此时的conf不需任何设置，只需读取远程的配置文件即可。
//         conf = new Configuration();
//         // Hadoop的用户名
//         String hdfsUserName = "root";
// 
//         URI hdfsUri = null;
//         try {
//             // HDFS的访问路径
//             hdfsUri = new URI("hdfs://192.168.146.158:9000");
//         } catch (URISyntaxException e) {
//             e.printStackTrace();
//             logger.error(e);
//         }
// 
//         try {
//             // 根据远程的NN节点，获取配置信息，创建HDFS对象
//             fs = FileSystem.get(hdfsUri,conf,hdfsUserName);
//         } catch (IOException e) {
//             e.printStackTrace();
//             logger.error(e);
//         } catch (InterruptedException e) {
//             e.printStackTrace();
//             logger.error(e);
//         }
 
         // 方法三，反正我们没有搞懂。
         //下面三行如果不加则自动读取项目路径下的配置文件
//         conf = new Configuration();
//         conf.addResource(new Path("c://conf/core-site.xml"));
//         conf.addResource(new Path("c://conf/hdfs-site.xml"));
//         conf.addResource(new Path("c://conf/mapred-site.xml"));
//
//         fs = FileSystem.get(conf);
 
         
         conf = new Configuration();
         conf.set("fs.defaultFS", "hdfs://192.168.146.158:9000");
         conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
         conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
         fs = FileSystem.get(conf);
         
         return fs;
     } 
	
}
