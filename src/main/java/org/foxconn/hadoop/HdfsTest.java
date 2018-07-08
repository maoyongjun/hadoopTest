package org.foxconn.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.out.println(System.getenv("HADOOP_HOME"));
			System.setProperty("HADOOP_USER_NAME", "hadoop");
//			downFromHdfs() ;
//			uploadFileToHdfs() ;
//			mkdirToHdfs() ;
//			createFile() ;
//			renameFileOrDir() ;
			listDir();
//			delFile() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 涓嬭浇鏂囦欢
	public static void downFromHdfs() throws Exception {
		String path = "hdfs://192.168.198.128:9000";
		URI uri = new URI(path);
		FileSystem fs = FileSystem.get(uri, new Configuration());
		// Hadoop鏂囦欢绯荤粺涓�氳繃Hadoop Path瀵硅薄鏉ヤ唬琛ㄤ竴涓枃浠�
		Path src = new Path("/tfiles/a.txt");
		FSDataInputStream in = fs.open(src);

		File targetFile = new File("d://aa.txt");
		FileOutputStream out = new FileOutputStream(targetFile);
		// IOUtils鏄疕adoop鑷繁鎻愪緵鐨勫伐鍏风被锛屽湪缂栫▼鐨勮繃绋嬩腑鐢ㄧ殑闈炲父鏂逛究
		// 鏈�鍚庨偅涓弬鏁板氨鏄槸鍚︿娇鐢ㄥ畬鍏抽棴鐨勬剰鎬�
		IOUtils.copyBytes(in, out, 4096, true);
		System.out.println("=========鏂囦欢涓嬭浇鎴愬姛=========");
	}

	// 2锛氫笂浼犳枃浠�
	public static void uploadFileToHdfs() throws Exception {
		// 閽堝杩欑鏉冮檺闂锛屾湁闆嗕腑瑙ｅ喅鏂规锛岃繖鏄竴绉嶏紝杩樺彲浠ラ厤缃甴dfs鐨剎ml鏂囦欢鏉ヨВ鍐�
		// System.setProperty("HADOOP_USER_NAME","hadoop") ;
		// FileSystem鏄竴涓娊璞＄被锛屾垜浠彲浠ラ�氳繃鏌ョ湅婧愮爜鏉ヤ簡瑙�
		String path = "hdfs://192.168.198.128:9000";
		URI uri = new URI(path);// 鍒涘缓URI瀵硅薄
		FileSystem fs = FileSystem.get(uri, new Configuration());// 鑾峰彇鏂囦欢绯荤粺
		// 鍒涘缓婧愬湴鍧�
		Path src = new Path("d://aa.txt");
		// 鍒涘缓鐩爣鍦板潃
		Path dst = new Path("/");
		// 璋冪敤鏂囦欢绯荤粺鐨勫鍒跺嚱鏁帮紝鍓嶉潰鐨勫弬鏁版槸鎸囨槸鍚﹀垹闄ゆ簮鏂囦欢锛宼rue涓哄垹闄わ紝鍚﹀垯涓嶅垹闄�
		fs.copyFromLocalFile(false, src, dst);
		// 鏈�鍚庡叧闂枃浠剁郴缁�
		System.out.println("=========鏂囦欢涓婁紶鎴愬姛==========");
		fs.close();// 褰撶劧杩欓噷鎴戜滑鍦ㄦ寮忎功鍐欎唬鐮佺殑鏃跺�欓渶瑕佽繘琛屼慨鏀癸紝鍦╢inally鍧椾腑鍏抽棴
	}

	// 3锛氬垱寤烘枃浠跺す
	public static void mkdirToHdfs() {

		String path = "hdfs://192.168.198.128:9000";
		URI uri = null;
		FileSystem fs = null;
		try {
			// 鍒涘缓URI瀵硅薄
			uri = new URI(path);
			fs = FileSystem.get(uri, new Configuration());// 鑾峰彇鏂囦欢绯荤粺
			Path dirPath = new Path("/mktest");
			fs.mkdirs(dirPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("==========鍒涘缓鐩綍鎴愬姛=========");
	}

	// 4锛氬垱寤烘枃浠�
	public static void createFile() {

		String path = "hdfs://192.168.198.128:9000";
		// 鍒涘缓URI瀵硅薄
		URI uri = null;
		FileSystem fs = null;
		FSDataOutputStream out = null;
		try {
			uri = new URI(path);
			fs = FileSystem.get(uri, new Configuration());// 鑾峰彇鏂囦欢绯荤粺
			Path dst = new Path("/mktest/aa.txt");// 瑕佸垱寤虹殑鏂囦欢鐨勮矾寰�
			byte[] content = "鎴戠埍浣犱滑".getBytes();
			// 鍒涘缓鏂囦欢
			out = fs.create(dst);
			// 鍐欐暟鎹�
			out.write(content);
			System.out.println("=======鏂囦欢鍒涘缓鎴愬姛========");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// 鍏抽棴娴�
				out.close();
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	// 5锛氭枃浠堕噸鍛藉悕
	public static void renameFileOrDir() {

		String path = "hdfs://192.168.198.128:9000";
		// 鍒涘缓URI瀵硅薄
		URI uri = null;
		FileSystem fs = null;

		// 鏃ф枃浠跺悕绉扮殑path
//			Path oldName = new Path("/mktest/aa.txt") ;
//			Path newName = new Path("/mktest/bb") ;		
		Path oldName = new Path("/mktest");
		Path newName = new Path("/mktest2");
		try {
			uri = new URI(path);
			fs = FileSystem.get(uri, new Configuration());// 鑾峰彇鏂囦欢绯荤粺
			fs.rename(oldName, newName);
			System.out.println("=========閲嶅懡鍚嶆垚鍔�========");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// 閬嶅巻鏂囦欢绯荤粺鐨勬煇涓洰褰�
	public static void listDir() {

		String path = "hdfs://192.168.198.128:9000";
		// 鍒涘缓URI瀵硅薄
		URI uri = null;
		FileSystem fs = null;
		try {
			uri = new URI(path);
			Configuration cfg = new Configuration();
			cfg.set("fs.defaultFS", "hdfs://192.168.198.128:9000");
			cfg.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
			cfg.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			System.out.println("cfg:"+cfg);
			fs = FileSystem.get(uri, cfg);
			System.out.println("fs:"+fs);
			// 杈撳叆瑕侀亶鍘嗙殑鐩綍璺緞
			Path dst = new Path("/");
			// 璋冪敤listStatus()鏂规硶鑾峰彇涓�涓枃浠舵暟缁�
			// FileStatus瀵硅薄灏佽浜嗘枃浠剁殑鍜岀洰褰曠殑鍏冩暟鎹紝鍖呮嫭鏂囦欢闀垮害銆佸潡澶у皬銆佹潈闄愮瓑淇℃伅
			FileStatus[] liststatus = fs.listStatus(dst);
			for (FileStatus ft : liststatus) {
				// 鍒ゆ柇鏄惁鏄洰褰�
				String isDir = ft.isDirectory() ? "鏂囦欢澶�" : "鏂囦欢";
				// 鑾峰彇鏂囦欢鐨勬潈闄�
				String permission = ft.getPermission().toString();
				// 鑾峰彇澶囦唤鍧�
				short replication = ft.getReplication();
				// 鑾峰彇鏁扮粍鐨勯暱搴�
				long len = ft.getLen();
				// 鑾峰彇鏂囦欢鐨勮矾寰�
				String filePath = ft.getPath().toString();
				System.out.println("鏂囦欢淇℃伅锛�");
				System.out.println("鏄惁鏄洰褰曪紵 " + isDir);
				System.out.println("鏂囦欢鏉冮檺 " + permission);
				System.out.println("澶囦唤鍧� " + replication);
				System.out.println("鏂囦欢闀垮害  " + len);
				System.out.println("鏂囦欢璺姴  " + filePath);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	// 鍒犻櫎鏂囦欢
	public static void delFile() {

		String path = "hdfs://192.168.198.128:9000";
		// 鍒涘缓URI瀵硅薄
		URI uri = null;
		FileSystem fs = null;
		try {
			uri = new URI(path);
			fs = FileSystem.get(uri, new Configuration());
//				Path dst = new Path("/job.txt") ;
			Path dst = new Path("/mktest2");

			// 姘镐箙鎬у垹闄ゆ寚瀹氱殑鏂囦欢鎴栫洰褰曪紝濡傛灉鐩爣鏄竴涓┖鐩綍鎴栬�呮枃浠讹紝閭ｄ箞recursive鐨勫�煎氨浼氳蹇界暐銆�
			// 鍙湁recursive锛漷rue鏃讹紝涓�涓潪绌虹洰褰曞強鍏跺唴瀹规墠浼氳鍒犻櫎
			boolean flag = fs.delete(dst, true);
			if (flag) {
				System.out.println("==========鍒犻櫎鎴愬姛=========");
			} else {
				System.out.println("==========鍒犻櫎澶辫触=========");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
