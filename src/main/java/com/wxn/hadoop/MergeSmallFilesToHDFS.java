package com.wxn.hadoop;
/*
 * Created by wxn
 * 2018/11/13 4:24
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 合并小文件到HDFS
 */
public class MergeSmallFilesToHDFS {

	private static FileSystem hdfs = null; //定义HDFS上的文件系统对象
	private static FileSystem local = null; //定义本地文件系统对象

	/**
	 * @function 过滤 regex 格式的文件
	 */
	public static class RegexExcludePathFilter implements PathFilter {

		private final String regex;

		public RegexExcludePathFilter(String regex) {
			// TODO Auto-generated constructor stub
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			boolean flag = path.toString().matches(regex);
			return !flag;
		}

	}

	/**
	 * @function 接受 regex 格式的文件
	 */
	public static class RegexAcceptPathFilter implements PathFilter {

		private final String regex;

		public RegexAcceptPathFilter(String regex) {
			// TODO Auto-generated constructor stub
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			boolean flag = path.toString().matches(regex);
			return flag;
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		list();

	}

	private static void list() throws URISyntaxException, IOException, InterruptedException {

		Configuration conf = new Configuration();//读取Hadoop配置文件

		//设置文件系统访问接口，并创建FileSystem在本地的运行模式
		URI uri = new URI("hdfs://hadoop000:8020");
		hdfs = FileSystem.get(uri, conf, "hadoop");

		local = FileSystem.getLocal(conf);//获取本地文件系统

		//过滤目录下的svn文件
//		FileStatus[] dirstatus = local.globStatus(new Path("D://Code/EclipseCode/mergeSmallFilesTestData/*"),
//				new RegexExcludePathFilter("^.*svn$"));
		//获取Country目录下的文件
		FileStatus[] dirstatus = local.globStatus(new Path("C:\\Users\\WuXinnan\\Desktop\\Country\\*"));

		//获取目录下的所有文件路径
		Path[] dirs = FileUtil.stat2Paths(dirstatus);
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		for (Path dir : dirs) {

			//获取文件名
			String fileName = dir.getName();


			//只接受日期目录下的.txt文件
			FileStatus[] localStatus = local.globStatus(new Path(dir + "/*"),
					new RegexAcceptPathFilter("^.*txt$"));

			// 获得目录下的所有文件
			Path[] listPath = FileUtil.stat2Paths(localStatus);

			// 输出路径
			Path outBlock = new Path("hdfs://hadoop000:8020/mergeSmallFiles/Country/" + fileName + "/" + fileName + ".txt");
			System.out.println("合并后的文件名称：" + fileName + ".txt");

			// 打开输出流
			out = hdfs.create(outBlock);

			//循环操作目录下的所有文件
			for (Path p : listPath) {
				in = local.open(p);// 打开输入流
				IOUtils.copyBytes(in, out, 4096, false);// 复制数据
				in.close();// 关闭输入流
			}

			if (out != null) {
				out.close();// 关闭输出流
			}
		}

	}

}