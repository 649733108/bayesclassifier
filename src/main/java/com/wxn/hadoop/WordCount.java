package com.wxn.hadoop;
/*
 * Created by wxn
 * 2018/11/11 19:48
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 计算class的数量
 */
public class WordCount {

	/**
	 * Map: 读取输入的文件
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, MyMapWritable, LongWritable> {
		LongWritable one = new LongWritable(1);
		private static Logger logger = LoggerFactory.getLogger(MyMapper.class);


		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			logger.info("=============map start===========");


			Path path = ((FileSplit) context.getInputSplit()).getPath();

			String fileName = path.getParent().getName();

			MyMapWritable myMapWritable = new MyMapWritable();
			myMapWritable.put(new Text(fileName),value);

			context.write(myMapWritable, one);

			logger.info("=============map end===========");

		}


	}


	/**
	 * 归并操作
	 */
	public static class MyReducer extends Reducer<MyMapWritable, LongWritable, MyMapWritable, LongWritable> {

		@Override
		protected void reduce(MyMapWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));

		}
	}

	/**
	 * 定义Driver:封装了MapReduce作业的所有信息
	 */
	public static void main(String args[]) throws Exception {


		//创建Configuration
		Configuration configuration = new Configuration();
		//准备清理已存在的输出目录
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
//		URI uri = new URI("hdfs://192.168.26.128:8020");
//		String user = "hadoop";
		FileSystem fileSystem = FileSystem.get(configuration);
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
			System.out.println("output file exist, but it has deleted");
		}
		//创建Job
		Job job = Job.getInstance(configuration, "classcount");
		//设置job的处理类
		job.setJarByClass(WordCount.class);
		//设置作业处理的输入路径
		Path[] paths = getChildPaths(inputPath, fileSystem);
		FileInputFormat.setInputPaths(job, paths);
		//设置map相关参数
		job.setMapperClass(WordCount.MyMapper.class);
		job.setMapOutputKeyClass(MyMapWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		//设置reduce相关参数
		job.setReducerClass(WordCount.MyReducer.class);
		job.setOutputKeyClass(MyMapWritable.class);
		job.setOutputValueClass(LongWritable.class);

		//设置作业处理的输出路径
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	/**
	 * 获取目录下所有文件的路径
	 */
	private static Path[] getChildPaths(Path inputPath, FileSystem fileSystem) throws IOException {
		FileStatus[] fileStatuses = fileSystem.listStatus(inputPath);
		List<Path> pathList = new ArrayList<>();
		for (FileStatus fileStatus : fileStatuses) {
			Path inFile = new Path(fileStatus.getPath().toString());
			pathList.add(inFile);
		}
		Path[] paths = new Path[pathList.size()];
		for (int i = 0; i < pathList.size(); i++) {
			paths[i] = pathList.get(i);
		}
		return paths;
	}


}
