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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 计算class的数量
 */
public class ClassCount {

	/**
	 * Map: 读取输入的文件
	 */
	public static class MyMapper extends Mapper<Text, Text,Text,LongWritable>{
		LongWritable one = new LongWritable(1);
		private static Logger logger = LoggerFactory.getLogger(MyMapper.class);


		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			logger.info("=============log===========");
			logger.info("key:" + key);

			Path path = ((FileSplit) context.getInputSplit()).getPath();

			String fileName = path.getParent().getName();

			context.write(new Text(fileName) , one);
		}


	}


	/**
	 * 归并操作
	 */
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			long sum = 0;
			for (LongWritable value : values){
				sum+=value.get();
			}
			context.write(key,new LongWritable(sum));

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
		if (fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath,true);
			System.out.println("output file exist, but it has deleted");
		}
		//创建Job
		Job job = Job.getInstance(configuration, "classcount");
		//设置job的处理类
		job.setJarByClass(ClassCount.class);
		//设置作业处理的输入路径
		Path[] paths = getChildPaths(inputPath, fileSystem);
		job.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.setInputPaths(job, paths);
		//设置map相关参数
		job.setMapperClass(ClassCount.MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		//设置reduce相关参数
		job.setReducerClass(ClassCount.MyReducer.class);
		job.setOutputKeyClass(Text.class);
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

	/**
	 * 一次读取整个文件而非按行读取
	 */
	public static class WholeFileInputFormat extends FileInputFormat<Text,Text>{

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			RecordReader<Text,Text> recordReader = new WholeFileRecordReader();
			return recordReader;
		}

	}



	public static class WholeFileRecordReader extends RecordReader<Text,Text>{

		private FileSplit fileSplit;
		private JobContext jobContext;
		private Text currentKey = new Text();
		private Text currentValue = new Text();
		private boolean finishConverting = false;
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			float progress = 0;
			if(finishConverting){
				progress = 1;
			}
			return progress;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) arg0;
			this.jobContext = arg1;
			String filename = fileSplit.getPath().getName();
			this.currentKey = new Text(filename);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(!finishConverting){
				int len = (int)fileSplit.getLength();
//          byte[] content = new byte[len];
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
				FSDataInputStream in = fs.open(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
				String line="";
				String total="";
				while((line= br.readLine())!= null){
					total =total+line+"\n";
				}
				br.close();
				in.close();
				fs.close();
				currentValue = new Text(total);
				finishConverting = true;
				return true;
			}
			return false;
		}

	}








}
