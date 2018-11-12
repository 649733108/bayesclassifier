package com.wxn.hadoop;
/*
 * Created by wxn
 * 2018/11/11 19:48
 */

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 预测类
 */
public class Prediction {

	//先验概率
	private static Map<String, Double> pCi ;
	//条件概率
	private static Map<Map<String, String>, Double> pTC;


	/**
	 * Map: 读取输入的文件
	 */
	public static class MyMapper extends Mapper<Text, Text, Text, MapWritable> {
		LongWritable one = new LongWritable(1);
		private Logger logger = LoggerFactory.getLogger(MyMapper.class);
		private Configuration configuration;
		private FileSystem fileSystem;
		private Map<String, Double> pCi ;
		private Map<Map<String, String>, Double> pTC;
		private Map<String, Long> classMap;
		private Map<Map<String, String>, Long> wordMap;



		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			configuration = new Configuration();
			fileSystem = FileSystem.get(configuration);
			classMap = getClassMap(fileSystem);
			wordMap = getWordMap(fileSystem);
			pCi = getPCi(classMap);
			pTC = getpTC(wordMap);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			fileSystem.close();
		}

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			logger.info("=============log===========");
			logger.info("key : " + key);
			logger.info("value : " + value);

			logger.info("pCi : " + pCi);


			for (String className : pCi.keySet()) {
				//条件概率
				double conditionalProbability = conditionalProbabilityForClass(value.toString(), className,pCi,pTC);
				MapWritable mapWritable = new MapWritable();
				mapWritable.put(new Text(className), new DoubleWritable(conditionalProbability));
				logger.info("mapWritable : "+mapWritable.toString());
				context.write(key, mapWritable);
			}

		}


	}


	/**
	 * 归并操作
	 */
	public static class MyReducer extends Reducer<Text, MapWritable, Text, Text> {

		private static Logger logger = LoggerFactory.getLogger(MyReducer.class);

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			logger.info("==============reduce=============");
			logger.info("key : " + key.toString());
			logger.info("values : " + values.toString());
			String className="";
			double maxProb = -99999.0;
			for (MapWritable value : values) {
				logger.info("value : " + value);
				for (Writable writable : value.keySet()) {
					double tempProb = Double.parseDouble(value.get(writable).toString());
					logger.info("wirtable :" + writable.toString());
					logger.info("maxProb :" + maxProb);
					logger.info("tempProb :" + tempProb);
					if (tempProb > maxProb) {
						maxProb = tempProb;
						logger.info("new MaxProb:" + maxProb);
						className = writable.toString();
						logger.info("new ClassName : " + className);
					}
				}
			}
			logger.info("final ClassName : " + className);
			context.write(key, new Text(className));

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

		FileSystem fileSystem = FileSystem.get(configuration);
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
			System.out.println("output file exist, but it has deleted");
		}

		//读取classcount文件 并写入classMap中
		Map<String, Long> classMap = getClassMap(fileSystem);

		//读取wordcount文件并写入wordMap中
		Map<Map<String, String>, Long> wordMap = getWordMap(fileSystem);

		System.out.println("===============计算概率===============");
		//计算先验概率PCi
		pCi = getPCi(classMap);
		System.out.println("pCi size : " + pCi.size());
		for (String s : pCi.keySet()) {
			System.out.println("class name : " + s);
		}
		//计算条件概率pTC
		pTC = getpTC(wordMap);

//		System.out.println("=========概率=========");
//		System.out.println(conditionalProbabilityForClass("a\nb\nec", "UK"));


		//创建Job
		Job job = Job.getInstance(configuration, "prediction");
		//设置job的处理类
		job.setJarByClass(Prediction.class);
		//设置作业处理的输入路径
//		Path[] paths = getChildPaths(inputPath, fileSystem);
		job.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.setInputPaths(job, inputPath);
		//设置map相关参数
		job.setMapperClass(Prediction.MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		//设置reduce相关参数
		job.setReducerClass(Prediction.MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//设置作业处理的输出路径
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	private static Map<Map<String, String>, Long> getWordMap(FileSystem fileSystem) throws IOException {
		FSDataInputStream inputStream;
		BufferedReader bufferedReader;
		String line;
		inputStream = fileSystem.open(new Path("/wordcount/part-r-00000"));
		String wordcount = IOUtils.toString(inputStream);
		bufferedReader = new BufferedReader(new StringReader(wordcount));
		line = "";
		Map<Map<String, String>, Long> wordMap = new HashMap<>();
		while (line != null) {
			line = bufferedReader.readLine();
			if (StringUtils.isNotBlank(line)) {
				String[] words = line.split("\t");
				Map<String, String> keyMap = new HashMap<>();
				keyMap.put(words[0], words[1]);
				wordMap.put(keyMap, Long.parseLong(words[2]));
			}
		}
		return wordMap;
	}

	/**
	 * 获取classMap
	 */
	private static Map<String, Long> getClassMap(FileSystem fileSystem) throws IOException {
		FSDataInputStream inputStream = fileSystem.open(new Path("/classcount/part-r-00000"));
		String classcount = IOUtils.toString(inputStream);
		BufferedReader bufferedReader = new BufferedReader(new StringReader(classcount));
		String line = "";
		Map<String, Long> classMap = new HashMap<>();
		while (line != null) {
			line = bufferedReader.readLine();
			if (StringUtils.isNotBlank(line)) {
				String[] words = line.split("\t");
				classMap.put(words[0], Long.parseLong(words[1]));
			}
	}
		return classMap;
	}

	/**
	 * 计算一个文档属于某类的条件概率
	 */
	private static double conditionalProbabilityForClass(String content, String className,Map<String,Double> pCi, Map<Map<String, String>, Double> pTC) throws IOException {

		BufferedReader bufferedReader = new BufferedReader(new StringReader(content));
		List<String> words = new ArrayList<>();
		String line = "";
		while (line != null) {
			line = bufferedReader.readLine();
			if (StringUtils.isNotBlank(line)) {
				words.add(line);
			}
		}
		double p = Math.log10(pCi.get(className));
		for (String word : words) {
			Map<String, String> map = new HashMap<>();
			map.put(className, word);
			if (pTC.get(map) == null) {
				map.put(className, null);
			}
			p += Math.log10(pTC.get(map));
		}
		return p;
	}

	/**
	 * 计算先验概率
	 */
	private static Map<String, Double> getPCi(Map<String, Long> classMap) {

		Iterator iterator = classMap.keySet().iterator();
		//计算总文档个数
		Long docTotal = 0L;
		while (iterator.hasNext()) {
			docTotal += classMap.get(iterator.next());
		}
		iterator = classMap.keySet().iterator();
		Map<String, Double> pCiMap = new HashMap<>();
		while (iterator.hasNext()) {
			String key = (String) iterator.next();
			//计算概率
			pCiMap.put(key, classMap.get(key) * 1.0 / docTotal);
		}
		return pCiMap;
	}

	/**
	 * 计算条件概率
	 */
	private static Map<Map<String, String>, Double> getpTC(Map<Map<String, String>, Long> wordclass) {
		//条件概率
		Map<Map<String, String>, Double> pTCMap = new HashMap<>();
		//单词集合大小
		Map<String, Integer> wordSetMap = new HashMap<>();
		//单词总数
		Map<String, Long> totalWordMap = new HashMap<>();

		for (Map<String, String> classWordMap : wordclass.keySet()) {
			for (String className : classWordMap.keySet()) {
				if (wordSetMap.get(className) == null) {
					wordSetMap.put(className, 1);
				} else {
					wordSetMap.put(className, wordSetMap.get(className) + 1);
				}
				if (totalWordMap.get(className) == null) {
					totalWordMap.put(className, wordclass.get(classWordMap));
				} else {
					totalWordMap.put(className, totalWordMap.get(className) + wordclass.get(classWordMap));
				}
			}
		}

		for (Map<String, String> classWordMap : wordclass.keySet()) {
			for (String className : classWordMap.keySet()) {
				//单词t在C中出现的次数
				Long t_in_c = wordclass.get(classWordMap);
				pTCMap.put(classWordMap, (t_in_c + 1) * 1.0 / (totalWordMap.get(className) + wordSetMap.get(className)));
				Map<String, String> notExistMap = new HashMap<>();
				notExistMap.put(className, null);
				pTCMap.put(notExistMap, 1.0 / (totalWordMap.get(className) + wordSetMap.get(className)));
			}
		}
		return pTCMap;

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
	public static class WholeFileInputFormat extends FileInputFormat<Text, Text> {

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) {
			// TODO Auto-generated method stub
			RecordReader<Text, Text> recordReader = new WholeFileRecordReader();
			return recordReader;
		}

	}

	public static class WholeFileRecordReader extends RecordReader<Text, Text> {

		private FileSplit fileSplit;
		private JobContext jobContext;
		private Text currentKey = new Text();
		private Text currentValue = new Text();
		private boolean finishConverting = false;

		@Override
		public void close() {
			// TODO Auto-generated method stub

		}

		@Override
		public Text getCurrentKey() {
			// TODO Auto-generated method stub
			return currentKey;
		}

		@Override
		public Text getCurrentValue() {
			// TODO Auto-generated method stub
			return currentValue;
		}

		@Override
		public float getProgress() {
			// TODO Auto-generated method stub
			float progress = 0;
			if (finishConverting) {
				progress = 1;
			}
			return progress;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) {
			this.fileSplit = (FileSplit) arg0;
			this.jobContext = arg1;
			String filename = fileSplit.getPath().getName();
			this.currentKey = new Text(filename);
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			// TODO Auto-generated method stub
			if (!finishConverting) {
				int len = (int) fileSplit.getLength();
//          byte[] content = new byte[len];
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
				FSDataInputStream in = fs.open(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
				String line = "";
				String total = "";
				while ((line = br.readLine()) != null) {
					total = total + line + "\n";
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
