package com.wxn.hadoop;
/*
 * Created by wxn
 * 2018/11/12 5:22
 */


import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.*;

public class PredictionTest {

	public static final String HDFS_PATH = "hdfs://192.168.26.128:8020";

	FileSystem fileSystem = null;
	Configuration configuration = null;

	@Test
	public void testRead() throws Exception {
		//读取classcount文件 并写入classMap中
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
		//读取wordcount文件并写入wordMap中
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
		Map<String, Double> pCiMap = getPCi(classMap);
		Map<Map<String, String>, Double> pTCMap = getpTC(wordMap);
		Map<String, String> map = new HashMap<>();
		map.put("UK", "english0");
		if (pTCMap.get(map)!=null){
			System.out.println(pTCMap.get(map));
		}else {
			map.put("UK",null);
			System.out.println(pTCMap.get(map));
		}
	}


	/**
	 * 计算先验概率
	 */
	private Map<String, Double> getPCi(Map<String, Long> classMap) {

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
	private Map<Map<String, String>, Double> getpTC(Map<Map<String, String>, Long> wordclass) {
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





	@Before
	public void setUp() throws Exception {

		configuration = new Configuration();
		fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
		System.out.println("HDFSApp.setUp");
	}

	@After
	public void tearDown() throws Exception {
		configuration = null;
		fileSystem = null;

		System.out.println("HDFSApp.tearDown");
	}
}
