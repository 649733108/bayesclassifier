package com.wxn.hadoop;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest {
	/**
	 * Rigorous Test :-)
	 */
	@Test
	public void testReadFile() throws Exception {

		String dirPath = "C:\\Users\\64973\\Desktop\\Country\\CHINA";

		List<String> countryList = readDictionary(dirPath);

		for (String s : countryList) {
			System.out.println(s);
		}
		System.out.println();
		System.out.println(countryList.size());

	}

	/**
	 * 读取文件夹中的所有txt文件
	 * 将单词输出到List
	 */
	private List<String> readDictionary(String dirPath) throws IOException {
		List<String> resultList = new ArrayList<>();
		File file = new File(dirPath);
		File[] files = file.listFiles();
		for (int i = 0; i < files.length; i++) {
			InputStreamReader reader = new InputStreamReader(new FileInputStream(files[i]));
			BufferedReader bufferedReader = new BufferedReader(reader);
			String line = bufferedReader.readLine();
			while (line != null) {
				resultList.add(line);
				line = bufferedReader.readLine();
			}
		}
		return resultList;
	}

}
