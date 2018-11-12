package com.wxn.hadoop;
/*
 * Created by wxn
 * 2018/11/12 2:43
 */


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;

/**
 * 重写MapWritable方法
 * 实现WritableComparable接口
 */
public class MyMapWritable extends MapWritable implements WritableComparable<MyMapWritable> {

	@Override
	public int compareTo(MyMapWritable myMapWritable) {
		if (myMapWritable == null) {
			return 1;
		}
//		Iterator it = this.keySet().iterator();
//		while (it.hasNext()) {
//			Writable key = (Writable) it.next();
//			if (this.get(key) == null){
//				return -1;
//			}
//			if (myMapWritable.get(key)==null){
//				return 1;
//			}
//			return this.get(key).toString().compareTo(myMapWritable.toString());
//		}
//
//		return -1;

		Writable key1 = (Writable) this.keySet().toArray()[0];
		Writable key2 = (Writable) myMapWritable.keySet().toArray()[0];
		if (!key1.toString().equals(key2.toString())) {
			return key1.toString().compareTo(key2.toString());
		}
		if (this.get(key1) == null) {
			return -1;
		}
		if (myMapWritable.get(key1) == null) {
			return 1;
		}
		return this.get(key1).toString().compareTo(myMapWritable.get(key1).toString());
	}

	@Override
	public String toString() {
		Iterator it = this.keySet().iterator();
		StringBuilder stringBuilder = new StringBuilder();
		while (it.hasNext()) {
			Writable key = (Writable) it.next();
			stringBuilder.append(key.toString()).append("\t")
					.append(this.get(key).toString());
		}
		return stringBuilder.toString();
	}
}
