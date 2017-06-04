package com.whl.bigdata.hive.udf;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFunction extends UDF {
	private static HashMap<Integer, String> map = new HashMap<>();
	static {
		map.put(136, "beijing");
		map.put(137, "shanghai");
		map.put(138, "nanchang");
		map.put(139, "guangzhou");
	}

	public String evaluate(String str) {
		String res = str.toLowerCase();
		return res;
	}

	public String evaluate(Integer phoneNur) {
		String key = String.valueOf(phoneNur);
		String res = map.get(Integer.parseInt(key.substring(0, 3)));
		return res != null ? res : "";
	}
}
