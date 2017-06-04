package com.whl.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.gson.Gson;

public class ParseJson extends UDF {
	private static Gson gson = new Gson();

	public String evaluate(String line) {
		MovieRateBean movieRateBean = gson.fromJson(line, MovieRateBean.class);
		return movieRateBean.toString();
	}
}
