package com.whl.bigdata.hadoop.mr;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class StringUtilsTest {
	public static void main(String[] g) {
		Float float1 = new Float(2F);
		Float float2 = new Float(1F);
		int i = float1.compareTo(float2);
		System.out.println(i);
		// null 和 ""操作~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// 判断是否Null 或者 ""
		System.out.println(StringUtils.isEmpty(null));
		System.out.println(StringUtils.isNotEmpty(null));
		// 判断是否null 或者 "" 去空格~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		System.out.println(StringUtils.isBlank(""));
		System.out.println(StringUtils.isNotBlank(null));
		// 去空格.Null返回null~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		System.out.println(StringUtils.trim(null));
		// 去空格，将Null和"" 转换为Null
		System.out.println(StringUtils.trimToNull(""));
		// 去空格，将NULL 和 "" 转换为""
		System.out.println(StringUtils.trimToEmpty(null));

		String[] str = { "abc", "bca", "cab", "cba", "aaa", "111", "232", "112", "ABC" };

		Arrays.sort(str);

		for (String string : str) {
			System.out.println(string);
		}
	}
}
