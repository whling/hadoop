package com.whl.bigdata.hadoop.mr;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateFormatTest {
	public static void main(String[] args) throws Exception {
		SimpleDateFormat dateFormat1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Date date = dateFormat1.parse("18/Sep/2013:06:49:18");
		String string = dateFormat2.format(date);
		System.out.println(string);
	}
}
