package com.whl.bigdata.hadoop.mr.logenhance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class DBLoader {
	public static void loadMap(Map<String, String> ruleMap) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/urldb", "root", "sorry");
			PreparedStatement prepareStatement = connection.prepareStatement("select url,content from url_rule");
			ResultSet res = prepareStatement.executeQuery();
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> map = new HashMap<>();
		loadMap(map);
		System.out.println(map.size());
	}
}
