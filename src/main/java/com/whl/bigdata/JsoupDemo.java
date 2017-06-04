package com.whl.bigdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public class JsoupDemo {
	public static void main(String[] args) throws Exception {
		// Connection connect =
		// Jsoup.connect("http://site.baidu.com/").userAgent("I am Casablanca");
		// Document doc = connect.get();
		// String title = doc.title();
		// System.out.println(title);
		// Elements elements2 = doc.select("[class=bd]
		// [target=_blank]").tagName("a");
		// for (Element element : elements2) {
		// System.out.println(element.html());
		//
		// }
		// Elements elements = doc.getElementsByClass("search-tab");
		// for (Element element : elements) {
		// System.out.println(element.getElementsByTag("a"));
		// }
		ThreadLocal<String> tLocal = new ThreadLocal<>();
		tLocal.set("hello");
		tLocal.set("world");
		String string = tLocal.get();
		System.out.println(string);

		HashMap<String, String> map = new HashMap<>();
		map.put(null, "hello");
		map.put("薛之谦", "暧昧");
		map.put("莫文蔚", "忽然之间");
		Set<Entry<String, String>> entrySet = map.entrySet();
		for (Entry<String, String> entry : entrySet) {
			System.out.println("key" + entry.getKey() + "  value:" + entry.getValue());
		}
	}
}
