package com.whl.bigdata.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsDemo {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");
		// fs.copyFromLocalFile(new Path("d://hello"), new Path("/"));
		fs.delete(new Path("/hello"), true);
		fs.close();
	}
}
