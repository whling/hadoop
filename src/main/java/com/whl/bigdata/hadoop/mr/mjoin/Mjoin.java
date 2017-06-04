package com.whl.bigdata.hadoop.mr.mjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * Title: Mjoin Description: 之前如果join操作都是在reducer端进行的话，那么可能会造成数据倾斜，
 * 导致某些reducer任务压力过大，应该直接在map阶段完成join操作(使用distributeCache将资源加载至map工作目录) Company:
 * 
 * @author 17697
 * @date 2017年5月18日 下午12:40:04
 */
public class Mjoin {

	static class MjoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		// 用来存放product信息的map，此map会先缓存在map任务中
		private HashMap<String, String> pdtsMap = new HashMap<>();
		// map输出的key
		private Text text = new Text();

		/**
		 * 经过查看Mapper类源码，发现，执行map任务之前会调用一次setup方法，可以利用此方法做初始化工作
		 * 执行完map任务之后会调用一次cleanup方法，可以利用此方法做一些map数据再处理或者判断工作
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFile = context.getCacheFiles();
			Path tagSetPath = new Path(cacheFile[0]);

			FSDataInputStream inputStream = null;
			try {
				FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000/"), context.getConfiguration(),
						"hadoop");
				inputStream = fileSystem.open(tagSetPath);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			while (StringUtils.isNotEmpty(line = br.readLine())) {
				String[] pdtsInfo = line.split(",");
				pdtsMap.put(pdtsInfo[0], pdtsInfo[1]);
			}
			br.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String odsInfo = value.toString();
			String[] fields = odsInfo.split("\t");
			String pName = pdtsMap.get(fields[1]);
			text.set(odsInfo + "\t" + pName);
			context.write(text, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://hadoop01:9000/");

		Job job = Job.getInstance(configuration);

		job.setJarByClass(Mjoin.class);
		job.setMapperClass(MjoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 指定一个文件缓存到所有task的运行节点工作目录
		/* job.addArchiveToClassPath(archive); */ // 设置缓存压缩文件(jar包)到classpath
		/* job.addFileToClassPath(file); */ // 缓存普通文件到task运行节点的classpath
		/* job.addCacheArchive(uri); */ // 缓存jar包到task节点的工作目录
		/* job.addCacheFile(uri); */ // 缓存普通文件到task节点的工作目录
		job.addCacheFile(new URI("hdfs://hadoop01:9000/pdts.txt"));
		// 指定reducer的任务数量
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
