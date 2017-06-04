package com.whl.bigdata.hadoop.mr.logenhance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogenhanceMaster {
	static class LogenhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Map<String, String> ruleMap = new HashMap<>();
		Text k = new Text();
		NullWritable v = NullWritable.get();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			DBLoader.loadMap(ruleMap);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 组名,计数器名称
			Counter counter = context.getCounter("logenhance", "errorNum");
			try {
				String line = value.toString();
				String[] fields = StringUtils.split(line, "\t");
				String url = fields[26];
				String content = ruleMap.get(url);
				if (StringUtils.isEmpty(content)) {
					// 写到待爬取文件
					k.set(url + "\t" + "tocrawl" + "\n");
					context.write(k, v);
				} else {
					// 将content内容增强到原日志之中
					k.set(line + "\t" + content + "\n");
					context.write(k, v);
				}
			} catch (Exception e) {
				counter.increment(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(LogenhanceMaster.class);

		job.setMapperClass(LogenhanceMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 要控制不同的内容写往不同的目标路径，可以采用自定义outputformat的方法
		job.setOutputFormatClass(LogenhanceOutputformat.class);

		FileInputFormat.setInputPaths(job, new Path("D:/mr/weblogenhance/input"));

		// 尽管我们用的是自定义outputformat，但是它是继承制fileoutputformat
		// 在fileoutputformat中，必须输出一个_success文件，所以在此还需要设置输出path
		FileOutputFormat.setOutputPath(job, new Path("D:/mr/weblogenhance/output"));

		// 不需要reducer
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
		System.exit(0);
	}
}
