package com.whl.bigdata.hadoop.mr.group;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {
	static class SecondarySortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
		OrderBean orderBean = new OrderBean();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] orderInfo = value.toString().split(",");
			
			orderBean.set(orderInfo[0], Float.parseFloat(orderInfo[2]));
			context.write(orderBean, NullWritable.get());

		}
	}

	static class SecondarySortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

		// 在设置了groupingcomparator以后，这里收到的kv数据 就是： <1001 87.6>,null <1001
		// 76.5>,null ....
		// 此时，reduce方法中的参数key就是上述kv组中的第一个kv的key：<1001 87.6>
		// 要输出同一个order的所有订单中最大金额的那一个，就只要输出这个key
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(SecondarySort.class);

		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);

		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 指定shuffle所使用的GroupingComparator类
		job.setGroupingComparatorClass(OrderGroupingComparator.class);
		// 指定shuffle所使用的partitioner类
		job.setPartitionerClass(OrderPartitioner.class);

		job.setNumReduceTasks(4);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
