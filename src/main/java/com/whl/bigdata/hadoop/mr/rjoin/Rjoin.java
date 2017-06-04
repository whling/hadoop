package com.whl.bigdata.hadoop.mr.rjoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rjoin {
	static class RjoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		private Text text = new Text();
		private InfoBean infoBean = new InfoBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String name = fileSplit.getPath().getName();
			if (name.startsWith("order")) {
				text.set(split[1]);
				infoBean.set(Integer.parseInt(split[0]), split[1], "", 0);
				context.write(text, infoBean);
			} else {
				text.set(split[0]);
				infoBean.set(0, split[0], split[1], 1);
				context.write(text, infoBean);
			}
		}
	}

	static class RioinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {

		@Override
		protected void reduce(Text text, Iterable<InfoBean> values, Context context)
				throws IOException, InterruptedException {
			InfoBean bean = new InfoBean();
			ArrayList<InfoBean> beans = new ArrayList<InfoBean>();
			for (InfoBean infoBean : values) {
				if (0 == infoBean.getFlag()) {
					// order
					InfoBean iBean = new InfoBean();
					try {
						BeanUtils.copyProperties(iBean, infoBean);
						beans.add(iBean);
					} catch (IllegalAccessException | InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} else {
					// product
					try {
						BeanUtils.copyProperties(bean, infoBean);
					} catch (IllegalAccessException | InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			for (InfoBean infoBean : beans) {
				infoBean.setPname(bean.getPname());
				context.write(infoBean, NullWritable.get());
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// conf.set("mapred.textoutputformat.separator", "\t");

		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		// job.setJarByClass(RJoin.class);
		// job.setJar("c:/join.jar");

		job.setJarByClass(Rjoin.class);
		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(RjoinMapper.class);
		job.setReducerClass(RioinReducer.class);

		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
