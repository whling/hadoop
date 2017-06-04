package com.whl.bigdata.hadoop.mr.logenhance;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * Title: LogenhanceOutputformat
 * <p>
 * Description:map和reduce阶段输出都会调用outputformat的getRecordWriter获取RecordWriter,调用此对象的write方法
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @author 17697
 * @date 2017年5月19日 下午12:49:12
 */
public class LogenhanceOutputformat extends FileOutputFormat<Text, NullWritable> {
	/**
	 * task往外面写数据的时候会调用此方法获取recordwriter对象
	 */
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
		// 增强日志的存放路径
		FSDataOutputStream fs1 = fs.create(new Path("D:/mr/weblogenhance/logenhance/log.data"));
		// 待处理的日志存放路径
		FSDataOutputStream fs2 = fs.create(new Path("D:/mr/weblogenhance/logenhance/handler.data"));
		return new LogenhanceRecordWriter(fs1, fs2);
	}

	static class LogenhanceRecordWriter extends RecordWriter<Text, NullWritable> {
		FSDataOutputStream fs1;
		FSDataOutputStream fs2;

		public LogenhanceRecordWriter(FSDataOutputStream fs1, FSDataOutputStream fs2) {
			super();
			this.fs1 = fs1;
			this.fs2 = fs2;
		}

		/**
		 * 自定义写出数据流程
		 */
		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			// 往外面写数据
			String text = key.toString();
			if (text.contains("tocrawl")) {
				// 表示写到待抓取文件中
				fs2.write(text.getBytes("utf-8"));
			} else {
				// 表示是增强日志
				fs1.write(text.getBytes("utf-8"));
			}

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if (fs1 != null) {
				fs1.close();
			}
			if (fs2 != null) {
				fs2.close();
			}
		}

	}

}
