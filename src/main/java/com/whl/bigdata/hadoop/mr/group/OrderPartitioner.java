package com.whl.bigdata.hadoop.mr.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
	/**
	 * 相同orderID号会发往同一个reducer
	 */
	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
