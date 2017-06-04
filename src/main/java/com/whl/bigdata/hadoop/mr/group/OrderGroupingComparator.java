package com.whl.bigdata.hadoop.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

	public OrderGroupingComparator() {
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean abean = (OrderBean) a;
		OrderBean bbean = (OrderBean) b;

		// 将orderid相同的bean都视为相同，从而聚合为一组
		return abean.getOrderId().compareTo(bbean.getOrderId());
	}

}
