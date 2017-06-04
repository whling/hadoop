package com.whl.bigdata.hadoop.mr.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {
	private String orderId;
	private Float price;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeFloat(price);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.price = in.readFloat();

	}

	/**
	 * 自定义排序规则： 1.因为不同的key可能会分到同一个区，所以先进行订单ID排序
	 * 2.订单ID排序完事之后，再根据金额排序，再在reducer阶段通过groupingComparator分组直接取出
	 */
	@Override
	public int compareTo(OrderBean o) {
		/**
		 * 这个地方不必先根据订单ID排序，因为后面的groupingcomparator是根据订单ID分组的，只要价格顺序由大到小就行
		 */
		int i = this.orderId.compareTo(o.getOrderId());
		if (0 == i) {
			// 表示订单ID相同
			i = -this.price.compareTo(o.getPrice());
		}
		return i;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public Float getPrice() {
		return price;
	}

	public void setPrice(Float price) {
		this.price = price;
	}

	public OrderBean() {
	}

	public void set(String orderId, Float price) {
		this.orderId = orderId;
		this.price = price;
	}

	@Override
	public String toString() {
		return "orderId=" + orderId + ", price=" + price;
	}

}
