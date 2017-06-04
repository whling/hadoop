package com.whl.bigdata.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * Title: HbaseTest
 * <p>
 * Description:Hbase的java客户端，只需要连接zookeeper就能操作hbase
 * zookeeper上保存着hbase的ROOT表信息以及一些HRegionServer、RHMaster信息
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @author 17697
 * @date 2017年6月3日 下午9:37:50
 */
public class HbaseTest {
	private Configuration conf;
	private Connection connection;
	private Table table;

	@Before
	public void init() throws Exception {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");// zookeeper地址
		conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = ConnectionFactory.createConnection(conf);
		String tableName = "user";
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		if (hBaseAdmin.tableExists(Bytes.toBytes(tableName))) {
			// 此处要求表已经存在，否则会出错
			table = connection.getTable(TableName.valueOf(tableName));
		} else {
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			desc.addFamily(new HColumnDescriptor("base_info"));
			desc.addFamily(new HColumnDescriptor("extra_info"));
			hBaseAdmin.createTable(desc);
			table = connection.getTable(TableName.valueOf(tableName));
		}
	}

	@Test
	public void testCreateTable() throws Exception {
		// 创建表管理类
		HBaseAdmin admin = new HBaseAdmin(conf);
		TableName name = TableName.valueOf("test");
		// 创建表描述类
		HTableDescriptor desc = new HTableDescriptor(name);
		// 创建列族，并将其添加至表描述信息中
		desc.addFamily(new HColumnDescriptor("info1"));
		desc.addFamily(new HColumnDescriptor("info2"));
		admin.createTable(desc);
	}

	@Test
	@SuppressWarnings("deprecation")
	public void deleteTable() throws MasterNotRunningException, ZooKeeperConnectionException, Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable("test");
		admin.deleteTable("test");
		admin.close();
	}

	/**
	 * 向hbase中增加数据
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "deprecation", "resource" })
	@Test
	public void insertData() throws Exception {
		table.setAutoFlushTo(false);
		table.setWriteBufferSize(534534534);
		ArrayList<Put> arrayList = new ArrayList<Put>();
		for (int i = 21; i < 50; i++) {
			/**
			 * 每一个put表示一行数据，每行有唯一的rowkey,此处rowkey为1234
			 */
			Put put = new Put(Bytes.toBytes("1234" + i));
			put.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("wangwu" + i));
			put.add(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes(1234 + i));
			arrayList.add(put);
		}

		// 插入数据
		table.put(arrayList);
		// 提交
		table.flushCommits();
	}

	/**
	 * 修改数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void uodateData() throws Exception {
		Put put = new Put(Bytes.toBytes("1234"));
		put.add(Bytes.toBytes("base_info"), Bytes.toBytes("namessss"), Bytes.toBytes("whling"));
		put.add(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes(1234));
		// 插入数据
		table.put(put);
		// 提交
		table.flushCommits();
	}

	/**
	 * 删除数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteDate() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("1234"));
		table.delete(delete);
		table.flushCommits();
	}

	@After
	public void close() throws Exception {
		table.close();
		connection.close();
	}
}
