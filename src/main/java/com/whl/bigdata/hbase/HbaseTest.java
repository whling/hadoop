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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
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

	/**
	 * 单条查询
	 * 
	 * @throws Exception
	 */
	@Test
	public void queryData() throws Exception {
		// 指定rowkey
		Get get = new Get(Bytes.toBytes("123449"));
		Result result = table.get(get);
		/**
		 * 获取指定的列族、列名的值（columnFamily/qualifer/value）
		 */
		System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("password"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
	}

	/**
	 * 全表扫描
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		// scan.addFamily(Bytes.toBytes("info"));
		// scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
		scan.setStartRow(Bytes.toBytes("123422"));
		scan.setStopRow(Bytes.toBytes("123431"));
		ResultScanner scanner = table.getScanner(scan);
		int length = 0;
		for (Result result : scanner) {
			length++;
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
			// System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("password"))));
			// System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("name"))));
		}
		System.out.println("总结果:" + length);
	}

	/**
	 * 全表扫描的过滤器 列值过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter1() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 过滤器：列值过滤器
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("wangwu46"));
		// 设置过滤器
		scan.setFilter(filter);

		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
			// System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("password"))));
			// System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("name"))));
		}

	}

	/**
	 * rowkey过滤器(正则表达式)
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter2() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 匹配rowkey以12344开头的
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^12344"));
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
			// System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("password"))));
			// System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("name"))));
		}

	}

	/**
	 * 匹配列名前缀(测试结果不符合预期)
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter3() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 匹配rowkey以12344开头的
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("123446"));
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			if (result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name")) != null) {
				System.out.println("base_info:name："
						+ Bytes.toString(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("password")) != null) {
				System.out.println("base_info:password："
						+ Bytes.toInt(result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("password"))));
			}

		}

	}

	/**
	 * 过滤器集合
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter4() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 过滤器集合：MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
		FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
		// 匹配rowkey以12344开头的
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^12344"));
		// 匹配name的值等于wangsenfeng
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes("wangwu41"));
		filterList.addFilter(filter);
		filterList.addFilter(filter2);
		// 设置过滤器
		scan.setFilter(filterList);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println(
					"info:name：" + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
				System.out.println(
						"info:age：" + Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
				System.out.println(
						"infi:sex：" + Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
				System.out.println(
						"info2:name：" + Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
				System.out.println(
						"info2:age：" + Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
				System.out.println(
						"info2:sex：" + Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex"))));
			}
		}

	}

	@After
	public void close() throws Exception {
		table.close();
		connection.close();
	}
}
