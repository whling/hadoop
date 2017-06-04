package com.whl.bigdata.hadoop.mr.rjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

/**
 * id date pid amount id pname category_id price
 * <p>
 * Title: InfoBean
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @author 17697
 * @date 2017年5月18日 上午8:57:51
 */
public class InfoBean implements Writable {
	private int oid;
	private String pid;
	private String pname;
	/* 标识：指定是order还是product,order为0，product为1 */
	private int flag;

	public InfoBean() {
	}

	public void set(int oid, String pid, String pname, int flag) {
		this.oid = oid;
		this.pid = pid;
		this.pname = pname;
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(oid);
		out.writeUTF(pid);
		out.writeUTF(pname);
		out.writeInt(flag);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.oid = in.readInt();
		this.pid = in.readUTF();
		this.pname = in.readUTF();
		this.flag = in.readInt();

	}


	public int getOid() {
		return oid;
	}

	public void setOid(int oid) {
		this.oid = oid;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	@Override
	public String toString() {
		return "oid=" + oid + ", pid=" + pid + ", pname=" + pname;
	}

}
