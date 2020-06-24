package com.qf.mr.join.reducer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一个订单产品Bean,用来同时封装Order和Product的信息
 * 因为要对bean进行传输,所以要实现Writable接口Write和readfields方法
 */
public class OrderProduct implements Writable {
//    定义<订单表>中的属性   1001	20150710	P001	2

    //订单ID
    private String orderId = "";
    //订单日期
    private String dateString = "";
    //产品ID
    private String pid;
    //产品数量
    private int amount;

//    定义<产品表>中的属性   P001	苹果	1000	8000

    //产品ID
//    private String pid;   使用订单表中的共享pid
    //产品名称
    private String pname = "";
    //产品类型
    private int type;
    //产品价格
    private double price;

    //定义当前行记录是属于订单表还是产品表(因为同一时刻只有一张表的一行数据传入) 0=订单表  1=产品表
    private int flag;

    @Override
    public String toString() {
        return "OrderProduct{" +
                "orderId='" + orderId + '\'' +
                ", dateString='" + dateString + '\'' +
                ", pid='" + pid + '\'' +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                ", type=" + type +
                ", price=" + price +
                ", flag=" + flag +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrderProduct that = (OrderProduct) o;

        if (amount != that.amount) return false;
        if (type != that.type) return false;
        if (Double.compare(that.price, price) != 0) return false;
        if (flag != that.flag) return false;
        if (orderId != null ? !orderId.equals(that.orderId) : that.orderId != null) return false;
        if (dateString != null ? !dateString.equals(that.dateString) : that.dateString != null) return false;
        if (pid != null ? !pid.equals(that.pid) : that.pid != null) return false;
        return pname != null ? pname.equals(that.pname) : that.pname == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = orderId != null ? orderId.hashCode() : 0;
        result = 31 * result + (dateString != null ? dateString.hashCode() : 0);
        result = 31 * result + (pid != null ? pid.hashCode() : 0);
        result = 31 * result + amount;
        result = 31 * result + (pname != null ? pname.hashCode() : 0);
        result = 31 * result + type;
        temp = Double.doubleToLongBits(price);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + flag;
        return result;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(dateString);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeInt(type);
        out.writeDouble(price);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        dateString = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        pname = in.readUTF();
        type = in.readInt();
        price = in.readDouble();
        flag = in.readInt();
    }
}
