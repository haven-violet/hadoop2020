package com.qf.mr.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一个订单类Order,用来处理文本文件的输入
 * 因为要对当前订单类作为key,进行排序和序列化,所以要实现WritableComparable
 */
public class Order implements WritableComparable<Order> {
    private String orderId;
    private String proId;
    private Double price;

    public Order() {
    }

    @Override
    public String toString() {
        return  orderId + "\t" +
                proId + "\t" +
                price;
    }

    @Override
    public boolean equals(Object o) {
        System.out.println("================================888988989");
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Order order = (Order) o;

        if (orderId != null ? !orderId.equals(order.orderId) : order.orderId != null) return false;
        if (proId != null ? !proId.equals(order.proId) : order.proId != null) return false;
        return price != null ? price.equals(order.price) : order.price == null;
    }

    @Override
    public int hashCode() {
        System.out.println("---------------------99999999999");
        int result = orderId != null ? orderId.hashCode() : 0;
        result = 31 * result + (proId != null ? proId.hashCode() : 0);
        result = 31 * result + (price != null ? price.hashCode() : 0);
        return result;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProId() {
        return proId;
    }

    public void setProId(String proId) {
        this.proId = proId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(Order o) {
        int result = orderId.compareTo(o.getOrderId());
        if(result == 0){
            result = Double.compare(price, o.getPrice());
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(proId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        proId = in.readUTF();
        price = in.readDouble();
    }
}
