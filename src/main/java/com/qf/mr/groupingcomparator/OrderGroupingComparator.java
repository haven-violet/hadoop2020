package com.qf.mr.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 写一个分组排序的抽象类的实现类,定义分组排序的规则
 */
public class OrderGroupingComparator extends WritableComparator {
    /**
     * 在实现继承分组排序的类中,默认一定要重写父类的构造器,并且要传入当前分组排序对象Order.class
     * 并且还要指定必须创建新的实例参数为true,这个分组排序中大坑
     */
    public OrderGroupingComparator() {
        super(Order.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Order orderBefore = (Order) a;
        Order orderAfter = (Order) b;
        //具体规则是: 如果两个order的ID相同,那么就把他们放在一个组里面
        return orderBefore.getOrderId().compareTo(orderAfter.getOrderId());
    }
}
