package com.qf.mr.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 编写一个订单的处理Mapper
 */
public class OrderMapper extends Mapper<LongWritable, Text, Order, NullWritable> {
    private Order order = new Order();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split("\t");
        order.setOrderId(str[0]);
        order.setProId(str[1]);
        order.setPrice(Double.parseDouble(str[2]));

        context.write(order, NullWritable.get());
    }
}
