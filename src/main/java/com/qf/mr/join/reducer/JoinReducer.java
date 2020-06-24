package com.qf.mr.join.reducer;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text, OrderProduct, OrderProduct, NullWritable> {

    @Override
    protected void reduce(Text pid, Iterable<OrderProduct> values, Context context) throws IOException, InterruptedException {
        OrderProduct product = new OrderProduct();
        List<OrderProduct> list = new ArrayList<>();

        //遍历所有key为p001(p001)为例的记录
        for (OrderProduct value : values) {
            //如果是产品表,记录只有一条
            if(value.getFlag() == 1){
                product.setPid(pid.toString());
                product.setPname(value.getPname());
                product.setType(value.getType());
                product.setPrice(value.getPrice());
            }

            //如果是订单表,记录有0到多个
            if(value.getFlag() == 0){
//                1001	20150710	P001	2
                OrderProduct order = new OrderProduct();
                order.setOrderId(value.getOrderId());
                order.setDateString(value.getDateString());
                order.setPid(pid.toString());
                order.setAmount(value.getAmount());
                list.add(order);
            }
        }

        //对order列表进行遍历,然后把product中的数据进行赋值
        for (OrderProduct order : list) {
            order.setPname(product.getPname());
            order.setType(product.getType());
            order.setPrice(product.getPrice());

            context.write(order, NullWritable.get());
        }
    }
}
