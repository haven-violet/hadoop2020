package com.qf.mr.join.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 定义一个Mapper用来同时处理产品文件和订单文件中的记录
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, OrderProduct> {
    //定义一个字段key和value,避免在map中多次创建
    OrderProduct orderProduct = new OrderProduct();
    Text pidKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        //从上下文对象中获取当前key的切片信息
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        //根据文件切片得当前切片的文件名
        String fileName = inputSplit.getPath().getName();

        OrderProduct orderProduct = new OrderProduct();

        //判断当前是否是order文件
        if("order.txt".equals(fileName)){
            //定义<订单表>中的属性 1001	20150710	P001	2
            orderProduct.setOrderId(split[0]);
            orderProduct.setDateString(split[1]);
            orderProduct.setPid(split[2]);
            orderProduct.setAmount(Integer.parseInt(split[3]));
            orderProduct.setFlag(0);    //0表示订单表
            pidKey.set(split[2]);   //给pid赋值
        } else {//如果是product.txt
//            定义<产品表>中的属性 P001	苹果	1000	8000
            orderProduct.setPid(split[0]);
            orderProduct.setPname(split[1]);
            orderProduct.setType(Integer.parseInt(split[2]));
            orderProduct.setPrice(Double.parseDouble(split[3]));
            orderProduct.setFlag(1);    //1表示产品表
            pidKey.set(split[0]);   //给pid赋值
        }
        //把pid作为key,orderProduct作为value进行写入Reducer
        context.write(pidKey, orderProduct);
    }
}
