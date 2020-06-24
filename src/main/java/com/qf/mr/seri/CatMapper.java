package com.qf.mr.seri;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 定义一个Cat的mapper类,用来读取硬盘上的文本数据,注意泛型输出的key为Cat,value为NullWritable
 */
public class CatMapper extends Mapper<LongWritable, Text, Cat, NullWritable> {


    /**
     * map方法从传入的value读取一行数据,转化成Cat对象
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] cats = s.split(" ");

        //创建Cat对象,并用传入的值进行赋值
        Cat cat = new Cat();
        cat.setName(cats[0]);
        cat.setAge(Integer.parseInt(cats[1]));
        cat.setPrice(Integer.parseInt(cats[2]));
        cat.setNumber(cats[3]);

        //通过context把outKey outValue输出,如果不想输出具体的值,那么要用NullWritable.get()
        context.write(cat, NullWritable.get());

    }


}
