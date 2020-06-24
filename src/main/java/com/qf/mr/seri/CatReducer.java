package com.qf.mr.seri;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 定义一个CatReducer用来处理mapper序列化的Cat,输入的key,value是Mapper的输出key value
 */
public class CatReducer extends Reducer<Cat, NullWritable, Cat, NullWritable> {

    @Override
    protected void reduce(Cat paramCat, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //新建一个Cat对象,并且从传入的参数Cat中获取属性(注意: 不要直接使用参数Cat)虽然属性值一样,但是反序列化还是有些不同
        Cat cat = new Cat();
        cat.setName(paramCat.getName());
        cat.setAge(paramCat.getAge());
        cat.setPrice(paramCat.getPrice()*10);
        cat.setNumber(paramCat.getNumber());

        //通过context对Cat进行持久化输出
        context.write(cat, NullWritable.get());
    }
}
