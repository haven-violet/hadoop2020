package com.qf.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 新增一个Reducer的继承类,要定义泛型,其中输入的keyin, valuein就是Mapper输出keyout, valueout
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 实现Reducer中的reduce方法,其中key就是Mapper输出的key, value就是Mapper输出value所有key相同的集合(Iterable)
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        key = java (以key为java为例,每个key会执行一次reduce函数)
//        value = (1,1,1)
        //对key相同的value进行累加,求得当前为key的单词的数量
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();

        }
        //对reduce的结果进行输出
        context.write(key, new IntWritable(sum));
    }
}
