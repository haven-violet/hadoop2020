package com.qf.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 定义一个WordCountCombiner,用在MapTask阶段进行汇总操作,减少网络传输,等价于一个小的Reducer
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 类似于一个小型reducer,相当于省份统计人数
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));
    }
}
