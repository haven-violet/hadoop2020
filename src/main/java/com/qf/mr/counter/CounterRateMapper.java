package com.qf.mr.counter;

import com.qf.mr.top.Rate;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 在原来的Mapper基础之上添加计数器功能
 */
public class CounterRateMapper extends Mapper<LongWritable, Text, Rate, NullWritable> {
    private SortedMap<Rate, String> top = new TreeMap<>();

    private int topN;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        topN = context.getConfiguration().getInt("topN", 10);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
//        value = move14 01 2012-12-38 user1
        Rate rate = new Rate();
        rate.setMovie(s[0]);
        rate.setRate(s[1]);
        rate.setTimeStamp(s[2]);
        rate.setUid(s[3]);

        top.put(rate, null);

        //如果超过topN个,删除最小,第一个元素
        if(top.size() > topN){
            top.remove(top.firstKey());
        }

        //通过枚举构建一个计数器,计数器的名字可以认为是枚举名
        Counter counter = context.getCounter(Level.INFO);

        //计数器递增
        counter.increment(1);

        //通过组名和计数器名构建一个计数器,计数器结果可以通过控制台查看
        Counter counter1 = context.getCounter("qf", "user1");
        if("user1".equals(rate.getUid())){
            counter1.increment(1);
        }

        /**
         * 需求: 打印出评分好的电影和评分差的电影数量, 评分>9分认为是好电影
         */
        Counter good = context.getCounter("rate", "good");
        Counter bad = context.getCounter("rate", "bad");
        if(Integer.parseInt(rate.getRate()) > 9){
            good.increment(1);
        }else{
            bad.increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Rate> rates = top.keySet();
        for (Rate rate : rates) {
            System.out.println(rate);
            context.write(rate, NullWritable.get());
        }
    }
}
