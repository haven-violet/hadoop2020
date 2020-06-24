package com.qf.mr.top;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class RateMapper extends Mapper<LongWritable, Text, Rate, NullWritable> {
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
