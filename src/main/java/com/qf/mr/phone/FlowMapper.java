package com.qf.mr.phone;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean bean = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\s+");
        //18879001479    00-FD-07-A4-72-B8:CMCC    120.196.100.82    i02.c.aliimg.com        24    27    2481    24681    200

        bean.set(Long.parseLong(split[split.length-2]), Long.parseLong(split[split.length-1]));
        v.set(split[0]);

        context.write(bean, v);
    }
}
