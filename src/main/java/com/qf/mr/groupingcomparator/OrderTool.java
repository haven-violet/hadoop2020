package com.qf.mr.groupingcomparator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OrderTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //1.新建一个job
        Job job = Job.getInstance(getConf());

        //2.指定jar位置
        job.setJarByClass(OrderTool.class);

        //3.指定Mapper和Reducer
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4.指定Mapper和Reducer的输出key, value
        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Order.class);
        job.setOutputValueClass(NullWritable.class);

        //指定分组排序的实现类,让同一个组中的对象,Reducer只输出同一组中第一条记录
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        //5.指定输入文件夹路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //6.指定输出文件夹路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交任务,等待执行完成, 如果参数为true, 那么打印信息
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"F:/orderinput", "F:/orderoutput"};
        ToolRunner.run(new OrderTool(), args);
    }
}
