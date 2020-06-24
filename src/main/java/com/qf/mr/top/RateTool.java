package com.qf.mr.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RateTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        //通过Configuration可以从Job向Mapper或Reducer传递数据
        conf.setInt("topN", 10);

        //1.新建job
        Job job = Job.getInstance(getConf());

        //2.指定jar位置
        job.setJarByClass(RateTool.class);

        //3.指定Mapper和Reducer的运行类
        job.setMapperClass(RateMapper.class);
        job.setReducerClass(RateReducer.class);

        //4.指定Mapper和Reducer输出的key,value
        job.setMapOutputKeyClass(Rate.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Rate.class);
        job.setOutputValueClass(NullWritable.class);

        //5.指定输入文件夹路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //6.指定输出文件夹路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交任务,等待执行完成, 如果指定true,打印信息
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"F:/rateinput", "F:/rateoutput"};
        ToolRunner.run(new RateTool(), args);
    }
}
