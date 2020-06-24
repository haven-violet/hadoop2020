package com.qf.mr.counter;

import com.qf.mr.top.Rate;
import com.qf.mr.top.RateReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountRateTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        //通过Configuration可以从Job向Mapper或Reducer传递数据
        conf.setInt("topN", 10);

        //1.新建job
        Job job = Job.getInstance(getConf());

        //2.指定jar位置
        job.setJarByClass(CountRateTool.class);

        //3.指定Mapper和Reducer的运行类
        job.setMapperClass(CounterRateMapper.class);
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

        //如果需要程序最后获取计数器的具体的值,那么必须要在所有的Mapper和Reducer执行完成后通过job去获取
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(Level.INFO);
        //可以用程序拿到具体的计数器的值,并且可以进行相应的处理
        long value = counter.getValue();
        System.err.println("value = " + value);

        //通过组名和计数器的名字来获取计数器
        Counter counter1 = counters.findCounter("qf", "user1");
        System.err.println("counter1 = " + counter1.getValue());


        //获取系统内置的所有计数器的名字,并且获取计数器的值
        for (CounterGroup counterGroup : counters) {
            System.out.println("counterGroup.getDisplayName() = " + counterGroup.getDisplayName()+"------------");
            for (Counter counter2 : counterGroup) {
                System.out.println("counter2.getName() = " + counter2.getName());
                System.out.println("counter2.getValue() = " + counter2.getValue());
            }
        }

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"F:/rateinput", "F:/rateoutput"};
        ToolRunner.run(new CountRateTool(), args);
    }
}
