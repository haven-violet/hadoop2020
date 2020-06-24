package com.qf.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 定义一个Driver类,把Mapper和Reducer进行关联,打包成一个任务(job),并且提交执行
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        启动一个job的流程
//        1.得到配置文件
        Configuration configuration = new Configuration();


//        2.获取一个job,通过配置文件获取
        Job job = Job.getInstance(configuration);

//        3.指定jar的位置
        job.setJarByClass(WordCountDriver.class);

//        4.指定Mapper的运行类
        job.setMapperClass(WordCountMapper.class);

//        5.指定Mapper输出的key的类型
        job.setMapOutputKeyClass(Text.class);

//        6.指定Mapper输出的value的类型
        job.setMapOutputValueClass(IntWritable.class);

//        7.指定Reducer运行类
        job.setReducerClass(WordCountReducer.class);

//        8.指定Reducer输出的key的类型
        job.setOutputKeyClass(Text.class);

//        9.指定Reducer输出的value的类型
        job.setOutputValueClass(IntWritable.class);

//        10.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

//        11.指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        12.提交任务,等待执行完成,如果参数为true,那么打印信息
        boolean b = job.waitForCompletion(true);

//        13.自定义退出,根据job返回的结果为0 正常, 非0 有错误
        System.exit(b ? 0 : 1);
    }
}
