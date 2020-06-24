package com.qf.mr.depend;

import com.qf.mr.partitioner.CatPartitioner;
import com.qf.mr.seri.Cat;
import com.qf.mr.seri.CatDriver;
import com.qf.mr.seri.CatMapper;
import com.qf.mr.seri.CatReducer;
import com.qf.mr.wordcount.WordCountDriver;
import com.qf.mr.wordcount.WordCountMapper;
import com.qf.mr.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobFactory {
    public static Job getCatJob() throws IOException {
        //        1.得到配置文件
        Configuration configuration = new Configuration();


//        2.获取一个job,通过配置文件获取
        Job job = Job.getInstance(configuration);

//        3.指定jar的位置
        job.setJarByClass(CatDriver.class);

//        4.指定Mapper的运行类
        job.setMapperClass(CatMapper.class);

//        5.指定Mapper输出的key的类型
        job.setMapOutputKeyClass(Cat.class);

//        6.指定Mapper输出的value的类型
        job.setMapOutputValueClass(NullWritable.class);

//        7.指定Reducer运行类
        job.setReducerClass(CatReducer.class);

//        8.指定Reducer输出的key的类型
        job.setOutputKeyClass(Cat.class);

//        9.指定Reducer输出的value的类型
        job.setOutputValueClass(NullWritable.class);

        //通过job设置自定义分区类
        job.setPartitionerClass(CatPartitioner.class);

        //设置reducer的数量
        //NumReduceTasks的设定决定了输出为几个分区,优先级大于自定义分区的数量
        //分区数为4,但是设定reduceTask数量大于自定义分区数,那么可以正常输出，多余出来的分区中数据为0
        //分区数为4,但是设定reduceTask数量小于自定义分区数,系统会报错
        //如果NumberReduceTasks设定为1(或者不设,默认就是1),那么系统就根本不会去找自定义分区类,而是返回1-1=0号分区
        job.setNumReduceTasks(4);


        String outputString = "F:/catinput";
        String inputString = "F:/catoutput";


//        10.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(outputString));

//        11.指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job, new Path(inputString));

        return job;
    }


    public static Job getWordCountJob() throws IOException {
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

        String inputString = "F:/catoutput";
        String outputString = "F:/wcoutput";

//        10.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(inputString));

//        11.指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job, new Path(outputString));

        return job;
    }

    public static Job getWordCountJob1() throws IOException {
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

        String inputString = "F:/wcoutput";
        String outputString = "F:/wcoutput1";

//        10.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(inputString));

//        11.指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job, new Path(outputString));

        return job;
    }
}
