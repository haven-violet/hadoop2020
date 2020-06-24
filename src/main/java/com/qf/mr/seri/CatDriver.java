package com.qf.mr.seri;

import com.qf.mr.partitioner.CatPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 定义一个Driver类,把Mapper和Reducer进行关联,打包成一个任务(job),并且提交执行
 */
public class CatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        启动一个job的流程
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
