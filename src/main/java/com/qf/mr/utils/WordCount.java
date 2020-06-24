package com.qf.mr.utils;

import com.qf.mr.combiner.WordCountCombiner;
import com.qf.mr.wordcount.WordCountMapper;
import com.qf.mr.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 定义一个MR的驱动类,等价于驱动Driver类,但是默认要继承Configured和实现Tool接口
 * 可以把原来Job只能单个放在main方法中变成类似多线程操作的方式去执行,可以动态设置job,调用,传参
 */
public class WordCount extends Configured implements Tool {

    /**
     * 实现Tool中的run方法类似于前面Driver的main方法,是job执行的入口,第一步先把原来的main方法中代码考入即可
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        //1.获取配置文件
        //Configuration configuration = new Configuration();

        //2.创建一个Job任务
        Job job = Job.getInstance();

        //3.指定jar类
        job.setJarByClass(WordCount.class);

        //4.指定Mapper和Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //5.指定Mapper和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置当前job的Combiner类
        job.setCombinerClass(WordCountCombiner.class);

        //6.指定输入和输出文件夹路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交任务
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        args = new String[]{"F:/input", "F:/output"};
        ToolRunner.run(new WordCount(), args);
    }
}
