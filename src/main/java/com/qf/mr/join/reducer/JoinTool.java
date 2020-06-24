package com.qf.mr.join.reducer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 定义reducer端Tool的编写
 */
public class JoinTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //1.创建job
        Job job = Job.getInstance(getConf());

        //2.指定jar的位置
        job.setJarByClass(JoinTool.class);

        //3.指定Mapper和Reducer的运行类
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        //4.指定Mapper和Reducer输出的 key, value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderProduct.class);

        job.setOutputKeyClass(OrderProduct.class);
        job.setOutputValueClass(NullWritable.class);

        //5.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    /**
     * 定义执行Tool的main方法,用来执行当前MR程序
     * @param args
     */
    public static void main(String[] args) throws Exception {
        args = new String[]{"F:/qian/input", "F:/qian/output"};
        ToolRunner.run(new JoinTool(), args);
    }
}
