package com.qf.mr.phone;

import com.qf.mr.utils.FolderUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class FlowTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //1.创建job
        Job job = Job.getInstance(getConf());

        //2.指定jar的位置
        job.setJarByClass(FlowTool.class);

        //3.指定Mapper和Reducer的运行类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.指定Mapper和Reducer输出的 key, value
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"F:/qian/input", "F:/qian/output"};
        FolderUtil.deleteFolder(new File(args[1]));
        ToolRunner.run(new FlowTool(), args);
    }
}
