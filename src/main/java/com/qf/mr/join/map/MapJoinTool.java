package com.qf.mr.join.map;

import com.qf.mr.utils.FolderUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.net.URI;

public class MapJoinTool extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //设置Reducer阶段的输出为默认压缩格式
        //configuration.set("mapreduce.output.fileoutputformat.compress", "true");

        Job job = Job.getInstance(configuration);

        //3.指定jar位置
        job.setJarByClass(MapJoinTool.class);

        //4.指定Mapper运行类
        job.setMapperClass(MapJoinMapper.class);

        //5.指定Mapper输出的key类型
        job.setMapOutputKeyClass(Text.class);

        //6.指定Mapper输出的value的类型
        job.setMapOutputValueClass(Text.class);

        //不需要Reducer,Map端就可以完成,把numReduceTask设为0即可
        job.setNumReduceTasks(0);

        //给job中添加一个小表缓存文件,注意添加的格式URI是file:///e:/..
        job.addCacheFile(new URI("file:///F:/ATGUIGU/product.txt"));

        //10.指定输入文件夹的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //11.指定输出文件夹的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //通过FileOutputFormat设置输出文件的压缩，并且可以设置具体的压缩编码
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"F://atguigu/input", "F://atguigu/output3"};
        FolderUtil.deleteFolder(new File(args[1]));
        ToolRunner.run(new MapJoinTool(), args);
    }
}
