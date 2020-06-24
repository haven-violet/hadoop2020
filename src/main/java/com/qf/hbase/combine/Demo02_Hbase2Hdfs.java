package com.qf.hbase.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/30 0030
*@Description hbase的数据转存hdfs
**/
public class Demo02_Hbase2Hdfs implements Tool {

    //1. 创建配置对象
    private Configuration configuration;
    private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String HBASE_CONNECT_VALUE = "hadoop201:2181,hadoop202:2181,hadoop203:2181";
    private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
    private final static String HDFS_CONNECT_VALUE = "hdfs://hadoop201:9000";
    private final static String MAPREDUCE_CONNECT_KEY = "mapreduce.framework.name";
    private final static String MAPREDUCE_CONNECT_VALUE = "yarn";

    @Override
    public int run(String[] args) throws Exception {
        //1. 获取job
        Job job = Job.getInstance(configuration, "hbase2hdfs");
        //2. 设置运行jar
        job.setJarByClass(Demo02_Hbase2Hdfs.class);
        /*
         * 3. 设置TableMapper初始参数
         * 设置从HBase表中读取数据作为输入
         * 表名tablename, 扫描器scan,mapper类，mapper输出key的类，mapper输出的value类，jo
         */
//            job.setMapperClass(HBaseMapper.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(NullWritable.class);
        TableMapReduceUtil.initTableMapperJob("test", getScan(), HBaseMapper.class,
                Text.class, NullWritable.class, job);
        //4. 设置输出格式
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        /*
         * 5. 设置从HBase表中读取数据作为输入
         * 表名tablename, 扫描器scan,mapper类，mapper输出key的类，mapper输出的value类，job
         */
        //6. 提交
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE); // 设置连接的hbase
        conf.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE); // 设置连接的hadoop
        //conf.set(MAPREDUCE_CONNECT_KEY, MAPREDUCE_CONNECT_VALUE); // 设置使用的mr运行平台
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    private static Scan getScan() {
        return new Scan();
    }

    /**
     * 一、 自定义Mapper类
     * 从HBase中读取某表数据：ns1:user_info
     * 1. 读取一行
     * 003                                                column=base_info:age, timestamp=1546957041028, value=15
     * 003                                                column=base_info:name, timestamp=1546957041028, value=narudo
     * 003                                                column=base_info:sex, timestamp=1546957041028, value=male
     *
     *  2. 输出
     *  age:15 name:narudo sex:male*
     *
     */
    public static class HBaseMapper extends TableMapper<Text, NullWritable> {

        private Text k = new Text();

        /**
         *file:
         * hbase的一张表===> scan
         * @param key ：
         * @param value : 返回根据rowkey的一行结果
         * @param context
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //0. 定义字符串存放最终结果
            StringBuffer sb = new StringBuffer();
            //1. 获取扫描器进行扫描解析
            CellScanner cellScanner = value.cellScanner();

            //2. 推进
            while (cellScanner.advance()) {
                //3. 获取当前单元格
                Cell cell = cellScanner.current();
                //4. 拼接字符串
                sb.append(new String(CellUtil.cloneQualifier(cell))); //列名称
                sb.append(":");
                sb.append(new String(CellUtil.cloneValue(cell))); //列对应的值
                sb.append("\t");
            }
            //5. 写出
            k.set(sb.toString());
            context.write(k, NullWritable.get());
        }
    }

    //主函数
    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseConfiguration.create(), new Demo02_Hbase2Hdfs(), args);
    }
}