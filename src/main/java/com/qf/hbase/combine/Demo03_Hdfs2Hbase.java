package com.qf.hbase.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/30 0030
*@Description 将hdfs中的数据进行计算后存储到hbase中
**/
public class Demo03_Hdfs2Hbase implements Tool {

    //1. 创建配置对象
    private Configuration configuration;
    private final static String HBASE_CONNECT_KEY = "hbase.zookeeper.quorum";
    private final static String HBASE_CONNECT_VALUE = "hadoop201:2181,hadoop202:2181,hadoop203:2181";
    private final static String HDFS_CONNECT_KEY = "fs.defaultFS";
    private final static String HDFS_CONNECT_VALUE = "hdfs://hadoop201:9000";

    @Override
    public void setConf(Configuration conf) {
        conf.set(HBASE_CONNECT_KEY, HBASE_CONNECT_VALUE); // 设置连接的hbase
        conf.set(HDFS_CONNECT_KEY, HDFS_CONNECT_VALUE); // 设置连接的hadoop
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    /**
    *@Description
    **/
    public static class HbaseMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        public LongWritable v = new LongWritable(1);
        public Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           //获取hdfs中数据
            String lines = value.toString();
            String[] fileds = lines.split("\t");
            for (String filed:fileds){
                //简单的清洗  age:18
                if(filed.contains("age")){
                    String[] kvs = filed.split(":");
                    k.set(kvs[1]);
                    //输出
                    context.write(k,v);
                }
            }
        }
    }


    /**
    *@Description
    **/
    public static class HbaseReducer extends TableReducer<Text,LongWritable, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //计算
            int counter = 0;
            for (LongWritable l : values){
                counter += l.get();
            }

            //构造输出到hbase的put对象
            /**
            *  18 2
             * 16 1
            **/

            Put put = new Put(Bytes.toBytes(key.toString()));
            //往put中添加数据
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age_count"),Bytes.toBytes(counter+""));

            //写出  value:可以是put和delete对象
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //获取job
        //1. 获取job
        Job job = Job.getInstance(configuration, "hdfs2hbase");
        //2. 设置运行jar
        job.setJarByClass(Demo03_Hdfs2Hbase.class);
        //设置mapper阶段
        job.setMapperClass(HbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关
        //创建表
        createTable("age_count");
        TableMapReduceUtil.initTableReducerJob("age_count",HbaseReducer.class,job);

        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //6. 提交
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    /**
    *@Author 东哥
    *@Company 千锋好程序员大数据
    *@Date 创建表
    *@Description
    **/
    private void createTable(String age_count) {
        Admin admin = null;
        try {
            //获取admin
            Configuration conf = HBaseConfiguration.create();
            conf.set(HBASE_CONNECT_KEY,HBASE_CONNECT_VALUE);
            Connection conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();

            //判断表是否存在
            if(!admin.tableExists(TableName.valueOf(age_count))){
                //表不存在创建
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(age_count));
                //创建列簇
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
                hColumnDescriptor.setBloomFilterType(BloomType.ROW);
                hColumnDescriptor.setVersions(1,3);
                hColumnDescriptor.setBlockCacheEnabled(true);
                //将列簇添加到表中
                hTableDescriptor.addFamily(hColumnDescriptor);
                //提提交创建
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
            } catch (IOException e) {
               //do nothing
            }
        }
    }

    //主入口
    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseConfiguration.create(), new Demo03_Hdfs2Hbase(), args);
    }
}