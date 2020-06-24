package com.qf.mr.join.map;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 定义一个Join的Mapper端,用来处理输入数据
 * 需求分析:
 *      1.因为Map端join优先适合大表和小表的组合,所以大表的数据是通过map方法来传入
 *      2.因为在Mapper的map方法中只能传入一种类型的数据,所以小表数据不需要map中传入
 *          而是Mapper端自定义加载,因为只加载一次就ok,所以在setup中加载
 *
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    //定义一个map存储小表的数据,并且定义成field类型,可以在setup方法中初始化,把结果传给map方法
    private Map<String, String> productMap = new HashMap();

    /**
     * setup是对一个MapTask只执行一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        String pid = split[0];
        Integer amount = Integer.parseInt(split[1]);
        //从小表中获取pName的数据,等价于两张表连接Join
        String pName = productMap.get(pid);
        context.write(new Text(pid), new Text(pName + "\t" + amount));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //从上下文中context中获取缓存的文件(小表数据文件)
        URI[] cacheFiles = context.getCacheFiles();
        //获取第一个缓存文件
        String filePath = cacheFiles[0].getPath().toString();

        FileReader reader = new FileReader(filePath);
        //用处理缓冲流BufferedReader来接收节点流reader中的数据,除了缓冲提高性能外,还可以提供readLine方法(整行读取)
        BufferedReader bufferedReader = new BufferedReader(reader);
        String lineString = "";

        //循环变量小表文件中的所有行,并且对每一行进行切分
        while((lineString=bufferedReader.readLine()) != null){
            String[] split = lineString.split(",");
            //把小表的数据存储在map中
            productMap.put(split[0], split[1]);
        }
        bufferedReader.close();
        reader.close();
        IOUtils.closeStream(bufferedReader);
    }
}
