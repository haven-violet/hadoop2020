package com.qf.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 写一个简单的wordCount的Mapper类,注意一定要指定输入的<key, value>和输出的</key, value>
 * Mapper<KEYIN, VALUEIN, KEYIN, VALUEIN>
 *     框架在调用我们写的map业务方法时,会将数据作为参数(一个key,一个value)传入到map方法
 *     KEYIN:是框架(maptask)要传递给map方法的输入参数中的key的数据类型
 *     VALUEIN:是框架(maptask)要传递给map方法的输入参数的value的数据类型
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * sss 重写Mapper类的map方法,注意方法中的参数类型要跟定义类中声明的泛型参数类型一致
     * @param key   指的是每行文本在文件中的偏移量
     * @param value    表示读取的每行文本
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        value = java hive junit
//        把搜集到的每行数据转化为字符串
        String string = value.toString();

//        把字符串分割,分解成每个单词
        String[] strs = string.split("\t");

        //要把 java hive junit 转化成如下格式
        //java 1
        //hive 1
        //junit 1
        for (String word : strs){
            //通过context把map方法中的数据进行输出(keyout, valueout)
            context.write(new Text(word), new IntWritable(1));
        }
    }


}
