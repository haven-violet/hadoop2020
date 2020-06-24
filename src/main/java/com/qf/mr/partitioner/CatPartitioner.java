package com.qf.mr.partitioner;

import com.qf.mr.seri.Cat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义一个分区类,默认传入的泛型是Mapper输出的Key,Value
 */
public class CatPartitioner extends Partitioner<Cat, NullWritable> {
    //定义一个数据字典的属性,把所属手机号和分区号对应起来
    private static Map<String, Integer> numberMap = new HashMap<>();

    //初始化手机号和分区的对应关系
    static {
        numberMap.put("133", 0);
        numberMap.put("136", 1);
        numberMap.put("138", 2);
    }

    @Override
    public int getPartition(Cat cat, NullWritable nullWritable, int numPartitions) {
        String number = cat.getNumber();
        String subStr = number.substring(0, 3);
        Integer result = numberMap.get(subStr);

        //如果手机号不在初始化分区号内,则将该分区号设置为4
        if(result == null){
            result = 3;
        }
        return result;
    }
}
