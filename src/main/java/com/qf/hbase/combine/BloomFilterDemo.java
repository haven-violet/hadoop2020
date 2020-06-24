package com.qf.hbase.combine;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.commons.lang3.StringUtils;

public class BloomFilterDemo {

    public static void main(String[] args){
        BloomFilterDemo bloomFilterDemo = new BloomFilterDemo();

        //判断
        System.out.println(bloomFilterDemo.contains("100"));
        System.out.println(bloomFilterDemo.contains("200"));
        System.out.println(bloomFilterDemo.contains("100"));

    }


    //定义一个bloom过滤器
    private final BloomFilter<String> bloomFilter = BloomFilter.create(new Funnel<String>() {
        @Override
        public void funnel(String s, PrimitiveSink primitiveSink) {
            primitiveSink.putString(s, Charsets.UTF_8);
        }
    }, 1024*1024*32);


    /**
     * 判断元素是否包含
     * @return
     */
    public synchronized boolean contains(String id){
        //先判断是否为空
        if(StringUtils.isEmpty(id)){
            return true;
        }

        //布隆过滤器是否包含这个id
        boolean exists = bloomFilter.mightContain(id);
        if(!exists){
            bloomFilter.put(id);
        }
        return exists;
    }
}
