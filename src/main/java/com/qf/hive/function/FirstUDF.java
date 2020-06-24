package com.qf.hive.function;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 定义第一个hive自定义函数,默认继承于UDF类
 */
public class FirstUDF extends UDF {

//    需要在自定义函数中写一个evaluate(),因为本方法的传参和类型都不确定,所以此方法不是覆盖父类的方法
//    因为hive的操作本质上转化为MR,所以在传参和返回值的时候数据类型一定要hive序列化的才可以
//    当前函数的调用机制是hive通过回调机制来调用
    public String evaluate(String param){
        if(param == null){
            return "";
        }
//        把当前字符串转化为大写
        String s = param.toUpperCase();

        return s;
    }

    
}


