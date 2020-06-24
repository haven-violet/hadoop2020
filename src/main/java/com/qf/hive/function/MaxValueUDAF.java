package com.qf.hive.function;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * 定义一个UDAF自定义函数类,默认要继承于UDAF类
 */
//给当前函数添加描述信息,方便在desc function方法时进行查看
@Description(name="maxInt", value="Find Max Value", extended = "Extended:Find Max Value for all Col")
public class MaxValueUDAF extends UDAF {
    //UDAF要求: 并且包含一个或者多个嵌套的实现了org.apache.hadoop.hive.ql.exec.UDAFEvaluator的静态类
    public static class MaxnumIntUDAFEvaluator implements UDAFEvaluator {
        //在静态类内部定义一个返回值,作为当前UDAF最后的唯一返回值,因为返回值要在hive调用,所以必须要使用序列化类型
        private IntWritable result;

        /**
         * 在初始化时把返回值设为null,避免和上次调用时混淆
         */
        @Override
        public void init() {
            result = null;
        }

        //定义一个函数iterator用来处理遍历多行时,每行值传进来是调用的函数
        public boolean iterate(IntWritable value){
            //把遍历每行的值value传入,和result比较,如果比result大,那么result就设置为value,否则result不变
            if(value == null){
                return true;
            }

            //如果是第一行数据,那么直接给result赋值为第一行数据
            if(result == null){
                result = new IntWritable(value.get());
            }else{
                //给result赋值result和value之间的最大值
                result.set(Math.max(result.get(), value.get()));
            }
            return true;
        }

        /**
         * 在map端进行并行执行后的结果
         * @return
         */
        public IntWritable terminatePartial(){
            return result;
        }

        /**
         * 接收terminatePartial的返回结果,进行数据merge操作,其返回类型为boolean
         * @param other
         * @return
         */
        public boolean merge(IntWritable other){
            return iterate(other);
        }

        /**
         * 将最终的结果返回为Hive
         * @return
         */
        public IntWritable terminate(){
            return result;
        }
    }



}
