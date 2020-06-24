package com.qf.hive.function;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 定义一个UDTF的自定义函数(一对多),默认要继承于GenericUDTF
 */
public class ParseMapUDTF extends GenericUDTF {

//    定义一个Logger对象,用来代替System.out
    //slf4j 更广泛, log4j是其子类
    public static Logger logger = LoggerFactory.getLogger(ParseMapUDTF.class);


//在initializez中初始化要输出字段的名称和类型
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
//        定义要输出列的名字的List,并且要添加输出的列名
        List<String> structFieldNames = new ArrayList<>();
        structFieldNames.add("key");
        structFieldNames.add("value");
        
//        定义要输出列的类型的List,并且添加要输出列的类型
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

//    process方法用来处理输入的每行数据,每行数据处理调用一次process,类似于Mapper中的Map方法
    @Override
    public void process(Object[] objects)  {
//        得到第一个参数,转化为字符串,类似于->, name:zhang;age:20;address:shenzhen
        String inputString = objects[0].toString();
        
//        把上述例子字符串按照分号，切分为数组
        String[] split = inputString.split(";");
        
//        s=name:zhang
        for (String s : split) {
//            把每个切分后的key value分开
            String[] kvArray = s.split(":");
//            如果产生多列可以将多个列的值放在一个数组中,然后将该数组传入到forward()函数
            try {
                forward(kvArray);
            } catch (HiveException e) {
//                在实际项目中不能直接打印printStackTrace
//                e.printStackTrace();
//                或者不能写任何System.out的子句,性能非常低(内存速度是 10 - 100 倍硬盘速度)
//                在所有项目中,所有的日志输出都要用logger对象的输出,由配置文件来决定logger输出到哪里(可以是文件,也可以是控制台)
                logger.error("forward函数出错,出错的具体信息是", e);
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) {
        logger.info("这是提示信息");
        logger.warn("这是警告信息");
        logger.error("这是错误信息");
    }
}
