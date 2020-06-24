package com.qf.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

public class HdfsTest {
    @Test
    public void testGetFileSystem() throws IOException {
        //创建配置对象,用于加载配置信息(四个默认配置文件: core-default.xml, hdfs-default.xml, mapred-default.xml, yarn-default.xml)
        Configuration conf = new Configuration();
        //修改fs.defaultFS属性的值
        conf.set("fs.default", "hdfs:");
    }


}
