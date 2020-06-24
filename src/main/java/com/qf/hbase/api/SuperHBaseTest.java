package com.qf.hbase.api;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SuperHBaseTest {
    private  static Logger logger = LoggerFactory.getLogger(SuperHBaseTest.class);
    protected static Connection connection;
    protected static HBaseAdmin hBaseAdmin;

    /**
     * 定义一个方法,用来初始化连接对象Connection和HBaseAdmin,因为在多个方法之前只执行一次,那么这个方法就是静态方法
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception{

//      把原来的得到连接对象和admin抽取成方法
        connection = com.qf.hbase.api.HBaseUtil.getConnection();
        hBaseAdmin = com.qf.hbase.api.HBaseUtil.getHBaseAdmin();

    }

    /**
     * 在所有测试方法执行完成以后,最后需要关闭一次HBaseAdmin和connection对象,那么这个关闭对象可以写在静态方法afterClass中
     * @throws Exception
     */
    @AfterClass
    public static void afterClass()  {
        HBaseUtil.closeTable();
        HBaseUtil.closeAdminAndConnection();

    }

    public static Logger getLogger() {
        return logger;
    }

    protected void showResult(Result result) throws IOException {
        //      通过result可以拿到RowKey

        HBaseUtil.showResult(result);
    }

    public static void showFilterResult(Filter filter) throws Exception {
        HBaseUtil.showFilterResult(filter);
    }
}
