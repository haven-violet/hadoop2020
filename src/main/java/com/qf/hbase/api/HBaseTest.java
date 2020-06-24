package com.qf.hbase.api;

import org.junit.Test;

/**
 * 定义一个测试HBase的单元测试类
 */
public class HBaseTest extends SuperHBaseTest {

    @Test
    public void testHBaseEnv() throws Exception{
        System.out.println("hBaseAdmin.tableExists(\"ns1:user\") = " + com.qf.hbase.api.HBaseUtil.hBaseAdmin.tableExists("ns1:user"));

    }
}
