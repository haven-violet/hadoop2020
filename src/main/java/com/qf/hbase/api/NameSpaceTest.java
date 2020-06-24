package com.qf.hbase.api;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

/**
 * 定义一个NameSpace的测试类,通过继承SuperHBaseTest直接可以获取Connection和HBaseAdmin对象
 */
public class NameSpaceTest extends SuperHBaseTest {
    @Test
    public void testCreateNameSpace() throws Exception {
       // 创建NameSpace先要创建一个namespaceDescriptor对象,使用build模式构建
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("qfNamespace").build();

//        使用HBaseAdmin创建NameSpace
        com.qf.hbase.api.HBaseUtil.hBaseAdmin.createNamespace(namespaceDescriptor);
        System.out.println("创建Namespace成功");
    }

    @Test
    public void testListNameSpace() throws Exception {
        //得到当前HBase中的所有NameSpace,返回是Namespace描述器的数组对象
        NamespaceDescriptor[] namespaceDescriptors = com.qf.hbase.api.HBaseUtil.hBaseAdmin.listNamespaceDescriptors();

//        遍历打印所有NameSpace
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println("namespaceDescriptor.getName() = " + namespaceDescriptor.getName());
        }
    }

    /**
     * 查看当前NameSapce中的表
     */

    @Test
    public void testListNameSpaceTables() throws Exception {
        TableName[] ns1s = com.qf.hbase.api.HBaseUtil.hBaseAdmin.listTableNamesByNamespace("ns1");
        for (TableName ns1 : ns1s) {
            System.out.println("ns1.getName() = " + ns1.getNameAsString());
        }
    }

    /*
    * 列出所有NameSpace中的表
     */
    @Test
    public void testListAllNameSpaceTables() throws Exception{
        TableName[] tableNames = com.qf.hbase.api.HBaseUtil.hBaseAdmin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println("tableName = " + tableName);
        }
    }

    /**
     * 删除NameSpace
     */
    @Test
    public void testNameSpace() throws Exception {
        com.qf.hbase.api.HBaseUtil.hBaseAdmin.deleteNamespace("qfNamespace");
    }
}
