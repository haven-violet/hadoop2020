package com.qf.hbase.api;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 定义一个类,用来测试HBase中的表的DDL操作
 */
public class TableDDLTest extends SuperHBaseTest {
    //定义一个通用的日志记录器,如果当前对象不是频繁创建可以,如果频繁创建,那一定要用static
    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * create 't2', {NAME => 'default', VERSIONS => 1}
     * 创建一张表
     * @throws Exception
     */
    @Test
    public void testCreateTable() throws IOException {
//        按照Namespace的规则,创建表之前先要定义表的描述符
//       因为相同的表名在数据库中只有一张表,但是如果通过new,可以有多个对象,那么就用工厂方法valueOf来获取唯一表名
        TableName qftable = TableName.valueOf("qftable001");

        HTableDescriptor hTableDescriptor=new HTableDescriptor(qftable);
        hTableDescriptor.addCoprocessor("com.qf.hbase.combine.Demo01_Corprocesser");

        //创建一个列蔟
       // HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("base_info");
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf");

        //可以给当前列蔟设置属性
        hColumnDescriptor.setVersions(1, 3); //设置最大,最小版本号
        hColumnDescriptor.setTimeToLive(24 * 60 * 60);//设置数据存活时间

        //HColumnDescriptor hColumnDescriptorExtra=new HColumnDescriptor("extra_info");

//     把创建好的列蔟添加到表中
        hTableDescriptor.addFamily(hColumnDescriptor);
        //hTableDescriptor.addFamily(hColumnDescriptorExtra);

        try {
            com.qf.hbase.api.HBaseUtil.hBaseAdmin.createTable(hTableDescriptor);
        } catch (IOException e) {
            //演示在catch一定要用error去记录错误信息
            logger.error("表创建错误 错误信息是: "+e.getMessage()+"\n",e);
        }
    }
    /**
     * 修改HBase表结构:后面添加的列蔟会覆盖前面的列蔟
     */
    @Test
    public void testModifyTable() throws Exception{
//        创建一个一个表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("qftable"));

//        创建列蔟
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("newColumn");

//        把列蔟添加到表描述器中
        hTableDescriptor.addFamily(columnDescriptor);

//        修改表结构
//      !!!重要提示 如果当前的表描述器是新建,那么这时候通过修改表描述器来改变列蔟,后面新加的列蔟会覆盖前面原来的列蔟
        com.qf.hbase.api.HBaseUtil.hBaseAdmin.modifyTable(TableName.valueOf("qftable"),hTableDescriptor);

    }
    /**
     * 修改HBase表结构:后面添加的列蔟会不会覆盖原来的列蔟
     */
    @Test
    public void testModifyTableNotOverwrite() throws Exception{
//        先要把原来的数据库中的表描述器拿到,然后再在上面修改列蔟,这样后面添加的列蔟就不会覆盖前面的列蔟
        HTableDescriptor qftable = com.qf.hbase.api.HBaseUtil.hBaseAdmin.getTableDescriptor(TableName.valueOf("qftable"));

//        创建一个列蔟描述器
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("newColumn123");

//        把列蔟描述器加入到表描述器,实现后面的列蔟添加不会覆盖前面
        qftable.addFamily(hColumnDescriptor);

//        提交修改
        com.qf.hbase.api.HBaseUtil.hBaseAdmin.modifyTable(TableName.valueOf("qftable"),qftable);
    }

    /**
     * 直接删除列蔟
     * @throws Exception
     */
    @Test
    public void testDeleteFamily() throws Exception{
        com.qf.hbase.api.HBaseUtil.hBaseAdmin.deleteColumn(TableName.valueOf("qftable"),"newColumn".getBytes());
    }

    /**
     * 通过修改表结构来删除列蔟
     * @throws Exception
     */
    @Test
    public void testDeleteFamilyFromTable() throws Exception{
//        先拿到表描述器
        HTableDescriptor qftable = com.qf.hbase.api.HBaseUtil.hBaseAdmin.getTableDescriptor(TableName.valueOf("qftable"));

        //通过表描述器删除列蔟
        HColumnDescriptor hColumnDescriptor = qftable.removeFamily("newColumn123".getBytes());

//      提交修改表结构
        com.qf.hbase.api.HBaseUtil.hBaseAdmin.modifyTable(TableName.valueOf("qftable"),qftable);
    }

    /**
     * 列出Table中所有列蔟
     */
    @Test
    public void testListFamily() throws Exception {
        HTableDescriptor qftable = com.qf.hbase.api.HBaseUtil.hBaseAdmin.getTableDescriptor(TableName.valueOf("qftable"));

//        通过表描述器拿到所有列蔟
        HColumnDescriptor[] columnFamilies = qftable.getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            //使用logger来代替System.out作为标准信息删除
            logger.info("columnFamily = " + columnFamily);
            logger.info("columnFamily.getMaxVersions() = " + columnFamily.getMaxVersions());
        }
    }

    /**
     * 删除表
     * @throws Exception
     */
    @Test
    public void testDelete() throws IOException {
        String deletedTableName = "qftable";
        boolean tableExists = com.qf.hbase.api.HBaseUtil.hBaseAdmin.tableExists(deletedTableName);
        if (!tableExists) {
//          演示了logger.warn用法,用户输入有逻辑错误,警告用户
            logger.warn("你要删除的表不存在:"+deletedTableName);
            return ;
        }

        if (!com.qf.hbase.api.HBaseUtil.hBaseAdmin.isTableDisabled(TableName.valueOf(deletedTableName))) {
            logger.warn("表没有被禁用,现在disable...");
            com.qf.hbase.api.HBaseUtil.hBaseAdmin.disableTable(TableName.valueOf(deletedTableName));
        }


        try {
            com.qf.hbase.api.HBaseUtil.hBaseAdmin.deleteTable(TableName.valueOf(deletedTableName));
            logger.info("表删除成功");
        } catch (IOException e) {
            logger.error("删除表错误"+e);
        }

    }
}
