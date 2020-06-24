package com.qf.hbase.api;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 定义Table的数据操作测试类
 */
public class TableDMLTest extends SuperHBaseTest{
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Test
    public void testPut() throws Exception{
        HBaseUtil.getConnection();
        Table table = com.qf.hbase.api.HBaseUtil.getTable();

        //使用Put对象来封装数据,同理可以用Get对象获取数据,这中间使用类似于命令模式:把命令封装成一个对象

        //构建一个Put对象,并且指定rowkey
        Put put = new Put(Bytes.toBytes("002"));

//        对一个put对象(rowkey)可以插入多列数据
//        插入的数据顺序是: 列蔟, 列名 值
        put.addColumn(Bytes.toBytes("person_info"), Bytes.toBytes("name"), Bytes.toBytes("zhang"));
        //put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("person_info"), Bytes.toBytes("age"), Bytes.toBytes(10));

        //提交put进行数据的插入
        table.put(put);
        logger.info("插入成功");
    }

    /**
     * 演示批量插入
     * @throws Exception
     */
    @Test
    public void testBatchPut() throws Exception {
        //0. 创建集合
        List<Put> list = new ArrayList<>();
        //1. 创建put对象指定行键
        Put rk004 = new Put(Bytes.toBytes("004"));
        Put rk005 = new Put(Bytes.toBytes("005"));
        Put rk006 = new Put(Bytes.toBytes("006"));

        //2. 创建列簇
        rk004.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("gaoyuanyuan"));
        rk005.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        rk005.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("sex"),Bytes.toBytes("2"));
        rk006.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("fanbinbin"));
        rk006.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        rk006.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("sex"),Bytes.toBytes("2"));

        //3. 添加数据
        list.add(rk004);
        list.add(rk005);
        list.add(rk006);

        com.qf.hbase.api.HBaseUtil.getTable().put(list);
        logger.info("批量插入成功");
    }

    /**
     * 查询单个列蔟的值
     * @throws Exception
     */
    @Test
    public void testGetSingle() throws Exception{
//        先要定义Get对象
        Get get = new Get(Bytes.toBytes("002"));

//      通过Table的get方法得到Result(结果对象)
        Result result = com.qf.hbase.api.HBaseUtil.getTable().get(get);

        //得到单个列蔟的Map值
        NavigableMap<byte[], byte[]> base_info = result.getFamilyMap(Bytes.toBytes("base_info"));

        //遍历列蔟中的Map集合,输出所有的列键值对
        Set<byte[]> bytes = base_info.keySet();
        for (byte[] aByte : bytes) {
            logger.info("key="+new String(aByte));
            logger.info("value="+new String(base_info.get(aByte)));
        }
    }
    /**
     * 得到表中所有列蔟的值
     */
    @Test
    public void testGetAll() throws Exception{
        //根据rowkey得到result对象
        Get get = new Get(Bytes.toBytes("002"));
        Result result = com.qf.hbase.api.HBaseUtil.getTable().get(get);

//        通过Result得到Cell的扫描器,默认要遍历所有Cell
        CellScanner cellScanner = result.cellScanner();

//        遍历所有Cell
        while (cellScanner.advance()) {
//            得到当前cell
            Cell cell = cellScanner.current();
//            如果直接输出列蔟,列,还有值,默认是三个合在一起输出,怎么解决? 使用偏移量和长度进行切割
            logger.info(new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()));
            logger.info(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
            logger.info(new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        }
    }

    /**
     * 查询所有数据使用clone方式获取数据
     * @throws Exception
     */
    @Test
    public void testGetAllDataByClone() throws Exception{
        //根据rowkey得到result对象
        Get get = new Get(Bytes.toBytes("002"));
        Result result = com.qf.hbase.api.HBaseUtil.getTable().get(get);
        HBaseUtil.showResult(result);

    }

    /**
     * 演示批量查找:思路是把多个get放到List中,并且传入到Table中即可
     * @throws Exception
     */
    @Test
    public void testBatchGet() throws Exception{
        //1. 创建集合存储get对象
        List<Get> getList = new ArrayList<>();
        //2. 创建多个get对象
        Get get = new Get(Bytes.toBytes("001"));

        Get get1 = new Get(Bytes.toBytes("002"));
        get1.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"));
        get1.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"));

        Get get3 = new Get(Bytes.toBytes("004"));
        //3. 添加get对象到集合中
        getList.add(get);
        getList.add(get1);
        getList.add(get3);

//        如果要查询多条记录,那么可以传入get的列表,返回对应Result的数组
        Result[] results = com.qf.hbase.api.HBaseUtil.getTable().get(getList);

//        遍历所有result,打印出结果
        for (Result result : results) {
//            通过result可以拿到RowKey
            HBaseUtil.showResult(result);
        }
    }
    /**
     * 查询Scan扫描的数据
     * @throws Exception
     */
    @Test
    public void testScan() throws Exception{
        Scan scan = new Scan();

        //!!!注意:在查询时候一定要设置条件,包括myslq,hive,hbase,redis都一样,如果数据量巨大,容易造成服务器或本机挂机
        // star stop 查询包头不包尾(前闭后开),可以小技巧实现包头包尾,前闭后闭
        scan.setStartRow(Bytes.toBytes("001"));
        scan.setStopRow(Bytes.toBytes("004"+"\001"));

//      给scan添加列的过滤
        scan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"));


//      通过Table对象返回Scanner
        ResultScanner scanner = com.qf.hbase.api.HBaseUtil.getTable().getScanner(scan);
//      通过scan得到当前scan所有的result迭代器对象
        Iterator<Result> iterator = scanner.iterator();

        //遍历所有result
        while (iterator.hasNext()) {
            Result result = iterator.next();
            HBaseUtil.showResult(result);
        }
    }

    /**
     * 删除单列
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        Delete delete = new Delete(Bytes.toBytes("004"));
        com.qf.hbase.api.HBaseUtil.getTable().delete(delete);
    }

    /**
     * 演示了多行的删除
     * @throws Exception
     */
    @Test
    public void testDeleteAll() throws Exception {
        List<Delete> list = new ArrayList<>();
        Delete delete = new Delete(Bytes.toBytes("005"));
        list.add(delete);
        com.qf.hbase.api.HBaseUtil.getTable().delete(list);
    }

    /**
     * 删除某列
     * @throws Exception
     */
    @Test
    public void testDeleteByColumn() throws Exception{
        Delete delete = new Delete((Bytes.toBytes("001")));
//      指定要删除的列蔟和列
//      !!! 重要:如果只是用addColumn指定要删除的列,在有多个版本的情况下那么它默认只删除最新一行,其余行不会删除,容易造成数据混乱
//        delete.addColumn(Bytes.toBytes("newFN_info"), Bytes.toBytes("name"));

//        那么怎样才能同时删除一个列的所有记录(正常业务)? 使用addColumns进行删除,强烈建议使用addColumns添加列进行删除
        delete.addColumns(Bytes.toBytes("base_info"), Bytes.toBytes("username"));

//        重新通过连接对象获取HBase中versions大于1的表
        Table userInfo = com.qf.hbase.api.HBaseUtil.connection.getTable(TableName.valueOf("ns1:user_info"));
        userInfo.delete(delete);
    }

    //   因为每次测试都会改变数据(插入或删除),那么这样对单元测试的结果都会有一定的影响
//   所以为了保证每次单元测试的数据的准确性和一致性,所有要在每次执行单元测试之前要把数据统一,每次测试之后要把数据进行统一清理
    @Before
    public void  init() throws Exception{
        //需求:每个单元测试方法之前,hbase中a表有1条数据,3列数据,那么这个数据准备就在init方法(Before修饰)
        Table table = com.qf.hbase.api.HBaseUtil.getTable();

        //使用Put对象来封装数据,同理可以用Get对象获取数据,这中间使用类似于命令模式:把命令封装成一个对象

        //构建一个Put对象,并且指定rowkey
        Put put = new Put(Bytes.toBytes("006"));

//        对一个put对象(rowkey)可以插入多列数据
//        插入的数据顺序是: 列蔟, 列名 值
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("zhang"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("30"));

        //提交put进行数据的插入
//        table.put(put);
        logger.error("模拟初始化数据");
    }

    //因为每个单元测试方法(查询除外),都有可能对HBase中的数据产生影响,所以在每个单元方法执行之后,都要清理测试产生的数据
//    那么这个清理就写在destory方法中(@After注解修饰的方法中)
    @After
    public void destory() throws Exception {
        //需求:清理每次产生的测试数据,下面是伪代码
        List<Delete> list = new ArrayList<>();
        Delete delete = new Delete(Bytes.toBytes("006"));
        list.add(delete);
        HBaseUtil.getTable().delete(list);
        logger.warn("模拟数据清理成功");
    }
}
