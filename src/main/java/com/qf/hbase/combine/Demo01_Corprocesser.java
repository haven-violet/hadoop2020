package com.qf.hbase.combine;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 需求: (注意先不要往表中添加数据,协处理器编写好才添加数据测试)
 * 1.向关注表添加数据
 * put 'follows', 'ergouzi', 'cf:star', 'wangbaoqiang'
 * 2.希望通过协处理器,在操作关注表时同时触发粉丝表操作
 * put 'fans', 'wangbaoqiang', 'cf:fensi', 'ergouzi'
 *
 * 自定义类需要继承BaseRegionObserver
 */
public class Demo01_Corprocesser extends BaseRegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //1.获取粉丝的put对象,并解析成key-value
        byte[] rowkey = put.getRow();
        List<Cell> cells = put.get(Bytes.toBytes("cf"), Bytes.toBytes("star"));
        //获取cells中的值
        Cell cell = cells.get(0);
        byte[] bytes = CellUtil.cloneValue(cell);

        //2.往fans表中添加对应的数据
        Put put1 = new Put(bytes);//wangbaoqiang
        put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fensi"), rowkey);//ergouzi

        //3.将put1然后创建表并提交
        try{
            Table fans = HBaseUtil.getTable(TableName.valueOf("fans"));
            fans.put(put1);//将粉丝put对象放到fans
            //关闭
            HBaseUtil.closeTable(fans);
            //HBaseUtil.close();
        } catch(Exception ex){

        }
    }

    
}
