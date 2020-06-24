package com.qf.hbase.combine;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Homework_day03 extends BaseRegionObserver {
    //创建c1表，然后使用协处理器，对get的数据操作时，将存在的任意列的值拼接上"_qianfeng"
    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        List<Cell> tempCells = new ArrayList<>();
        //遍历结果集<列簇, 列名, 列值>
        for (int i = 0; i < results.size(); i++) {
            Cell cell = results.get(i);
            byte[] value = CellUtil.cloneValue(cell);
            value = Bytes.toBytes(new String(value) + "_qianfeng");
            KeyValue keyValue = new KeyValue(get.getRow(), CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), value);
            results.remove(cell);
            i--;
            tempCells.add(keyValue);
        }
        for (Cell tempCell : tempCells) {
            results.add(tempCell);
        }
    }
}
