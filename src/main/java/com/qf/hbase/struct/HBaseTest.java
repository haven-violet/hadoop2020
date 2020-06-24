package com.qf.hbase.struct;

import org.junit.Test;

public class HBaseTest {
    @Test
    public void test(){
        //1.HBase的核心原理是一个Map格式的Map<Key,Value>
        //2.HBase中默认是根据rowkey来排序的Map,所以上述结构可以定义为SortedMap<RowKey, Value>
        //3.一个rowKey可以对应多个列簇,所以在顶级Map中的value的类型是List,所以上述结构可以表述为SortedMap<RowKey, List>
        //4.在每一个列簇中多个<列,值>的键值对,所以上面结构可以表述为SortedMap<RowKey, List<Map<列, 值>>>
        //5.在列簇中的键值对默认都是以key排序的,所以在List中放入的Map是SortedMap,上面结构表述为SortedMap<RowKey, List<SortedMap<列, 值>>>
        //6.在<列, 值>中,每个值有多个版本,版本通过时间戳来确定,所以最终的<列, 值>中,值的格式List<值, 时间戳>格式,所以上述结构表述为SortedMap<RowKey, List<SortedMap<列, List<值,时间戳>>>>
        //7.总结: 所以最终用java集合实现的HBase的Map结构是: SortedMap<RowKey, 列簇List<SortedMap<列, 值List<值, 时间戳(版本号)>>>
    }
}
