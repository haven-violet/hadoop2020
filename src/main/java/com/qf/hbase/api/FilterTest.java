package com.qf.hbase.api;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * 创建一个测试类,用来进行过滤器的测试
 */
public class FilterTest extends SuperHBaseTest{

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 需求: 模拟实现需求
     *  select * from qftable where age <= 18 and name = zhang
     */
    @Test
    public void testListFilter() throws Exception {
        //1.创建过滤器链
            //1.1过滤器中间的连接操作符 MUST_PASS_ALL等价于and
            //1.2MUST_PASS_ONE 等价于 or
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //2.构建查询条件
            //2.1创建查询条件 age <= 18   默认传入参数是
        SingleColumnValueFilter ageFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("10"));

            //2.2创建查询条件 name = zhang
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("qf"));

            //2.3 如果在查询的时候某个row没有上述列名,那么默认这个row的数据不会被过滤,所有要设缺失值为true,让没有列名的值不会输出
        ageFilter.setFilterIfMissing(true);
        nameFilter.setFilterIfMissing(true);

        //3.把查询条件加入到过滤器中
        filterList.addFilter(ageFilter);
        filterList.addFilter(nameFilter);

        //4.创建表扫描器进行扫描
        Scan scan = new Scan();

        //5.把过滤器链关联到扫描器
        scan.setFilter(filterList);

        //6.得到表对象
        Table table = HBaseUtil.getTable();

        //7.扫描表
        ResultScanner scanner = table.getScanner(scan);

        //8.遍历,打印表数据
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result next = iterator.next();
            HBaseUtil.showResult(next);
        }
    }


    /**
     * 写一个测试单个的SingleColumnValueFilter的伪代码测试
     */
    @Test
    public void testSingleColumnValueFilter() {
        SingleColumnValueFilter ageFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("18"));
        Scan scan = new Scan();
        scan.setFilter(ageFilter);
        //以下伪代码略,同上
    }

    /**
     * 测试正则表达式比较器,处理下面需求
     * select * from qftable where name like '%han%'
     */
    @Test
    public void testRegexStringComparator() throws Exception {
        //1.先创建一个正则表达式比较器,因为下面的SingleColumnValueFilter要使用,参数就是正则表达式,用来实现类似like语法
        RegexStringComparator regexStringComparator = new RegexStringComparator("[$a-z]han[$a-z]");

        //2.创建单列值过滤器,参数为:
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, regexStringComparator);

        //3.打印过滤器的值
        showFilterResult(nameFilter);

    }

    /**
     * 使用SubString比较器,实现sql中的like功能,比正则表达式速度要快,如果仅仅是字符串比较是否包含,优先考虑SubString
     * @throws Exception
     */
    @Test
    public void testSubString() throws Exception {
        SubstringComparator substringComparator = new SubstringComparator("an");
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, substringComparator);

        showFilterResult(nameFilter);
    }

    /**
     * 测试二进制比较器,因为HBase底层都是二进制存储,所以在数据量比较大,遍历比较多的时候,可以优先考虑二进制比较器,省略了转化的时间
     * 需求: select * from ns1_userinfo where name = ''
     */
    @Test
    public void testBinaryComparator() throws Exception {
        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("zhang"));
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, binaryComparator);

        showFilterResult(nameFilter);
    }

    /**
     * 测试二进制前缀比较器,如果数据量巨大,要求性能比较高,优先考虑二进制前缀比较器
     * 需求: select * from qftable where name like 'zh%'
     */
    @Test
    public void binaryPrefixComparatorTest() throws Exception {
        //1.创建比较器,正则: 以zh开头的
        BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes("zh"));
        //2.获取单列值过滤器
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("person_info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);

        //3.设置缺失
        nameFilter.setFilterIfMissing(true);

        //4.打印
        showFilterResult(nameFilter);
    }

    /**
     * 测试列簇过滤器,用来测试以列簇为条件查询的数据
     * select * from qftable where 列簇 like 'base%'
     */
    @Test
    public void testFamilyFilter() throws Exception {
        //1.构建一个正则比较器
        RegexStringComparator regexStringComparator = new RegexStringComparator("^person");

        //2.构建列簇过滤器
        //CompareOp familyCompareOp, ByteArrayComparable familyComparator
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, regexStringComparator);

        //3.打印数据
        showFilterResult(familyFilter);
    }

    /**
     * 以SubString的方式查询包含base的列簇
     * select * from qftable where 列簇 like '%base%'
     */
    @Test
    public void testColumnFamily2() throws Exception {
        //1.创建SubString比较器: 找包含base开头的字符串
        SubstringComparator substringComparator = new SubstringComparator("base");
        //2.创建FamilyFilter
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        //3.打印
        showFilterResult(familyFilter);
    }

    /**
     * 测试列名过滤器
     * 需求: 查询包含am的列名的值
     */
    @Test
    public void testQualifierFilter() throws Exception {
        //定义一个SubString过滤器
        SubstringComparator substringComparator = new SubstringComparator("am");
        //CompareOp op, ByteArrayComparable qualifierComparator
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);

        //使用上面编写的通用显示过滤器结果函数打印
        showFilterResult(qualifierFilter);
    }

    /**
     * 测试以a为列前缀的数据
     */
    @Test
    public void testProfixFilter() throws Exception {
        //定义前缀过滤器,小而美的定义
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("a"));

        //根据过滤器打印数据
        showFilterResult(columnPrefixFilter);
    }

    /**
     * 测试以多个列前缀a,或者n开头的数据
     */
    @Test
    public void testMultipleColumnPrefixFilter() throws Exception {
        //1.定义二进制前缀的数组,可以理解为byte数组的数组
        byte[][] prefixs = new byte[][]{Bytes.toBytes("a"), Bytes.toBytes("n")};


        //2.定义一个多列前缀过滤器,并且传入多列数组
        MultipleColumnPrefixFilter multipleColumnPrefixFilter1 = new MultipleColumnPrefixFilter(prefixs);

        //3.查询结果
        showFilterResult(multipleColumnPrefixFilter1);
    }

    /**
     * 查询以"age" 到 "name"的列的信息
     */
    @Test
    public void testColumnRangeFilter() throws Exception {
        //定义列的范围过滤器,列后面的参数如果是true,就是包含,false,就是不包含
        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes("age"), true, Bytes.toBytes("name"), false);

        //显示结果
        showFilterResult(columnRangeFilter);
    }

    /**
     * 定义row过滤器,虽然可以用get,或则Scan实现Rowkey查询的功能,但是很多时候为了代码或者API的统一性,用RowFilter进行统一查询
     */
    @Test
    public void testRowFilter() throws Exception {


        //1.定义一个二进制比较器进行row的比较
        BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes("002"));

        //2.定义Row过滤器CompareOp rowCompareOp, ByteArrayComparable rowComparator
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, binaryComparator);

        //3.
        showFilterResult(rowFilter);
    }

    /**
     * 查找指定表中的所有的行键的第一个列
     */
    @Test
    public void test() throws Exception {
        //1.创建所有行键第一列的过滤器
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();

        //2.显示结果
        showFilterResult(firstKeyOnlyFilter);
    }

    /**
     * 需求: 完成HBase的分页实现: 每页显示三条记录,把所有记录都显示
     * 原理: 因为HBase是海量数据,不能快速定位到第几条记录,定义记录都是通过rowkey来获取,所有分页都是基于rowkey来分页,而不是第几条记录来分页
     * 问题: HBase存储数据有可能不在同一Region或者store中,所有使用Hbase自带的分页有可能数据不准确(跨节点)
     * 思路:
     *      第一页 select * from qfuser where rowkey = \001 limit 3
     *      下一页 select * from qfuser where rowkey =上一页的MaxRowKey    limit3
     *      什么时候结束？ 在每一页中放一个统计值,当统计值小于3的时候,说明数据到了末尾就可以停止
     *      while(true)
     *      {
     *          当每页的统计值小于3的时候停止
     *      }
     *
     */
    @Test
    public void testPageFilter() throws Exception {
        //1.定义一个分页过滤器,设置每页显示3条记录
        PageFilter pageFilter = new PageFilter(3);

        //2.构建扫描器
        Scan scan = new Scan();

        //3.给扫描器设定过滤器
        scan.setFilter(pageFilter);

        //4.得到表
        Table table = HBaseUtil.getTable();

        //定义一个rowkey值,用来记录每次扫描的rowkey最大值
        byte[] maxRowKey = null;

        //通过while循环,显示所有数据,然后每页显示3条
        while(true){
            //定义一个计数器count,因为默认每次分页是三条记录,所有每次的count累加为3,如果小于3,那么说明就到最后
            int count = 0;

            //因为每次通过扫描都是三条记录,所有每次进入都要不断的扫描

            //对每次扫描的结果进行遍历,默认是3条
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while(iterator.hasNext()){
                //得到每次扫描的结果
                Result result = iterator.next();
                String rowstring = new String(result.getRow());

                //打印每次的行键
                logger.info(rowstring);

                //对每次的扫描结果进行统计,如果数据小于3,说明数据已经到末尾
                count++;

                //在每次遍历的时候记录maxRowkey
                maxRowKey = result.getRow();
            }

            //说明数据到了末尾,就退出
            if(count < 3) {
                break;
            }

            //为了每次扫描都是后三条记录,所以每次扫描以后设定下次扫描行键的开始就是上次扫描的最大值
            scan.setStartRow(Bytes.toBytes(new String(maxRowKey) + "\001"));
        }

    }
}
