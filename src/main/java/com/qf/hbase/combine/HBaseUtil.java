package com.qf.hbase.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseUtil {

    public static Admin hBaseAdmin;
    public static Connection connection = null;
    public static Configuration configuration = null;

    //定一个日志对象字段,用来记录当前类中的所有日志信息,另外多个对象可以共享当前这个日志对象,所以对象为static类型
    //static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 获取HBaseAdmin对象
     * @throws IOException
     */
    public static void getHBaseAdmin() throws IOException {
        //3.2在HBase新的api中,使用连接对象来创建HBaseAdmin,降低了耦合度,提高了扩展性
        //单列 饿汉模式
        if(hBaseAdmin == null){
            hBaseAdmin = connection.getAdmin();
            //logger.info("HBaseAdmin\t创建成功");
        }
    }

    public static Admin getAdmin() throws IOException {
        //3.2在HBase新的api中,使用连接对象来创建HBaseAdmin,降低了耦合度,提高了扩展性
        getConnection();  //初始化
        //单列 饿汉模式
        if(hBaseAdmin == null){
            hBaseAdmin = connection.getAdmin();

        }
        return hBaseAdmin;
    }

    /**
     * 关闭资源对象
     * @throws IOException
     */
    public static void close() throws IOException {
        //5.关闭连接资源
        if (hBaseAdmin != null) {
            hBaseAdmin.close();
            //logger.info("HBaseAdmin\t资源成功关闭");
        }
        if (connection != null) {
            connection.close();
            //logger.info("Connection\t资源成功关闭");
        }
    }

    /**
     * 获取连接资源
     * @throws IOException
     */
    public static void getConnection() throws IOException {
        //连接HBase的API步骤
        //饿汉模式
        if(connection == null){
            //1.构建一个Configuration配置文件对象
            configuration = new Configuration();
            //2.配置HBase的zookeeper的地址 hbase.zookeeper.quorum
            configuration.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
            //3.1通过ConnectionFactory对象创建一个连接，获得连接对象
            connection = ConnectionFactory.createConnection(configuration);
            //logger.info("Connection\t连接创建成功");
        }
    }

    /**
     * 获得HBase的表管理对象
     * @return
     * @throws Exception
     */
    public static Table getTable(TableName tableName) throws Exception{
        //  hBaseAdmin,顾名思义是用来管理表结构,对表数据不能做操作,如果我们要处理表中的数据,那么我们要使用Table
        //  Table是用来管理表中的数据,一般用工厂模式获取
        //饿汉模式
        if(connection == null){
            //1.构建一个Configuration配置文件对象
            configuration = new Configuration();
            //2.配置HBase的zookeeper的地址 hbase.zookeeper.quorum
            configuration.set("hbase.zookeeper.quorum", "hadoop201:2181,hadoop202:2181,hadoop203:2181");
            //3.1通过ConnectionFactory对象创建一个连接，获得连接对象
            connection = ConnectionFactory.createConnection(configuration);
            //logger.info("Connection\t连接创建成功");
        }
        Table table = connection.getTable(tableName);
        return table;
    }

    /**
    *@Author 东哥
    *@Company 千锋好程序员大数据
    *@Description
    **/
    public static void closeTable(Table table) throws IOException {
        //5.关闭连接资源
        if (table != null) {
            table.close();
        }
    }

    public static void main(String[] args) throws Exception {
        //System.out.println(getTable(TableName.valueOf("fans")));
        System.out.println(getAdmin());
    }
}
