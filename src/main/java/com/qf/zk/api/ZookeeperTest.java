package com.qf.zk.api;

import org.apache.zookeeper.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * 写一个简单的Zookeeper的api测试类
 */
public class ZookeeperTest {

    private static ZooKeeper zookeeper;

    /**
     * 初始化Zookeeper对象
     * @throws IOException
     */
    @BeforeClass
    public static void init() throws IOException {
        //新建一个ZK客户端,传入连接字符串,超过时间,默认的观察者对象
        zookeeper = new ZooKeeper("hadoop102:2181", 20000, new Watcher(){

            public void process(WatchedEvent event) {
                System.out.println("event = " + event);
            }
        });
    }

    /**
     * 通过zk客户端创建zk节点的数据
     * @throws Exception
     */
    @Test
    public void testCreateNode() throws Exception {

        zookeeper.create("/myRoot", "myRootData".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 通过zk客户端得到zk节点的数据
     * @throws Exception
     */
    @Test
    public void testGetData() throws Exception {
        //从zk客户端查询的结果也是字节数组
        byte[] data = zookeeper.getData("/myRoot", true, null);
        String s = new String(data);
        System.out.println("s = " + s);
    }

    /**
     * 通过zk客户端删除一个节点
     */
    @Test
    public void testDeleteNode() throws Exception {
        //delete可以删除一个节点,第一参数是删除的路径,第二个参数是数据版本号,如果是-1就表示都删除
        zookeeper.delete("/myRoot", -1);
        System.out.println("删除成功");
    }

    /**
     * 因为zookeeper是外边资源,不在jvm的堆中,所以使用完了为了防止内存泄漏,一定要在@AfterClass手工关闭
     * @throws Exception
     */
    @AfterClass
    public static void close() throws Exception {
        zookeeper.close();
    }
}
