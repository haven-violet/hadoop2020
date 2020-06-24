package com.qf.hbase.phoenix;

import java.sql.*;
import java.util.Properties;


public class PhoenixJdbc {
    //驱动的常量
    private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static void main(String[] args) throws SQLException {
        //selectCols("US_POPULATION");
        //CUR();

        //分页查询,查询第2页,每页显示6条记录
        pagination(1, 6);


    }

    //查询指定表的数据
    public static void selectCols(String table){
        //定义连接、ps、rs
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //加载驱动
            Class.forName(PHOENIX_DRIVER);
            //获取连接
            Properties pro = new Properties();
            //pro.setProperty()
            pro.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
            //获取连接  格式：zk1,zk2,zk3:2181
            conn = DriverManager.getConnection("jdbc:phoenix:hadoop201,hadoop202,hadoop203:2181",
                    pro);
            //查询
            ps = conn.prepareStatement("select * from "+table+"");
            rs = ps.executeQuery();
            //取值
            while (rs.next()){
                System.out.println(rs.getString(1)+"\t"+rs.getString(2)
                +"\t"+rs.getString(3));
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null){
                    rs.close();
                }
                if(ps != null){
                    ps.close();
                }
                if(conn != null){
                    conn.close();
                }

            } catch (SQLException e) {

            }
        }
    }

    //表创建和数据插入和查询
    public static void CUR() throws SQLException {
        Statement stmt = null;
        ResultSet rset = null;
        Connection conn = null;
        try {
            Class.forName(PHOENIX_DRIVER);
            //获取连接
            Properties pro = new Properties();
            //pro.setProperty()
            //pro.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
            //获取连接  格式：zk1,zk2,zk3:2181
            conn = DriverManager.getConnection("jdbc:phoenix:hadoop201,hadoop202,hadoop203:2181",
                    pro);
            stmt = conn.createStatement();

            //执行create语句
            stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
            //执行插入语句
            stmt.executeUpdate("upsert into test values (1,'Hello')");
            stmt.executeUpdate("upsert into test values (2,'qianfeng!')");
            //提交插入数据
            conn.commit();

            PreparedStatement statement = conn.prepareStatement("select * from test");
            rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
            }
            statement.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(conn != null){
                conn.close();
            }
        }
    }

    /**
     * 分页查询
     * @param page
     * @param num
     */
    public static void pagination (int page, int num) {
        //定义连接、ps、rs
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //加载驱动
            Class.forName(PHOENIX_DRIVER);
            //获取连接
            Properties pro = new Properties();
            //pro.setProperty()
            pro.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
            //获取连接  格式：zk1,zk2,zk3:2181
            conn = DriverManager.getConnection("jdbc:phoenix:hadoop201,hadoop202,hadoop203:2181",
                    pro);
            //查询
            ps = conn.prepareStatement("select * from US_POPULATION limit " + num + " offset " + (page - 1)*num);
            rs = ps.executeQuery();
            //取值
            while (rs.next()){
                System.out.println(rs.getString(1)+"\t"+rs.getString(2)
                        +"\t"+rs.getString(3));
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null){
                    rs.close();
                }
                if(ps != null){
                    ps.close();
                }
                if(conn != null){
                    conn.close();
                }
            } catch (SQLException e) {

            }
        }
    }
}
