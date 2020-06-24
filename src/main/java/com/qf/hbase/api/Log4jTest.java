package com.qf.hbase.api;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log4j的测试类
 * 1.定义Log日志对象,因为默认是Log4j作为日志类实现,但是为了扩展性,定义使用SLF4j来定义Logger
 */
public class Log4jTest {
    //定一个日志对象字段,用来记录当前类中的所有日志信息,另外多个对象可以共享当前这个日志对象,所以对象为static类型
    private static Logger logger = LoggerFactory.getLogger(Log4jTest.class);

    @Test
    public void testLevel() throws Exception {
        //演示通过logger进行不同级别的输出
        logger.trace("trace ------------我是基本跟踪的输出,");
        logger.debug("debug  -----------我是调试信息的输出");
        logger.info("info   ------------我是日常信息的输出");
        logger.warn(" warn   -----------我是警告信息的输出");
        logger.error("error ------------我是错误级别的输出,");
    }
}
