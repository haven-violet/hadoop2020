package com.qf.zk.oberver;

/**
 * 定义观察者模式的多方,当一发生变化时,会调用同一的方法(process)通知多方
 */
public interface Observer {
    void process(String event);
}
