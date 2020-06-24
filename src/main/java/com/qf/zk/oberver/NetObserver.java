package com.qf.zk.oberver;

public class NetObserver implements Observer {

    public void process(String event) {
        System.out.println("我是网络观察者:  " + event + "我是做了XXX");
    }
}
