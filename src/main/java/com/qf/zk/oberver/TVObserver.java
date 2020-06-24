package com.qf.zk.oberver;

public class TVObserver implements Observer {
    public void process(String event) {
        System.out.println("我是电视观察者: " + event + "我做了yyy");
    }
}
