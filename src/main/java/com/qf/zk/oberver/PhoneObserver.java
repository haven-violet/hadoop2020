package com.qf.zk.oberver;

public class PhoneObserver implements Observer {
    public void process(String event) {
        System.out.println("我是手机观察者: " + event + "我做了zzz");
    }
}
