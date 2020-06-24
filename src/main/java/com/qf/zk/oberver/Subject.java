package com.qf.zk.oberver;

import java.util.ArrayList;
import java.util.List;

/**
 * 观察者模式
 *  1.一句话总结:定义对象之间的一对多的依赖关系,当一发生变化时就会通知多
 *  2.生活例子: 红绿灯,天气预报等消息
 *  3.解决问题:多个对象依赖一个一个对象(譬如多个follower会依赖一个leader)
 *  4.项目或者开源框架中使用场景: 监听器listener,事件机制(event),通知机制(zk通知机制)等
 *  5.模式结构:
 *      5.1首先定义一方,默认情况下一方名称为subject(主题),等价于leader
 *      5.2定义多方,默认情况下名称为Observer,因为多方必须有相同的规则,所以多个观察者实现同一个接口
 *      5.3定义一(Subject)和多(Observer)的关系:在subject中定义一个Observer的列表
 */
public class Subject {
    //定义一(Subject)和多(Observer)的关系:在subject中定义一个Observer的列表
    private List<Observer> list = new ArrayList();

    /**
     * 定义多方(Subject)可以动态注册到一方(Observer)
     * @param observer
     */
    public void addObserver(Observer observer){
        list.add(observer);
    }

    /**
     * 当一方发生变化时通知多方
     * @param event
     */
    public void notifyAllObject(String event){
        for (Observer observer : list) {
            observer.process(event);
        }
    }

}
