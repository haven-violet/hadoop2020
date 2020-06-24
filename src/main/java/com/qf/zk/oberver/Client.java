package com.qf.zk.oberver;

/**
 * 写一个类进行观察者模式的测试
 */
public class Client {
    public static void main(String[] args) {
        //1.创建一个Subject(一方)
        Subject subject = new Subject();

        //2.创建多方,父类声明,子类实现,提高扩展性
        Observer netObserver = new NetObserver();
        Observer tvObserver = new TVObserver();
        Observer phoneServer = new PhoneObserver();

        //3.把多方(Observer)注册到一方(Subject)
        subject.addObserver(netObserver);
        subject.addObserver(tvObserver);
        subject.addObserver(phoneServer);

        //4.当一方发生变化时,通知多方
        subject.notifyAllObject("服务器挂了");
    }



}
