package com.qf.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author liaojincheng
 * @Date 2020/4/12 18:23
 * @Version 1.0
 * @Description 自定义拦截器,实现body中的内容转大写
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 单个事件处理,将拦截source发动到channel中的消息(event); event:处理好之后返回到通道中
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //获取event中的数据
//        event.getHeaders() //获取header
        byte[] bodys = event.getBody();
        //转大写逻辑
        byte[] res = null;
        if(bodys != null){
            res = new String(bodys).toUpperCase().getBytes();
        }

        //将转化后的内容进行封装到event中
        event.setBody(res);
        //返回处理好的event
        return event;
    }

    /**
     * 批量事件处理
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> result = new ArrayList<>();
        for (Event event : list) {
            result.add(intercept(event));
        }
        return result;
    }

    /**
     * 对初始化中开启的对象进行关闭
     */
    @Override
    public void close() {

    }

    /**
     * 静态内部类,需要实现Interceptor.Builder
     */
    public static class Builder implements Interceptor.Builder{

        //拦截器的实例化
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        //配置
        @Override
        public void configure(Context context) {

        }
    }

}
