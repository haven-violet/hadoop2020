package com.qf.flume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author liaojincheng
 * @Date 2020/4/12 20:53
 * @Version 1.0
 * @Description
 */
public class MyInterceptor1 implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 解析单条event
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //输入
        String inputBody = null;
        //输出
        byte[] outputBody = null;
        //解析--这里定义对单条Event处理规则

        inputBody  = new String(event.getBody(), Charsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(inputBody);
        List<String> temp = new ArrayList<>();

        //(1)获取公共字段
        String host = jsonObject.getString("host");
        long user_id = jsonObject.getLongValue("user_id");
        JSONArray items = jsonObject.getJSONArray("items");

        //(2)json数组
        for (Object item : items) {
            JSONObject itemObj = JSONObject.parseObject(item.toString());
            Map<String, Object> map = new HashMap<>();
            map.put("host", host);
            map.put("user_id", user_id);
            map.put("item_type", itemObj.getString("item_type"));
            map.put("active_time", itemObj.getLongValue("active_time"));

            temp.add(new JSONObject(map).toJSONString());
        }

        outputBody  = String.join("\n", temp).getBytes();
        event.setBody(outputBody);
        return event;
    }

    /**
     * 解析多条event
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        //输出--一批Event
        List<Event> result = new ArrayList<>();

        //输入--一批Event
        for (Event event : list) {
            Event interceptEvent = intercept(event);
            byte[] interceptEventBody = interceptEvent.getBody();
            if(interceptEventBody.length != 0){
                String multiEvent = new String(interceptEventBody, Charsets.UTF_8);
                String[] multiEventArr = multiEvent.split("\n");
                for (String needEvent : multiEventArr) {
                    SimpleEvent simpleEvent = new SimpleEvent();
                    simpleEvent.setBody(needEvent.getBytes());
                    result.add(simpleEvent);
                }
            }
        }
        return result;
    }

    @Override
    public void close() {

    }

    /**
     * 实现内部类接口
     */
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new MyInterceptor1();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
