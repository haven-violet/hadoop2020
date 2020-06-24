package com.qf.hive.function;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 定义一个自定义UDF函数,接收数据如: sex=1&height=180&weight=130&sal=28000 输入: key值 sex
 * 输出: 1
 */
public class Key2Value extends UDF {

    /**
     * 定义回调方法evaluate,用来处理自定义函数的核心任务
     * @return
     */
    public String evaluate(String str, String key) throws JSONException {
//        转化前str: sex=1&height=180&weight=130&sal=28000
//        转化后str: {sex=1&height=180&weight=130&sal=28000}
//        if(str != null && str.length() > 0){
//
//        }
        //只要key或value为空,直接退出
        if (StringUtils.isEmpty(str) || StringUtils.isEmpty(key) ) {
            return null;
        }

//        完成转化前str到转化后str
        String replace = str.replace("&", ",");
        String replace1 = replace.replace("=", ":");

        //给字符串加上{}转化为标准的json字符串
        String result = "{" + replace1 + "}";

        //把字符串转化成JSON对象
        JSONObject jsonObject = new JSONObject(result);

        System.out.println(jsonObject.getString(key));
        //通过Json对象获取获取其中的key属性
        result = jsonObject.getString(key);
        return result;
    }

    public static void main(String[] args) throws JSONException {
        new Key2Value().evaluate("sex=1&height=180&weight=130&sal=28000", "sal");
    }

}
