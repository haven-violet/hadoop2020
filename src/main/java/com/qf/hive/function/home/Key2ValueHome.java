package com.qf.hive.function.home;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 自定义UDF函数解析Json
 */
public class Key2ValueHome extends UDF {
    /**
     * 定义回调函数,用来处理自定义函数的核心业务
     * @return
     */
    public String evaluate(String str, String key) throws JSONException {
        //name-zhang$age-30$address-shenzhen
        if(StringUtils.isEmpty(str) || StringUtils.isEmpty(key)){
            return null;
        }

        String replace = str.replace("$", ",");
        String replace1 = replace.replace("-", ":");

        String result = "{" + replace1 + "}";

        //将json字符串转化为json对象
        JSONObject jsonObject = new JSONObject(result);
        result = jsonObject.getString(key);
        //System.out.println(result);

        return result;
    }

    public static void main(String[] args) throws JSONException {
        new Key2ValueHome().evaluate("name-zhang$age-30$address-shenzhen", "address");
    }
}
