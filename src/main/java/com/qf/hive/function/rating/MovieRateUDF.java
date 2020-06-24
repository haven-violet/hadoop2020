package com.qf.hive.function.rating;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * 定义一个UDF用来解析Json格式的数据为对象
 */
public class MovieRateUDF extends UDF {

    public String evaluate(String json) throws IOException {
        if(StringUtils.isEmpty(json)){
            return null;
        }

        //使用ObjectMapper<springMvc内置的json字符解析器>可以直接将字符串转化为Bean对象,使用泛型,不需要类型转化
        ObjectMapper objectMapper = new ObjectMapper();
        MovieRateBean movieRateBean = objectMapper.readValue(json, MovieRateBean.class);

        return movieRateBean.toString();
    }
}
