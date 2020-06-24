package com.qf.hive.function;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 自定义一个hive函数,默认继承于UDF
 */
public class AgeUDF extends UDF {

    public String evaluate(String param) throws ParseException {
        if(param == null){
            return "";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date parse = sdf.parse(param);
        int result = new Date().getYear() - parse.getYear();
        return String.valueOf(result);
    }
}
