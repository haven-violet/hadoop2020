package com.qf.hive.function;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 定义自定义函数使用正则表达式来清洗数据
 */
public class LogRegParser extends UDF {
    /**
     * 解析前: 220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] "GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1" 200 8784 "-" "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
     * 解析后: 220.181.108.151    20120131 120232    GET    /home.php?mod=space&uid=158&do=album&view=me&from=space    HTTP    200    Mozilla
     * @return
     */
    public String evaluate(String log) throws ParseException {
        //定义一个正则表达式,对原始字符串(解析前),进行匹配分组
        String reg = "^([0-9.]+\\d+) - - \\[(.* \\+\\d+)\\] .+(GET|POST) (.+) (HTTP)\\S+ (\\d+) .+\\\"(\\w+).+$";

        //通过编译方式得到模式匹配器,提高性能
        Pattern pattern = Pattern.compile(reg);

        //用模式匹配器去匹配传入的字符串
        Matcher matcher = pattern.matcher(log);

        //定义一个StringBuffer用来拼接字符串
        StringBuffer stringBuffer = new StringBuffer();

        //判断输入数据是否匹配分组正则表达式模式
        if(matcher.find()){
            //先获取匹配的分组数
            int count = matcher.groupCount();

            //第一组是下标从1开始
            for (int i = 1; i <= count; i++) {
                //对正则表达式中分组进行遍历,如果是第二组,那么就进行日期格式类型转化
                if(i == 2){
                    //原始日期格式为: 31/Jan/2012:00:02:32 +0800
                    //定义一个英文格式的日期格式解析器
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
                    //拿出第二组的数据,通过上述格式化类转化为标准的日期格式
                    Date parseDate = simpleDateFormat.parse(matcher.group(2));

                    //定义转化后的格式 20120131 120232
                    SimpleDateFormat simpleDateFormatAfter = new SimpleDateFormat("yyyyMMdd hhmmss");

                    //把日期按照自定义格式化转化
                    String format = simpleDateFormatAfter.format(parseDate);
                    stringBuffer.append(format + "\t");
                } else {//否则直接拼接
                    stringBuffer.append(matcher.group(i)+"\t");
                }
            }
            return stringBuffer.toString();
        }
        return "";
    }

    public static void main(String[] args) throws ParseException {
        String evaluate = new LogRegParser().evaluate("220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"-\" \"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\"");
        System.out.println(evaluate);
    }
}
