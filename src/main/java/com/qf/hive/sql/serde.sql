show tables;
-- 创建一个底层是CSV Serde的表,不能使用默认的行分隔符,和列分隔符
create table if not exists csv1
(
    uid int,
    uname string,
    age int
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile;

-- 直接加载csv格式的数据
load data local inpath '/opt/data/csv1.csv' overwrite into table csv1;
select * from csv1;




-- 使用定制的CSV Serde来创建表,可以定制分隔符,引号,转移符
create table if not exists csv3(
                                   uid int,
                                   uname string,
                                   age int
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        with serdeproperties(
        "separatorChar"=",",
        "qutoeChar"="'",
        "escapeChar"="\\"
        )
    stored as textfile;
load data local inpath '/opt/data/csv1.csv' overwrite into table csv3;
select *
from csv3;









-- 使用JSon Serde时,因为是第三方的Serde,所以必须先要加载到本次ClassPath中才能使用
add jar /opt/jar/json-serde-1.3.8-jar-with-dependencies.jar;

-- 创建一个底层是JSon Serde实现的表
create table if not exists js1(
                                  pid int,
                                  content string
)
    row format serde "org.openx.data.jsonserde.JsonSerDe";

-- JSon Serde 表加载json格式数据
load data local inpath '/opt/data/js.json' overwrite into table js1;
select *
from js1;





create table if not exists complex(
                                      uid int,
                                      uname string,
                                      belong array<String>,
                                      tax map<String,array<double>>
)
    row format serde "org.openx.data.jsonserde.JsonSerDe";

load data local inpath '/opt/data/complex.json' into table complex;
;
select *
from complex;

select
    c.*
from complex c
where size(c.belong) = 3
  and c.tax["gongjijin"][1] > 1200;




