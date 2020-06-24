show tables;

-- 通过add jar 路径, 把当前Linux目录下的jar加入到hive的classpath中,注意不能用引号，作用域是此次会话有效
add jar /opt/jar/udf.jar;

-- 创建临时函数名,传入刚才新建的函数全路径名
create temporary function toUp as 'com.qf.hive.function.FirstUDF';
show functions;

-- 测试当前函数
select toUp('violet');

-- 删除临时函数
drop temporary function if exists toUp;


add jar /opt/jar/udtf.jar;

create temporary function parseMap as 'com.qf.hive.function.ParseMapUDTF';

select parseMap("name:zhang;age:23;sex:boy");







create database if not exists defineFunction;
use defineFunction;

create table if not exists stu(
                                  id int,
                                  name string
)
    row format delimited fields terminated by ',';

load data local inpath '/opt/data/stu.txt' overwrite into table stu;
select *
from stu;
use defineFunction;
add jar /opt/jar/udaf.jar;
create temporary function maxInt1 as 'com.qf.hive.function.MaxValueUDAF';
select maxInt1(id)
from stu;

drop temporary function maxInt1;

desc function maxInt1;
set hive.exec.mode.local.auto=true;






