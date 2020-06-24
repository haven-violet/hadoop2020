show tables;
create database partdb;

-- drop table part1;

use partdb;

-- 创建一个一级分区表
create table part1
(
    id   int,
    name string
)
    partitioned by (dt string)
    row format delimited fields terminated by ',';

-- 往分区表加载数据
load data local inpath '/opt/data/user.csv' overwrite into table part1 partition (dt = '2018-08-08');
select *
from part1;

-- 创建二级分区
create table if not exists part2
(
    id   int,
    name string
) partitioned by (year string, mouth string)
    row format delimited fields terminated by ',';

load data local inpath '/opt/data/user.csv' overwrite into table part2 partition (year = '2018', mouth = '03');

-- 给三级分区加载数据
create table if not exists part3(
    id int,
    name string
)
    partitioned by (year string, month string, day string)
    row format delimited fields terminated by ',';

-- 给三级分区加载数据
load data local inpath '/opt/data/user.csv' overwrite into table part3 partition (year = '2018', month = '08', day = '08');

create table if not exists part4(
                                    id int,
                                    name string
)
    partitioned by (year string,month string,DAY string)
    row format delimited fields terminated by ',';

-- 测试数据分区是否区分大小写, 实际上是不区分的
load data local inpath '/opt/data/user.csv' overwrite into table part4 partition (year = '2018',month = '02',day = '02');
select *
from part4;

-- 查看分区
show partitions part4;

drop table part5;

-- 动态增加分区
create table if not exists part5
(
    id   int,
    name string
)
    partitioned by (dt string) row format delimited fields terminated by ",";
alter table part5 add partition (dt="2018-03-21");

-- 同时添加两个分区
alter table part5 add partition (dt="2018-03-20") partition (dt="2018-03-17");
show partitions part5;


-- 添加分区，并且设置数据
alter table part5 add partition (dt = '2018-11-11') location '/user/hive/warehouse/partdb.db/part1/dt=2018-08-08';

select *
from part5;

-- 修改(设置)某个分区的路径，路径location要使用hdfs的绝对路径
alter table part5 partition (dt="2018-11-11") set location "hdfs://hadoop91:9000/user/hive/warehouse/partdb.db/part1/dt=2018-08-08";
select *
from part5;
show partitions part5;

-- 删除分区
alter table part5 drop partition(dt='2018-03-17');

-- 删除多个分区
alter table part5 drop partition(dt='2018-03-20'), partition(dt='2018-11-11');


-- 创建动态分区
create table dy_part1
(
    id   int,
    name string
) partitioned by (dt string)
  row format delimited fields terminated by ',';

-- 给动态分区加载数据，不能直接加载数据
load data local inpath '/opt/data/user.csv' overwrite into table dy_part1 partition (dt);

-- 设置动态分区为true
set hive.exec.dynamic.partition=true;
-- 并且设置为非严格检查模式
set hive.exec.dynamic.partition.mode=nostrict;

-- 创建要导入动态分区的临时表
create table temp_part(
    id int,
    name string,
    dt string --给动态分区字段
)
    row format delimited fields terminated by ',';

-- 给临时表导入有分区字段的数据
load data local inpath '/opt/data/student.csv' overwrite into table temp_part;

select *
from temp_part;

-- 通过临时表给动态分区导入数据，一般把临时表中的字段都要写清除，不要用*
insert into dy_part1 partition(dt)
select id,name,dt from temp_part;

select * from dy_part1;

-- 创建一个混合分区
create table dy_part2
(
    id   int,
    name string
)
    partitioned by (year string, month string, day string)
    row format delimited fields terminated by ','
;

-- 创建混合分区的临时表
create table temp_part2
(
    id    int,
    name  string,
    year  string,
    month string,
    day   string
)
    row format delimited fields terminated by ',';

-- 导入数据到混合分区临时表中
load data local inpath '/opt/data/dystudent.csv' overwrite into table temp_part2;
select *
from temp_part2;

-- 从临时表向混合分区导入数据，静态数据直接硬代码，动态数据从临时表中获取
insert into dy_part2 partition (year = '2020', month, day)
select id, name, month, day
from temp_part2;

select *
from dy_part2;











