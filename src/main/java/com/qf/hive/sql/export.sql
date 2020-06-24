use definefunction;
-- 创建一张源表,并加载数据
create table text1
(
    uid int,
    uname string
)
    row format delimited fields terminated by ',';

load data local inpath '/opt/data/text1.txt' overwrite into table text1;
select * from text1;

-- 创建目标表1
create table text2
(
    uid int,
    uname string
)
    row format delimited fields terminated by ',';
select * from text2;

-- 创建目标表2
create table text3
(
    uname string
)
    row format delimited fields terminated by ',';
select * from text3;

-- 多表导入: 一次性扫描源表,多次导入到目标表
from text1
insert into text2 select uid,uname where uid < 3 --导入目标表1
insert into text3 select uname; --导入目标表2


-- 数据导出本地Linux文件
insert overwrite local directory '/opt/data/export'
select *
from text3;

-- 数据导出到hdfs文件,如果目标文件不存在,则创建
insert overwrite directory '/opt/data/export'
select *
from text3;

