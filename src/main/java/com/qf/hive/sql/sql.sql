--  查询所有数据库
show databases;

-- 创建数据库的几种方式
create database if not exists zoo;
create database if not exists qfdb comment 'this is a database of qf';

-- 查询数据库，通配符查询
show databases like 'z*';

-- 这个只能删除空库
drop database zoo;

-- 如果不是空库，则可以加cascade强制删除
drop database zoo cascade;

-- 创建一张表，如果只是内部管理数据，那么语法和mysql基本一样
create table cat
(
    id   int,
    name string
);

drop table cat;

-- 查看当前数据库的表
show tables;

-- 查看另外一个数据库中的表
show tables in qfdb;

-- 查看表信息
desc cat;

-- 查看表详细信息
desc formatted cat;

-- 查看创建表信息
show create table cat;

-- 创建外部表，需要加上external关键字
create external table excat
(
    id   int,
    name string
);
-- 查看外部表详细信息
desc formatted excat;

-- 删除内部表,元数据和实际数据一起删除
drop table cat;

-- 删除外部表,元数据删除，实际数据和目录没有删除
drop table excat;

-- 创建一个要加载数据的表
create table qfuser
(
    id   int,
    name string
)row format delimited fields terminated by ',';

drop table qfuser;

-- 从本地加载文件到qfuser表,数据从本地拷贝到hive
-- 如果加了overwrite,那么原表数据就会清除,表示了数据导入的幂等性
load data local inpath '/opt/data/user.csv' overwrite into table qfuser;
select *
from qfuser;

-- 从hdfs导入数据到hive,默认是把源文件移动到hive的对应目录下面
load data inpath '/user.csv' overwrite into table qfuser;

-- 创建表时指定列分隔符和行分隔符
create table qfusernew
(
    id   int,
    name string
)
    row format delimited fields terminated by ','
    lines terminated by '\n'
;

-- 通过insert into 方式灌入数据，可以加条件导入
insert into qfusernew
select *
from qfuser
where id <= 3;

select *
from qfusernew;

-- 克隆表结构，不需要表的数据
create table qfuserold like qfusernew;
select *
from qfuserold;

-- 克隆表结构，并且带数据
create table t5 like qfusernew location '/user/hive/warehouse/qfusernew';
select *
from t5;

-- 创建表时克隆数据
create table if not exists t6
(
    id   int,
    name string
) comment 'this is a table'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile
    location '/user/hive/warehouse/qfusernew';
select *
from t6;

-- 克隆表帶as
create table t7 as select * from t6;
select *
from t7;



-- 实战一个项目
CREATE TABLE log1(
                     id             string COMMENT 'this is id column',
                     phonenumber     bigint,
                     mac             string,
                     ip               string,
                     url              string,
                     status1          string,
                     status2           string,
                     upflow            int,
                     downflow         int,
                     status3          string,
                     dt string
)
    COMMENT 'this is log table'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
        LINES TERMINATED BY '\n'
    stored as textfile;

load data local inpath '/opt/data/log1.txt' overwrite into table log1;

select *
from log1;

-- 1.统计每个电话号码的总流量
select l.phonenumber, round(sum(l.upflow + l.downflow) / 1024.0, 2) total
from log1 l
group by l.phonenumber;

-- 启动本地模式，提高hql查询速度
set hive.exec.mode.local.auto=true;




























