show tables;
create database dataTypedb;
use dataTypedb;
set hive.exec.mode.local.auto=true;


-- 创建一个多种特殊基本类型的表
create table if not exists bs1
(
    id1     tinyint,
    id2     smallint,
    id3     int,
    id4     bigint,
    sla     float,
    sla1    double,
    isok    boolean,
    content binary,
    dt      timestamp
)
    row format delimited fields terminated by ',';

-- 导入特殊基本类型
load data local inpath '/opt/data/bs1.csv' overwrite into table bs1;
select *
from bs1;

-- 创建一个含有数组的表
create table if not exists arr1
(
    name  string,
    score array<String> --定义数组类型
)
    row format delimited fields terminated by '\t'
        collection items terminated by ','; -- 定义数组中元素的分隔符

-- 加载带有数组arr类型的数据
load data local inpath '/opt/data/arr1.csv' overwrite into table arr1;
select *
from arr1;

-- 查询带有数组列的表的数组的某个元素，数组使用下标score[下标]
select name, score[1]
from arr1 where size(score) > 3;


-- 使用展开函数explode展开一个列为多行**********************************  列 转化成 行
select explode(score) score
from arr1;

--  zhang   12,34,56
-- 使用侧展lateral view 方式搭配explode打开数据集, 加上lateral view等价于给每个行加上原来的映射关系，以便找回自己的老祖宗
select name, cj
from arr1 lateral view explode(score) score as cj;

-- 统计每个学生的总成绩
select name, sum(cj)
from arr1 lateral view explode(score) score as cj group by name;




-- 通过as方式创建一个临时表，包含多行的数据  *********************** 行 转化为 列
create table if not exists arr_temp
as
select name, cj
from arr1 lateral view explode(score) score as cj;

select *
from arr_temp;

-- 创建行转列的临时表
create table if not exists arr3
(
    name  string,
    score array<string>
)
    row format delimited fields terminated by ' '
    collection items terminated by ',';


-- 通过collect_set把多行的数据搜集到同一列中,并且跟group by搭配使用，完成行转列
select name, collect_set(cj)
from arr_temp
group by name;

-- 将行转列的数据插入到目标表
insert into arr3
select name, collect_set(cj)
from arr_temp
group by name;

select *
from arr3;




-- 创建Map类型的表
create table if not exists map2
(
    name  string,
    score map<string, int>
) row format delimited fields terminated by ' '
    collection items terminated by ','
    map keys terminated by ':';

-- 加载map类型的表的数据
load data local inpath '/opt/data/map1.csv' overwrite into table map2;
select *
from map2;

-- 使用map类型进行查找
select name, score['english'] as english, score['math'] as math
from map2
where score['math'] > 60;

-- 使用侧展方式查询Map数据,实现列转行功能
select explode(score) as (qfclass, qfscore)
from map2;

-- 使用侧展方式查询Map数据，实现列转行功能
select name, qfclass, qfscore
from map2 lateral view explode(score) score as qfclass, qfscore;

-- 创建行转列的临时表
create table if not exists map_temp(
    name string,
    score1 int,
    score2 int,
    score3 int
)
    row format delimited fields terminated by ',';

-- 加载临时表数据
load data local inpath '/opt/data/map_temp.csv' overwrite into table map_temp;
select *
from map_temp;

-- 创建行转列的目标表
create table if not exists map3(
    name string,
    score map<string, int>
)
    row format delimited fields terminated by ' '
    collection items terminated by ','
    map keys terminated by ':';

-- 通过map函数查询，把原来的列的值，合并成一个map对象
select name, map('chinese', score1, 'math', score2, 'english', score3)
from map_temp;

-- 把查询结果插入目标表，实现行转列功能
insert into map3
select name, map('chinese',score1, 'math',score2, 'english',score3)
from map_temp;
select *
from map3;







-- 创建含有struct类型的表
create table if not exists str1(
    name string,
    score struct<chinese:int, math:int, english:int> --创建struct类型，定义3个属性
)
    row format delimited fields terminated by '\t'
    collection items terminated by ',';

-- 加载数据到struct结构表
load data local inpath '/opt/data/arr1.csv' overwrite into table str1;
select *
from str1;

-- 演示struct结构的查询
select name,score.chinese,score.math from str1 where score.math > 35;





-- 创建一个复杂数据类型的表
create table if not exists ss(
    id int,
    name string,
    belong array<string>, --数组类型
    tax map<string,double>, --map类型
    addr struct<province:string, city:string, road:string> --struct类型
)
    row format delimited fields terminated by ' '
    collection items terminated by ','
    map keys terminated by ':'
stored as textfile;

-- 导入复杂类型的数据
load data local inpath '/opt/data/ss.csv' overwrite into table ss;
select *
from ss;

-- 进行复杂类型查询: #需求: 下属个数大于4个, 公积金小于1200， 省份在河北的数据
select ss.id,
       ss.name,
       ss.belong[0],
       ss.belong[1],
       ss.tax['wuxian'],
       ss.tax['shebao'],
       ss.addr.road
from ss
where size(ss.belong) > 4 and
      ss.tax['gongjijin'] < 1200 and
      ss.addr.province = '河北';

-- 演示嵌套类型的数据表
create table qt(
    id int,
    name string,
    addr map<string, array<string>>
)
    row format delimited fields terminated by '\t'
    collection items terminated by ','
    map keys terminated by ':';

-- 演示嵌套类型数据的加载
load data local inpath '/opt/data/qt.csv' overwrite into table qt;
select *
from qt;









