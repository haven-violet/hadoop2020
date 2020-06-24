show tables;

create database overfunction;
use overfunction;
set hive.exec.mode.local.auto=true;


-- 创建一个订单表
create table if not exists t_order
(
    name string,
    orderdate string,
    cost int
)
    row format delimited fields terminated by ',';

-- 加载订单数据
load data local inpath '/opt/data/order.csv' overwrite into table t_order;
select *
from t_order;

-- 明细查询
select * from t_order;

-- 统计查询
select count(*) from t_order;

-- 开始本地模式
set hive.exec.mode.local.auto=true;

-- 在默认情况下,hive的细节查询和统计查询不能同时执行
select *, count(*) from t_order;

-- 通过over语法,把明细查询和统计查询连在一起执行,如果over中没有参数,默认是对全体数据进行计算
select  *, count(*) over ()
from t_order;

--查询在2018年1月份购买过的顾客购买明细及总人数
select *, count(*) over ()
from t_order
where substr(orderdate, 1, 7) = '2018-01';

-- 需求: 按照每个月来统计购买总量和购买细节
select name, orderdate, cost, sum(cost) over (partition by month(orderdate))
from t_order;



-- 需求: 按照每个月来统计购买总量和购买细节，按照购买日期排序,sum(cost)此时在窗口函数系会进行累加
select name, orderdate, cost, sum(cost) over (partition by month(orderdate) order by orderdate)
from t_order;

-- window 子句的演示 rows开始
select name,
       orderdate,
       cost,
       sum(cost) over() as sample1, --所有行相加
       sum(cost) over(partition by name) as sample2, --按name分组,组内数据相加
       sum(cost) over(partition by name order by orderdate) as sample3, --按name分组,组内数据累加
       sum(cost) over(partition by name order by orderdate rows between unbounded preceding and current row) as sample4, --由起点到当前行的聚合
       sum(cost) over(partition by name order by orderdate rows between 1 preceding and current row) as sample5,--当前行和前面一行做聚合
       sum(cost) over(partition by name order by orderdate rows between 1 preceding and 1 following) as sample6,--当前行和前边一行及后面一行
       sum(cost) over(partition by name order by orderdate rows between current row and unbounded following) as sample7--当前行及后面所有行
from t_order;

-- 查询每个顾客到目前为止的购买总额
select name,
       orderdate,
       cost,
       sum(cost)    over(partition by name order by orderdate rows between unbounded preceding and current row) as sample
from t_order;

-- 查询顾客上一次购买的时间,如果是第一条记录,那么没有上一条,那么可以用一个默认值代替,避免查询的时候出现null
select name, orderdate, cost, lag(orderdate, 1, '2000-01-01') over(partition by name order by orderdate) as beforeDate
from t_order;

-- 查询顾客下一次购买的时间,如果是最后一条记录,那么没有下一条,那么可以用一个默认值代替,避免查询的时候出现null
select name, orderdate, cost, lead(orderdate, 1, '2020-01-01') over (partition by name order by orderdate) as afterDate
from t_order;

-- first_value 取分组内排序后,截止到当前行,第一个值
-- 需求:获取第一次购买的时间
select name, orderdate, cost, first_value(orderdate) over (partition by name order by orderdate) as FirstDate
from t_order;

-- 获取最后一次购买的时间
select name, orderdate, cost, last_value(orderdate) over (partition by name ) as LastDate
from t_order;



-- 统计用户每条记录前三条和后三条的数据之间所有的购买总额
select name, orderdate, cost, sum(cost) over(partition by name order by orderdate rows between 3 preceding and 3 following) as sample
from t_order;


-- 统计5月份每个人最早购买的时间和最迟购买的时间
select name, orderdate, cost,
       last_value(orderdate) over(partition by name ) as FirstDate,
       first_value(orderdate) over(partition by name order by orderdate desc) as LastDate
from t_order
where month(orderdate) = '01';

-- 对每个人的购买量进行切片分割为4份,并且显示每条记录所属的切片标记(譬如第几片)
select name, orderdate, cost, ntile(4) over(partition by name)
from t_order;

use overfunction;
-- 找出50%的数据,通过子查询代替查询结果,可以理解为一张表,然后通过表的属性就可以查找

select name, orderdate, cost, ntileNumber
from (select name,
             orderdate,
             cost,
             ntile(2) over (partition by name order by orderdate) as ntileNumber
      from t_order) temp
where temp.ntileNumber = '1';

-- 创建学生表
create table if not exists stu_score
(
    dt string,
    name string,
    score int
)
    row format delimited fields terminated by ',';
load data local inpath '/opt/data/stu_score.csv' overwrite into table stu_score;

-- 查询所有学生成绩,倒序
select *
from stu_score
order by dt, score desc;

-- 查看每科成绩排名情况,row_number给成绩排名,并且相同成绩按照顺序排列,哪个进来的早就按照哪个排序
-- 即使分数相同，也是有序从开始到结束,不会有空位
select dt, name, score, row_number() over(partition by dt order by score desc) as rowNum
from stu_score;

-- 查看排名情况,rank(): 有并列,相同名次空位
select dt, name, score, rank() over(partition by dt order by score desc) as rowNum
from stu_score;

-- 查看排名情况,dense_rank(): 有并列,相同名次不空位
select dt, name, score, rank() over(partition by dt order by score desc) as rowNum
from stu_score;

-- 需求:寻找每科成绩的前三名, 相同不并列
select dt, name, score, rowNum
from (select dt, name, score, row_number() over (partition by dt order by score desc) as rowNum
      from stu_score) temp
where temp.rowNum < 4;





