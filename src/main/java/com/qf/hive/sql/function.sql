show  tables;

-- 查看当前系统中所有函数
show functions;

-- 查看某个函数的具体用法
desc function map;

-- 调用函数，默认会用select调用,注意是否可以传入列名
select current_date;
select current_database();

-- 获取当前日期时间戳
select unix_timestamp();

-- 将时间戳转化为当前日期
select from_unixtime(1583944238);
select from_unixtime(1583944238, 'yyyy-MM-dd HH:mm:ss');
-- 默认的内置时间格式 yyyy-MM-dd HH:mm:ss
select unix_timestamp('2019-09-09 14:14:01');

-- 计算时间差
select datediff('2019-06-06', '2020-06-06');

-- 查询当月第几天
select dayofmonth(current_date);

-- 月末
select last_day(current_date);


select concat('我爱', '祝绪丹');
select concat_ws('-', '十元', '里美');
select length('石原里美');
-- 开始位置从1开始   截取几个字符
select substring('hello world', 2, 1);
select upper("heool");
select cast('202' as int);

