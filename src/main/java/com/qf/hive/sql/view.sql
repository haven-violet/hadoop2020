show tables;
use definefunction;

-- 视图可以理解为一个特殊的只读表,是一个逻辑机构,没有物理存在,所以视图创建只能依赖表
create view if not exists smallview
as
select id, name from stu where id > 1;

-- 查看视图是否存在
show tables;

-- 查看视图的创建方式, 注意: 要用table代替view
show create table smallview;

-- 查看视图的结构
desc smallview;

-- 视图的克隆, 1.2.1不支持
create view view2 like smallview;

-- 删除视图
drop view if exists smallview;

-- 视图的查看(用select查看),视图的结果是只读数据,隐藏了不想暴露的字段
select * from smallview;


