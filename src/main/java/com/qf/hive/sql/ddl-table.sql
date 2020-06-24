show tables;

-- 修改表名
-- alter table t7 rename to a;

-- 修改列,注意修改时也要把列类型加上
-- alter table a change column name name1 string;

-- 修改列的位置
-- 把ip列放到ststus3后面
alter table log1 change column ip ip string after status2;

-- 把ip字段放在首位
alter table log1 change column ip ip string first;


-- 修改列类型
-- alter table a change column name1 name int;

desc a;

-- 增加字段
alter table a
    add columns (sex int);

-- 删除或替换字段
alter table a replace columns (id int, address string, age int);

desc a;
desc formatted a;
desc extended a;

-- 内部表转化为外部表
alter table a set tblproperties ('EXTERNAL' = 'TRUE');

-- 外部表转内部表, false大小写忽略
alter table a set tblproperties ('EXTERNAL' = 'false');

-- 给hive中变量设值
set hive.cli.print.current.db=true;



