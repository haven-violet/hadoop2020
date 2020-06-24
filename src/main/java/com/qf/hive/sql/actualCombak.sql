show tables;
set hive.exec.mode.local.auto=true;

use definefunction;

--把json数据导入Hive表,但是Hive默认不支持JSon格式数据,所以导入的JSon格式数据是单列
create table if not exists t_json(json string);
load data local inpath '/opt/data/rating.json' overwrite into table t_json;
select * from t_json;
set hive.exec.mode.local.auto=true;
-- 添加新建函数到hive中
add jar /opt/jar/udaf.jar;
create temporary function jsonParser as 'com.qf.hive.function.rating.MovieRateUDF';

-- 测试把json格式转化为\t分割的数据
select json, jsonParser(json) as jsonLine from t_json;

-- 把通过自定义函数解析后的数据先插入临时表
create table if not exists t_json_tmp as select jsonParser(json) as jsonLine from t_json;
select * from t_json_tmp;
select *
from t_json_tmp;

-- 把临时表数据通过\t分隔符可以插入到最终的MovieRate表(4个字段)中,把一个字符数据解析为4个字段
desc function split;

-- 创建最终的MovieRate表,把临时表数据导入
create table if not exists t_movieRate
as
select split(jsonline, '\t')[0] as movice,
       split(jsonline, '\t')[1] as rate,
       split(jsonline, '\t')[2] as qfTimeStamp,
       split(jsonline, '\t')[3] as uid
from t_json_tmp;
select * from t_movieRate;



-- 可以通过内置的get_json_object得到json格式的数据
select get_json_object(json, "$.movie") as movie from t_json limit 10;






