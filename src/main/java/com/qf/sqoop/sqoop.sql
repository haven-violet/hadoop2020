

-- 创建用户行为日志数据 埋点数据
create external table if not exists ods_nshop.ods_nshop_01_useractlog
(
    action       string comment '行为类型:install安装|launch启动|interactive交互|page_enter_h5页面曝光|page_enter_native页面进入|exit退出',
    event_type   string comment '行为类型:click点击|view浏览|slide滑动|input输入',
    customer_id  string comment '用户id',
    device_num   string comment '设备号',
    device_type  string comment '设备类型',
    os           string comment '手机系统',
    os_version   string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier      string comment '电信运营商',
    network_type string comment '网络类型',
    area_code    string comment '地区编码',
    longitude    string comment '经度',
    latitude     string comment '纬度',
    extinfo      string comment '扩展信息(json格式)',
    duration     string comment '停留时长',
    ct           bigint comment '创建时间'
) partitioned by (bdp_day string)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE
    location '/data/nshop/ods/user_action_log/';

set hive.exec.mode.local.auto=true;

-- DWD层 明细粒度事实层
-- 用户主题
-- 1. 分析用户启动日志表

-- 使用动态分区使用开启动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- job file not exists 直接创建该文件即可

-- 少了一个分区字段

insert overwrite table dwd_nshop.dwd_nshop_actlog_launch partition (bdp_day)
select customer_id,
       device_num,
       device_type,
       os,
       os_version,
       manufacturer,
       carrier,
       network_type,
       area_code,
       from_unixtime(cast(ct / 1000 as int), 'HH'),
       ct,
       bdp_day
from ods_nshop.ods_nshop_01_useractlog
where action = '02'
  and bdp_day = '20200419';

select * from dwd_nshop.dwd_nshop_actlog_launch limit 10;







-- dwd 用户产品浏览表

-- 先查看一个用户行为日志数据log, 即使是浏览,那么就是 07 或者 08
select * from ods_nshop.ods_nshop_01_useractlog where action = '07' limit 10;

-- extinfo {"target_id":"4320207200701"}
-- 往用户产品浏览表中插入数据    get_json_object()
set hive.exec.mode.local.auto=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_nshop.dwd_nshop_actlog_pdtview partition (bdp_day)
select customer_id,
       device_num,
       device_type,
       os,
       os_version,
       manufacturer,
       carrier,
       network_type,
       area_code,
       get_json_object(extinfo, '$.target_id') as target_id,
       duration,
       ct,
       bdp_day
from ods_nshop.ods_nshop_01_useractlog
where bdp_day = '20200419'
  and action in ('07', '08');

select * from dwd_nshop.dwd_nshop_actlog_pdtview limit 10;



-- dwd 用户产品查询表
-- action = 05 交互       eventtype = 01 浏览| 04 滑动
-- {"target_type":"4","target_keys":"20206","target_order":"20","target_ids":"[\"4320206871201\",\"4320206738601\",\"4320206632601\",\"4320206465801\"]"}
select split(regexp_replace(get_json_object(extinfo, '$.target_ids'), '[\\[\\"\\]]', ''), ',') from ods_nshop.ods_nshop_01_useractlog where action = '05' and event_type in ('01', '04') limit 4;
select explode(split(regexp_replace(get_json_object(extinfo, '$.target_ids'), '[\\[\\"\\]]', ''), ',')) from ods_nshop.ods_nshop_01_useractlog where action = '05' and event_type in ('01', '04') limit 4;
-- get_json_object()函数,先获取json字符串中的key对应的值
-- regexp_replace()函数,先将字符串抽成1,2,3
-- split后将 1,2,3变成["1","2","3"]字符串数组
-- 使用explode会将数组,集合爆炸出多个值出来，但是不能和其他单个字段值进行相连,一对多不匹配
-- 而后explode结合lateral view explode() t as temp侧接,可以做到一对多关系
select customer_id, target_id
from ods_nshop.ods_nshop_01_useractlog lateral view explode(split(regexp_replace(get_json_object(extinfo, '$.target_ids'), '[\\[\\"\\]]', ''), ',')) t as target_id
where action = '05' and event_type in ('01', '04')  limit 4;


insert overwrite table dwd_nshop.dwd_nshop_actlog_pdtsearch partition(bdp_day)
select customer_id,
       device_num,
       device_type,
       os,
       os_version,
       manufacturer,
       carrier,
       network_type,
       area_code,
       get_json_object(extinfo, '$.target_order') as target_order,
       get_json_object(extinfo, '$.target_keys')  as target_keys,
       target_id,
       ct,
       bdp_day
from ods_nshop.ods_nshop_01_useractlog lateral view explode(
        split(regexp_replace(get_json_object(extinfo, '$.target_ids'), '[\\[\\"\\]]', ''), ',')) t as target_id
where
    bdp_day='20200419'
    and
    action='05'
    and
    event_type in ('01','04')
;


-- 用户产品关注表
-- action='05'关注  eventtype='02'点击      {"target_type":"3","target_id":"4320406544401","target_action":"01"}
-- 首先target_type='04'是没有数据的,所以该判断需要舍弃
-- 并且还需要判断target_action='01'关注,所以这里可以做一个临时表
with log as (
select
    customer_id,
    device_num,
    device_type,
    os,
    os_version,
    manufacturer,
    carrier,
    network_type,
    area_code,
--     因为数据问题,这个type判断要舍弃,不然没有数据,但是如果正常计算是需要判断的；没有'03'产品数据,所以'04'店铺来充当
--     get_json_object(extinfo, '$.target_type') as target_type,
    get_json_object(extinfo, '$.target_action') as target_action,
    get_json_object(extinfo, '$.target_id') as target_id,
    ct,
    bdp_day
from ods_nshop.ods_nshop_01_useractlog
where
    bdp_day = '20200419'
    and
    action = '05'
    and
    event_type = '02'
)
insert overwrite table dwd_nshop.dwd_actlog_product_comment partition(bdp_day)
select
    customer_id,
    device_num,
    device_type,
    os,
    os_version,
    manufacturer,
    carrier,
    network_type,
    area_code,
    target_id,
    ct,
    bdp_day
from log
where target_action='01';


select * from dwd_nshop.dwd_actlog_product_comment limit 10;

-- 上面的数据来自于ods中用户行为日志数据


-- 下面的数据来自于ods中业务数据
-- 交易主题域
-- 交易订单明细流水表
-- 先查看一下自己的order订单表中的日期 20191102
select from_unixtime(cast(order_ctime/1000 as bigint), "yyyyMMdd") from ods_nshop.ods_02_orders limit 10;


-- 交易订单明细流水表
-- 创建交易订单明细流水表
create external table if not exists dwd_nshop.dwd_nshop_orders_details
(
    order_id          string comment '订单ID',
    order_status      int comment '订单状态：5已收货(完成)|6投诉 7退货',
    supplier_code     VARCHAR(20) COMMENT '店铺ID',
    product_code      VARCHAR(20) COMMENT '商品ID',
    customer_id       string comment '用户id',
    consignee_zipcode string comment '收货人地址',
    pay_type          string comment '支付类型：线上支付 10 网上银行 11 微信 12 支付宝 | 线下支付(货到付款) 20 ',
    pay_nettype       varchar(1) COMMENT '支付网络方式：0 wifi | 1 4g | 2 3g |3 线下支付',
    pay_count         int comment '支付次数',
    product_price     DECIMAL(5, 1) COMMENT '购买商品单价',
    product_cnt       INT COMMENT '购买商品数量',
    weighing_cost     DECIMAL(2, 1) COMMENT '商品加权价格',
    district_money    DECIMAL(4, 1) COMMENT '优惠金额',
    shipping_money    DECIMAL(8, 1) COMMENT '运费金额',
    payment_money     DECIMAL(10, 1) COMMENT '支付金额',
    is_activity       int COMMENT '1:参加活动|0：没有参加活动',
    order_ctime       bigint comment '创建时间'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dwd/order/dwd_nshop_orders_details/';



with tborder as(
    select
        order_id,
        order_status,
        customer_id,
        consignee_zipcode,
        pay_type,
        pay_nettype,
        shipping_money,
        payment_money,
        order_ctime
    from ods_nshop.ods_02_orders
    where from_unixtime(cast(order_ctime/1000 as bigint), 'yyyyMMdd') = '20191102'
),
tbdetail as (
    select
        a.order_id,
        b.supplier_code,
        a.product_id,
        b.product_price,
        a.product_cnt,
        a.weighing_cost,
        a.district_money,
        a.is_activity
    from ods_nshop.ods_02_order_detail a
             join ods_nshop.dim_pub_product b
                  on a.product_id = b.product_code
    where from_unixtime(cast(a.order_detail_ctime/1000 as bigint), 'yyyyMMdd')='20191102'
),
pays as(
    select
        order_id,
        count(*) as pay_count
    from ods_nshop.ods_02_orders_pay_records
    where from_unixtime(cast(pay_ctime / 1000 as bigint), 'yyyyMMdd') = '20191102'
    group by order_id, from_unixtime(cast(pay_ctime / 1000 as int), 'yyyyMMdd')
)
insert overwrite table dwd_nshop.dwd_nshop_orders_details partition(bdp_day='20200419')
select
    tborder.order_id,
    tborder.order_status,
    tbdetail.supplier_code,
    tbdetail.product_id,
    tborder.customer_id,
    tborder.consignee_zipcode,
    tborder.pay_type,
    tborder.pay_nettype,
    pays.pay_count,
    tbdetail.product_price,
    tbdetail.product_cnt,
    tbdetail.weighing_cost,
    tbdetail.district_money,
    tborder.shipping_money,
    tborder.payment_money,
    tbdetail.is_activity,
    tborder.order_ctime
from
    tborder join tbdetail on tborder.order_id = tbdetail.order_id
    join pays on tborder.order_id = pays.order_id;

select * from dwd_nshop.dwd_nshop_orders_details limit 10;



create table student_temp(
    name string,
    class string,
    isLike int
)
location '/opt/student_temp/';

set hive.exec.mode.local.auto=true;
insert overwrite table student_temp values('zhangsan', '1', 1),('zhangsan', '1', 0),('zhangsan', '1', 1),('zhangsan', '1', 0);
insert into table student_temp values('lisi', '2', 1), ('lisi', '2', 0);
select * from student_temp;

select
    name,
    count(class) over(partition by class) as count_class
from student_temp;

select * from dwd_nshop.dwd_actlog_product_comment limit 10;


-- 查看广告投放信息
select * from ods_nshop.ods_nshop_01_releasedatas limit 10;
-- 投放请求参数
-- ip=3.61.2.222&deviceNum=723748&lon=null&lat=null&aid=null&ctime=
-- 1572654905000&sources=tencent&session=1572654785000723748416198&productPage=4320308033801
-- 页面布局表 page_code页面编码13位   page_target=11位商品实体或者8位店铺实体     13位中最后两位是详情,推荐,首页,介绍
select * from ods_nshop.dim_pub_page limit 10;
-- 用ods层中的投放请求参数里面的productPage匹配业务数据层中的页面布局表的page_code匹配成功   页面布局表中的page_target=43203080338     11位表示是一个商品实体
select * from ods_nshop.dim_pub_page where page_code = '4320308033801';
select * from ods_nshop.dim_pub_product limit 10;
select * from ods_nshop.dim_pub_product where product_code = '43203080338';
-- 投放请求参数ip=3.61.2.222&deviceNum=723748&lon=null&lat=null&aid=null&ctime=1572654905000&sources=tencent&session=1572654785000723748416198&productPage=4320308033801
select parse_url(concat("http://127.0.0.1:8080/release?", release_params), 'QUERY', 'productPage' ) from ods_nshop.ods_nshop_01_releasedatas limit 1;
-- 具体用到的时候在进行查询

set hive.exec.mode.local.auto=true;
-- dwd广告投放数据表
with tbrelease as (
    select c.customer_id,
           r.device_num,
           r.device_type,
           r.os,
           r.os_version,
           r.manufacturer,
           r.area_code,
           r.release_sid,
           r.release_session,
           r.release_sources,
           parse_url(concat('http://127.0.0.1:8080/release?', release_params), 'QUERY', 'ip') as release_ip,
           parse_url(concat('http://127.0.0.1:8080/release?', release_params), 'QUERY',
                     'productPage')                                                           as release_product_page,
           r.ct
    from ods_nshop.ods_nshop_01_releasedatas r
             join ods_nshop.ods_02_customer c
                  on r.device_num = c.customer_device_num
    where r.bdp_day = '20200419'
)
insert overwrite table dwd_nshop.dwd_nshop_releasedatas partition(bdp_day = '20200419')
select a.customer_id,
       a.device_num,
       a.device_type,
       a.os,
       a.os_version,
       a.manufacturer,
       a.area_code,
       a.release_sid,
       a.release_ip,
       a.release_session,
       a.release_sources,
       f.category_code,
       f.product_code,
       a.release_product_page,
       a.ct
from tbrelease a
         join ods_nshop.dim_pub_page p
              on a.release_product_page = p.page_code
                  and p.page_type = '4'
         join ods_nshop.dim_pub_product f
              on p.page_target = f.product_code;

select * from dwd_nshop.dwd_nshop_releasedatas limit 2;





-- 创建dws 公共汇总粒度事实层
create database dws_nshop;
-- 创建用户启动dws
create external table if not exists dws_nshop.dws_nshop_ulog_launch
(
    user_id      string comment '用户id',
    device_num   string comment '设备号',
    device_type  string comment '设备类型',
    os           string comment '手机系统',
    os_version   string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier      string comment '电信运营商',
    network_type string comment '网络类型',
    area_code    string comment '地区编码',
    launch_count int comment '启动次数'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/user/dws_nshop_ulog_launch/';

insert overwrite table dws_nshop.dws_nshop_ulog_launch partition (bdp_day = '20200419')
select
    user_id,
    device_num,
    device_type ,
    os          ,
    os_version  ,
    manufacturer,
    carrier     ,
    network_type,
    area_code,
    count(device_num) over(partition by device_num) as launch_count
from dwd_nshop.dwd_nshop_actlog_launch
where bdp_day = '20200419'
;

select * from dws_nshop.dws_nshop_ulog_launch limit 10;


-- 用户启动7day 一周数据dws轻度聚合dws 公共汇总粒度事实层
create external table if not exists dws_nshop.dws_nshop_ulog_launch_7d
(
    user_id      string comment '用户id',
    device_num   string comment '设备号',
    device_type  string comment '设备类型',
    os           string comment '手机系统',
    os_version   string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier      string comment '电信运营商',
    network_type string comment '网络类型',
    area_code    string comment '地区编码',
    launch_count int comment '启动次数'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/user/dws_nshop_ulog_launch_7d/';


insert overwrite table dws_nshop.dws_nshop_ulog_launch_7d partition (bdp_day = '20200419')
select
    user_id,
    device_num,
    device_type ,
    os          ,
    os_version  ,
    manufacturer,
    carrier     ,
    network_type,
    area_code,
    count(device_num) over(partition by device_num) as launch_count
from dwd_nshop.dwd_nshop_actlog_launch
where bdp_day between '20200413' and '20200419';

select * from dws_nshop.dws_nshop_ulog_launch_7d limit 10;


-- 创建用户浏览dws

create external table if not exists dws_nshop.dws_nshop_ulog_view
(
    user_id      string comment '用户id',
    device_num   string comment '设备号',
    device_type  string comment '设备类型',
    os           string comment '手机系统',
    os_version   string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier      string comment '电信运营商',
    network_type string comment '网络类型',
    area_code    string comment '地区编码',
    view_count   int comment '浏览次数'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/user/dws_nshop_ulog_view/';

insert overwrite table dws_nshop.dws_nshop_ulog_view partition(bdp_day = '20200419')
select
    user_id,
    device_num,
    device_type ,
    os          ,
    os_version  ,
    manufacturer,
    carrier     ,
    network_type,
    area_code,
    count(device_num) over(partition by device_num) as view_count
from dwd_nshop.dwd_nshop_actlog_pdtview
where bdp_day = '20200419';

select * from dws_nshop.dws_nshop_ulog_view limit 10;



-- 创建用户查询dws
create external table if not exists dws_nshop.dws_nshop_ulog_search
(
    user_id      string comment '用户id',
    device_num   string comment '设备号',
    device_type  string comment '设备类型',
    os           string comment '手机系统',
    os_version   string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier      string comment '电信运营商',
    network_type string comment '网络类型',
    area_code    string comment '地区编码',
    search_count int comment '搜索次数'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/user/dws_nshop_ulog_search/';

insert overwrite table dws_nshop.dws_nshop_ulog_search partition (bdp_day = '20200419')
select
    user_id,
    device_num,
    device_type ,
    os          ,
    os_version  ,
    manufacturer,
    carrier     ,
    network_type,
    area_code,
    count(device_num) over(partition by device_num) as search_count
from dwd_nshop.dwd_nshop_actlog_pdtsearch
where bdp_day = '20200419';


-- 创建用户关注dws
create external table if not exists dws_nshop.dws_nshop_ulog_comment
(
    user_id              string comment '用户id',
    device_num           string comment '设备号',
    device_type          string comment '设备类型',
    os                   string comment '手机系统',
    os_version           string comment '手机系统版本',
    manufacturer         string comment '手机制造商',
    carrier              string comment '电信运营商',
    network_type         string comment '网络类型',
    area_code            string comment '地区编码',
    comment_count        int comment '关注次数',
    comment_target_count int comment '关注产品次数',
    ct                   bigint comment '产生时间'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/user/dws_nshop_ulog_comment/';

insert overwrite table dws_nshop.dws_nshop_ulog_comment partition (bdp_day = '20200419')
select
    user_id,
    device_num,
    device_type  ,
    os           ,
    os_version   ,
    manufacturer ,
    carrier      ,
    network_type ,
    area_code,
    count(target_id) over(partition by target_id) as commnet_count, --关注次数
--     count(distinct target_id) over(partition by target_id) as comment_target_count, --关注产品次数
    1,
    ct
from dwd_nshop.dwd_actlog_product_comment
where bdp_day = '20200419';

select * from dws_nshop.dws_nshop_ulog_comment limit 10;




-- 创建商家用户交互记录宽表
create external table if not exists dws_nshop.dws_nshop_supplier_user
(
    supplier_id       string comment '商家id',
    supplier_type     int comment '供应商类型：1.自营，2.官方 3其他',
    view_count        int comment '浏览次数',
    comment_users     int comment '关注人数',
    comment_area_code int comment '关注地区数量',
    ct                bigint comment '产生时间'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/supplier/dws_nshop_supplier_user/';


set hive.exec.mode.local.auto=true;
-- 统计商家维度下用户的浏览次数
with pgview as (
    select su.supplier_code,
           su.supplier_type,
           count(*) as view_count
    from dwd_nshop.dwd_nshop_actlog_pdtview pv
             join ods_nshop.dim_pub_page pp
                  on pv.target_id = pp.page_code
                      and pp.page_type = '4'
             join ods_nshop.dim_pub_product pr
                  on pp.page_target = pr.product_code
             join ods_nshop.dim_pub_supplier su
                  on pr.supplier_code = su.supplier_code
    where bdp_day = '20200419'
    group by su.supplier_code, su.supplier_type
),
-- 统计商家维度下的关注数,关注地区数
prcomment as (
select
    su.supplier_code,
    su.supplier_type,
    count(distinct pc.user_id) as comment_users,
    count(distinct pc.area_code) as comment_area_code
from dwd_nshop.dwd_actlog_product_comment pc
join ods_nshop.dim_pub_page pp
    on pc.target_id = pp.page_code
    and pp.page_type = '4'
join ods_nshop.dim_pub_product pr
    on pp.page_target = pr.product_code
join ods_nshop.dim_pub_supplier su
    on pr.supplier_code = su.supplier_code
where pc.bdp_day = '20200419'
group by su.supplier_code, su.supplier_type
)
-- 整合指标到dws表  商家用户交互记录宽表
insert overwrite table dws_nshop.dws_nshop_supplier_user partition(bdp_day='20200419')
select
    pgview.supplier_code,
    pgview.supplier_type,
    pgview.view_count,
    prcomment.comment_users,
    prcomment.comment_area_code,
    current_timestamp() as ct
from pgview
join prcomment on pgview.supplier_code = prcomment.supplier_code
and pgview.supplier_type = prcomment.supplier_type;


select * from dws_nshop.dws_nshop_supplier_user limit 10;


create external table if not exists dws_nshop.dws_nshop_supplier_sales
(
    supplier_id            string comment '商家id',
    supplier_type          int comment '供应商类型：1.自营，2.官方 3其他',
    sales_users            int comment '购物人数',
    sales_users_area       int comment '购物地区数量',
    sales_orders           int comment '购物订单数',
    salaes_orders_pay      DECIMAL(10, 1) comment '订单金额',
    salaes_orders_district DECIMAL(10, 1) comment '订单优惠金额',
    ct                     bigint comment '产生时间'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/nshop/dws/supplier/dws_nshop_supplier_sales/';


-- 商家日流水宽表
insert overwrite table dws_nshop.dws_nshop_supplier_sales partition(bdp_day='20200419')
select
    su.supplier_code as supplier_id,
    su.supplier_type,
    count(distinct od.customer_id) as sales_users,
    count(distinct od.consignee_zipcode) as sales_users_area,
    count(distinct od.order_id) as sales_orders,
    sum(od.product_cnt*od.product_price) as salaes_orders_pay,
    sum(od.district_money) as salaes_orders_district,
    current_timestamp() as ct
from dwd_nshop.dwd_nshop_orders_details od
join ods_nshop.dim_pub_supplier su
on od.supplier_code = su.supplier_code
group by su.supplier_code, su.supplier_type;

select * from dws_nshop.dws_nshop_supplier_sales limit 10;




ads


