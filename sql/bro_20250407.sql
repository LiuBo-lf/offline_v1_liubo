set hive.exec.mode.local.auto = true;
use gmall;
show databases ;

----------------------ods------------------------------------------
drop table if exists gmall.ods_sku_info;
create table if not exists ods_sku_info
(
    `id`              bigint COMMENT '库存id(itemID)',
    `spu_id`          bigint COMMENT '商品id',
    `price`           decimal(10, 0) COMMENT '价格',
    `sku_name`        string COMMENT 'sku名称',
    `sku_desc`        string COMMENT '商品规格描述',
    `weight`          decimal(10, 2) COMMENT '重量',
    `tm_id`           bigint COMMENT '品牌(冗余)',
    `category3_id`    bigint COMMENT '三级分类id（冗余)',
    `sku_default_img` string COMMENT '默认显示图片(冗余)',
    `is_sale`         int COMMENT '是否销售（1：是 0：否）',
    `create_time`     string COMMENT '创建时间'
    ) partitioned by (dt string)
    row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_sku_info"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_sku_info"
    overwrite into table ods_sku_info partition (dt = "2025-04-02");
select *
from ods_sku_info;

drop table if exists gmall.ods_spu_info;
create table if not exists ods_spu_info
(
    `id`           bigint COMMENT '商品id',
    `spu_name`     string COMMENT '商品名称',
    `description`  string COMMENT '商品描述(后台简述）',
    `category3_id` bigint COMMENT '三级分类id',
    `tm_id`        bigint COMMENT '品牌id'
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_spu_info"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_spu_info"
    overwrite into table ods_spu_info;

drop table if exists gmall.ods_order_info;
create table if not exists ods_order_info
(
    `id`                     bigint COMMENT '编号',
    `consignee`              string COMMENT '收货人',
    `consignee_tel`          string COMMENT '收件人电话',
    `total_amount`           decimal(10, 2) COMMENT '总金额',
    `order_status`           string COMMENT '订单状态',
    `user_id`                bigint COMMENT '用户id',
    `payment_way`            string COMMENT '付款方式',
    `delivery_address`       string COMMENT '送货地址',
    `order_comment`          string COMMENT '订单备注',
    `out_trade_no`           string COMMENT '订单交易编号（第三方支付用)',
    `trade_body`             string COMMENT '订单描述(第三方支付用)',
    `create_time`            string COMMENT '创建时间',
    `operate_time`           string COMMENT '操作时间',
    `expire_time`            string COMMENT '失效时间',
    `process_status`         string COMMENT '进度状态',
    `tracking_no`            string COMMENT '物流单编号',
    `parent_order_id`        bigint COMMENT '父订单编号',
    `img_url`                string COMMENT '图片路径',
    `province_id`            int COMMENT '地区',
    `activity_reduce_amount` decimal(16, 2) COMMENT '促销金额',
    `coupon_reduce_amount`   decimal(16, 2) COMMENT '优惠券',
    `original_total_amount`  decimal(16, 2) COMMENT '原价金额',
    `feight_fee`             decimal(16, 2) COMMENT '运费',
    `feight_fee_reduce`      decimal(16, 2) COMMENT '运费减免',
    `refundable_time`        string COMMENT '可退款日期（签收后30天）'
    ) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_order_info"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_order_info"
    overwrite into table ods_order_info;

drop table if exists gmall.ods_order_detail;
create external table if not exists ods_order_detail
(
    `id`                    bigint COMMENT '编号',
    `order_id`              bigint COMMENT '订单编号',
    `sku_id`                bigint COMMENT 'sku_id',
    `sku_name`              string COMMENT 'sku名称（冗余)',
    `img_url`               string COMMENT '图片名称（冗余)',
    `order_price`           decimal(10, 2) COMMENT '购买价格(下单时sku价格）',
    `sku_num`               bigint COMMENT '购买个数',
    `create_time`           string COMMENT '创建时间',
    `source_type`           string COMMENT '来源类型',
    `source_id`             bigint COMMENT '来源编号',
    `split_total_amount`    decimal(16, 2),
    `split_activity_amount` decimal(16, 2),
    `split_coupon_amount`   decimal(16, 2)
)row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_order_detail"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/order_detail" overwrite into table ods_order_detail;

select * from ods_order_detail;
MSCK REPAIR TABLE ods_order_detail;

drop table if exists gmall.ods_base_dic;
create table if not exists ods_base_dic
(
    `dic_code`     string COMMENT '编号',
    `dic_name`     string COMMENT '编码名称',
    `parent_code`  string COMMENT '父编号',
    `create_time`  string COMMENT '创建日期',
    `operate_time` string COMMENT '修改日期'
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_base_dic"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_order_info"
    overwrite into table ods_base_dic;

drop table if exists gmall.ods_coupon_use;
create table if not exists ods_coupon_use
(
    `id`            bigint COMMENT '编号',
    `coupon_id`     bigint COMMENT '购物券ID',
    `user_id`       bigint COMMENT '用户ID',
    `order_id`      bigint COMMENT '订单ID',
    `coupon_status` string COMMENT '购物券状态（1：未使用 2：已使用）',
    `create_time`   string COMMENT '创建时间',
    `get_time`      string COMMENT '获取时间',
    `using_time`    string COMMENT '使用时间',
    `used_time`     string COMMENT '支付时间',
    `expire_time`   string COMMENT '过期时间'
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_coupon_use"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_order_info"
    overwrite into table ods_coupon_use;

drop table if exists gmall.ods_base_province;
create table if not exists ods_base_province
(
    `id`         bigint COMMENT 'id',
    `name`       string COMMENT '省名称',
    `region_id`  string COMMENT '大区id',
    `area_code`  string COMMENT '行政区位码',
    `iso_code`   string COMMENT '国际编码',
    `iso_3166_2` string COMMENT 'ISO3166编码'
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_base_province"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_base_province"
    overwrite into table ods_base_province;


drop table if exists ods_base_trademark;
create table if not exists ods_base_trademark
(
    `id`       bigint COMMENT '编号',
    `tm_name`  string COMMENT '属性值',
    `logo_url` string COMMENT '品牌logo的图片路径'
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ods/ods_base_trademark"
    tblproperties ("orc.compress" = "snappy");
load data inpath "/warehouse/lb/gmall/db/ods/ods_base_trademark"
    overwrite into table ods_base_trademark;

select * from ods_sku_info;
select * from ods_spu_info;
select * from ods_order_info;
select * from ods_order_detail;

select * from ods_base_dic;
select * from ods_coupon_use;
select * from ods_base_province;

----------------------------dwd------------------------------------
drop table if exists gmall.dwd_trade_order_detail;
create table if not exists dwd_trade_order_detail
(
    `id`                    bigint COMMENT '编号',
    `order_id`              bigint COMMENT '订单编号',
    `sku_id`                bigint COMMENT 'sku_id',
    `sku_name`              string COMMENT 'sku名称（冗余)',
    `img_url`               string COMMENT '图片名称（冗余)',
    `order_price`           decimal(10, 2) COMMENT '购买价格(下单时sku价格）',
    `sku_num`               bigint COMMENT '购买个数',
    `create_time`           string COMMENT '创建时间',
    `source_type_id`        string COMMENT '来源类型!!!!',
    `source_id`             bigint COMMENT '来源编号',
--     `split_total_amount`    decimal(16, 2),
    `split_activity_amount` decimal(16, 2),
    `split_coupon_amount`   decimal(16, 2),
    --指标需求拉宽
    tm_id                   string comment "品牌id",                                           -- 拉宽(加字段!)品牌 (品牌的复购率)
    tm_name                 string comment "品牌name",                                         -- 拉宽(加字段!)品牌 (品牌的复购率)
    `user_id`               bigint COMMENT '用户id',                                           --计算品牌的复购率(同一个品牌  同一个人 购买了2次以上才叫复购)
    `source_type_name`      string COMMENT '来源类型name',                                     --(订单来源数量和占比)
    `province_name`         string COMMENT '省份name(省份名称不可能重复,所以不需要拉宽省份id)' --(各省份交易统计)
    )
    row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/dwd/dwd_trade_order_detail"
    tblproperties ("orc.compress" = "snappy");
select * from dwd_trade_order_detail;

set hive.exec.dynamic.partition.mode=nonstrict;
select * from ods_order_detail;
-- DWD层下单事务事实表
insert overwrite table dwd_trade_order_detail
select od.id,
       od.order_id,
       od.sku_id,
       od.sku_name,
--        od.`img_url` ,
       od.order_price,
       od.sku_num,
       od.create_time,
       od.source_type,
       od.source_id,
--        od.split_total_amount,
       od.split_activity_amount,
       od.split_coupon_amount,
       si.tm_id,                     --  拉宽 (品牌的复购率)
       bt.tm_name,
       oi.user_id,                   -- 计算品牌的复购率(同一个品牌  同一个人 购买了2次以上才叫复购)
       bd.dic_name,                  --(订单来源数量和占比)
       bp.name,                      -- 各省份交易统计)
       substr(od.create_time, 1, 10) --截取分区字段
from ods_order_detail od
         left join ods_sku_info si on od.sku_id = si.id
         left join ods_base_trademark bt on si.tm_id = bt.id
         left join ods_order_info oi on od.order_id = oi.id
         left join ods_base_province bp on oi.province_id = bp.id
         left join ods_base_dic bd on od.source_type = bd.dic_code;
select * from dwd_trade_order_detail;
select * from ods_order_detail;
set hive.exec.mode.local.auto = true;
use gmall;

drop table if exists gmall.dwd_tool_coupon_get;
create table if not exists dwd_tool_coupon_get
(
    `id`            bigint COMMENT '编号',
    `coupon_id`     bigint COMMENT '购物券ID',
    `user_id`       bigint COMMENT '用户ID',
    `order_id`      bigint COMMENT '订单ID',
    `coupon_status` string COMMENT '购物券状态（1：未使用 2：已使用）',
--     `create_time`   string COMMENT '创建时间',
    `get_time`      string COMMENT '获取时间',
    `using_time`    string COMMENT '使用时间',
    `used_time`     string COMMENT '支付时间',
    `expire_time`   string COMMENT '过期时间'
)
    row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/dwd/dwd_tool_coupon_get"
    tblproperties ("orc.compress" = "snappy");
select * from dwd_tool_coupon_get;
-- DWD层优惠券领取事实表
insert overwrite table gmall.dwd_tool_coupon_get
select
    cu.id,
    cu.coupon_id,
    cu.user_id,
    cu.order_id,
    cu.coupon_status,
--        cu.`create_time`,
    cu.get_time,
    cu.using_time,
    cu.used_time,
    cu.expire_time,
    substr(cu.get_time, 1, 10)
from ods_coupon_use cu;
select *
from dwd_tool_coupon_get;
select * from ods_coupon_use;

--------------------------dws--------------------------------------
drop table if exists gmall.dws_province_order_1d;
create table if not exists dws_province_order_1d
(

    province_name      string,
    order_num          string,
    order_total_amount decimal(19, 2)
    ) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/dws/dws_province_order_1d"
    tblproperties ("orc.compress" = "snappy");
insert into dws_province_order_1d
select
    province_name,
    count(distinct order_id), --一个订单对应多个订单明细 统计订单数量的时候要去重
    sum(split_activity_amount)   --实付价格
from dwd_trade_order_detail
group by province_name;
select *
from dws_province_order_1d;
select * from dwd_trade_order_detail;

drop table if exists gmall.dws_province_order_30d;
create table if not exists dws_province_order_30d
(

    province_name      string,
    order_num          bigint,
    order_total_amount decimal(19, 2)
    ) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/dws/dws_province_order_30d"
    tblproperties ("orc.compress" = "snappy");
insert into dws_province_order_30d
select
    province_name,
    sum(order_num), --把每天的数量 加一起 使用sum  不要使用count
    sum(order_total_amount)
from dws_province_order_1d -- 题目明确要求依照最近1日统计最近n日 不要查询dwd层
-- where dt >= date_sub("2025-03-05", 29) --最近30日
group by province_name;

select *
from dws_province_order_1d;
select *
from dws_province_order_30d;

--------------------------ads--------------------------------------
--商品主题
--各品牌复购率
-- 各品牌复购率= 各品牌的复购人数/各品牌购买人数
drop table if exists gmall.ads_tm_fglv_td;
create table if not exists ads_tm_fglv_td
(
    tm_id   string,
    tm_name string,
    fglv    string
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ads/ads_tm_fglv_td"
    tblproperties ("orc.compress" = "snappy");
insert overwrite table ads_tm_fglv_td
select t1.tm_id,
       t1.tm_name,
       nvl(t2.cc / t1.cc * 100, 0)
from (
         -- 各品牌购买人数
         select tm_id,
                tm_name,
                count(distinct user_id) cc
         from dwd_trade_order_detail
         group by tm_id, tm_name) t1
         left join(
    --各品牌的复购人数
    select tm_id,
           tm_name,
           count(user_id) cc
    from (
             -- 各个品牌复购的明细
             select tm_id,
                    tm_name,
                    user_id
             from dwd_trade_order_detail
             group by tm_id, tm_name, user_id -- 购买的次数在>=2
             having count(*) >= 2) tt
    group by tm_id, tm_name) t2 on t1.tm_id = t2.tm_id;
select *
from ads_tm_fglv_td;

--订单来源数量和占比
drop table if exists gmall.ads_source_order_num_zb_td;
create table if not exists ads_source_order_num_zb_td
(
    source_type_id   string,
    source_type_name string,
    order_num        bigint,
    order_zb         decimal(5, 2)
    ) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ads/ads_source_order_num_zb_td"
    tblproperties ("orc.compress" = "snappy");

insert overwrite table ads_source_order_num_zb_td
select source_type_id,
       source_type_name,
       count(distinct order_id) ,                                                                     --按照来源查询的订单数量
       count(distinct order_id)/
       (select
            count(distinct order_id)
        from dwd_trade_order_detail
       ) * 100  --占比=源查询的订单数量/总数量
from dwd_trade_order_detail
group by source_type_id,
         source_type_name;
--优化后的sql
INSERT OVERWRITE TABLE ads_source_order_num_zb_td
SELECT
    main.source_type_id,
    main.source_type_name,
    main.order_count,  -- 分组订单数
    (main.order_count / total.total_order_count) * 100 AS ratio  -- 占比
FROM (
         SELECT
             source_type_id,
             source_type_name,
             COUNT(DISTINCT order_id) AS order_count  -- 分组统计订单数
         FROM dwd_trade_order_detail
         GROUP BY source_type_id, source_type_name
     ) main
         CROSS JOIN (
    SELECT
        COUNT(DISTINCT order_id) AS total_order_count  -- 总订单数
    FROM dwd_trade_order_detail
) total;

select *
from gmall.ads_source_order_num_zb_td;

select * from dwd_trade_order_detail;

-- 交易主题
-- 交易综合统计
drop table if exists gmall.ads_order_num_amount_td;
create table if not exists ads_order_num_amount_td
(
    order_num          bigint,
    order_total_amount string
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ads/ads_order_num_amount_td"
    tblproperties ("orc.compress" = "snappy");
insert overwrite table ads_order_num_amount_td
select sum(order_num),
       sum(order_total_amount)
from dws_province_order_1d;
select *
from ads_order_num_amount_td;

-- 各省份交易统计
drop table if exists gmall.ads_province_order_num_amount_td;
create table if not exists ads_province_order_num_amount_td
(
    province_name      string,
    order_num          bigint,
    order_total_amount string
) row format delimited fields terminated by "\t"
    stored as orc
    location "/warehouse/gmall/ads/ads_province_order_num_amount_td"
    tblproperties ("orc.compress" = "snappy");
insert overwrite table ads_province_order_num_amount_td
select province_name,
       sum(order_num),
       sum(order_total_amount)
from dws_province_order_1d
group by province_name;

-- 各省份交易统计
select *
from ads_province_order_num_amount_td;
-- 交易综合统计
select *
from ads_order_num_amount_td;
--订单来源数量和占比
select *
from ads_source_order_num_zb_td;
-- 各品牌复购率
select *
from ads_tm_fglv_td;