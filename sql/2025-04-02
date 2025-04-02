-- 2025-04-02
set hive.exec.mode.local.auto = true;
use gmall;
show databases ;
--------------------------------ods-------------------------------------------
drop table if exists gmall.ods_order_info;
create external table if not exists gmall.ods_order_info
(
    id                     string comment '编号',
    consignee              string comment '收货人',
    consignee_tel          string comment '收件人电话',
    total_amount           string comment '总金额',
    order_status           string comment '订单状态',
    user_id                string comment '用户id',
    payment_way            string comment '付款方式',
    delivery_address       string comment '送货地址',
    order_comment          string comment '订单备注',
    out_trade_no           string comment '订单交易编号（第三方支付用)',
    trade_body             string comment '订单描述(第三方支付用)',
    create_time            string comment '创建时间',
    operate_time           string comment '操作时间',
    expire_time            string comment '失效时间',
    process_status         string comment '进度状态',
    tracking_no            string comment '物流单编号',
    parent_order_id        string comment '父订单编号',
    img_url                string comment '图片路径',
    province_id            string comment '地区',
    activity_reduce_amount string comment '促销金额',
    coupon_reduce_amount   string comment '优惠券',
    original_total_amount  string comment '原价金额',
    feight_fee             string comment '运费',
    feight_fee_reduce      string comment '运费减免',
    refundable_time        string comment '可退款日期（签收后30天）'
) COMMENT '订单表'
    row format delimited fields terminated by '\t'
    STORED as textfile
    location '/warehouse/gmall/ods/ods_order_info/'
    TBLPROPERTIES ('compression' = "SNAPPY");
load data inpath "/warehouse/lb/gmall/db/ods/order_info" overwrite into table ods_order_info ;
select * from ods_order_info;

drop table if exists gmall.ods_order_detail;
create external table if not exists gmall.ods_order_detail
(
    id                    string comment '编号',
    order_id              string comment '订单编号',
    sku_id                string comment 'sku_id',
    sku_name              string comment 'sku名称（冗余)',
    img_url               string comment '图片名称（冗余)',
    order_price           string comment '购买价格(下单时sku价格）',
    sku_num               string comment '购买个数',
    create_time           string comment '创建时间',
    source_type           string comment '来源类型',
    source_id             string comment '来源编号',
    split_total_amount    string comment '',
    split_activity_amount string comment '',
    split_coupon_amount   string comment ''
) COMMENT '订单详情表'
    row format delimited fields terminated by '\t'
    STORED as textfile
    location '/warehouse/gmall/ods/ods_order_detail/'
    TBLPROPERTIES ('compression' = "SNAPPY");
load data inpath "/warehouse/lb/gmall/db/ods/order_detail" overwrite into table ods_order_detail ;
select * from ods_order_detail;

drop table if exists gmall.ods_sku_info;
create external table if not exists gmall.ods_sku_info
(
    id              string comment '库存id(itemID)',
    spu_id          string comment '商品id',
    price           string comment '价格',
    sku_name        string comment 'sku名称',
    sku_desc        string comment '商品规格描述',
    weight          string comment '重量',
    tm_id           string comment '品牌(冗余)',
    category3_id    string comment '三级分类id（冗余)',
    sku_default_img string comment '默认显示图片(冗余)',
    is_sale         string comment '是否销售（1：是 0：否）',
    create_time     string comment '创建时间'
) COMMENT 'SKU 商品表'
    row format delimited fields terminated by '\t'
    STORED as textfile
    location '/warehouse/gmall/ods/ods_sku_info/'
    TBLPROPERTIES ('compression' = "SNAPPY");
load data inpath "/warehouse/lb/gmall/db/ods/sku_info" overwrite into table ods_sku_info ;
select * from ods_sku_info;

drop table if exists gmall.ods_user_info;
create external table if not exists gmall.ods_user_info
(
    id           string comment '编号',
    login_name   string comment '用户名称',
    nick_name    string comment '用户昵称',
    passwd       string comment '用户密码',
    name         string comment '用户姓名',
    phone_num    string comment '手机号',
    email        string comment '邮箱',
    head_img     string comment '头像',
    user_level   string comment '用户级别',
    birthday     string comment '用户生日',
    gender       string comment '性别 M男,F女',
    create_time  string comment '创建时间',
    operate_time string comment '修改时间',
    status       string comment '状态'
) COMMENT '用户表'
    row format delimited fields terminated by '\t'
    STORED as textfile
    location '/warehouse/gmall/ods/ods_user_info/'
    TBLPROPERTIES ('compression' = "SNAPPY");
load data inpath "/warehouse/lb/gmall/db/ods/user_info" overwrite into table ods_user_info;
select *
from ods_user_info;

drop table if exists gmall.ods_base_trademark_full;
CREATE EXTERNAL TABLE ods_base_trademark_full
(
    `id`       STRING COMMENT '编号',
    `tm_name`  STRING COMMENT '品牌名称',
    `logo_url` STRING COMMENT '品牌logo的图片路径'
) COMMENT '品牌表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED as textfile
    LOCATION '/warehouse/gmall/ods/ods_base_trademark_full/'
    TBLPROPERTIES ('compression' = "SNAPPY");
select * from ods_base_trademark_full;
load data inpath "/warehouse/lb/gmall/db/ods/base_trademark" overwrite into table ods_base_trademark_full ;
select * from ods_base_trademark_full;

select * from gmall.dim_activity_rule_info;

--------------------------------ads-------------------------------------------
-- 复购率核心指标表
drop table if exists gmall.ads_repurchase_rate;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ads_repurchase_rate
(
    stat_date        STRING COMMENT '统计日期',
    user_type        STRING COMMENT '用户类型',
    time_window      STRING COMMENT '统计窗口',
    repurchase_users BIGINT COMMENT '复购用户数',
    total_users      BIGINT COMMENT '总购买用户数',
    repurchase_rate  DECIMAL(5, 2) COMMENT '复购率(%)'
) COMMENT '用户复购率统计表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED as PARQUET
    LOCATION '/warehouse/gmall/ads/ads_repurchase_rate/'
    TBLPROPERTIES ('compression' = "SNAPPY");
select *
from ads_repurchase_rate;

-- 3天用户复购率
INSERT OVERWRITE TABLE gmall.ads_repurchase_rate
SELECT current_date                                     as stat_date,
       'All Users'                                      as user_type,
       '3 days'                                         as time_window,
       -- 统计复购用户数
       SUM(case when order_count > 1 then 1 else 0 end) as repurchase_users,
       -- 统计总购买用户数
       COUNT(DISTINCT user_id)                          as total_users,
       -- 计算复购率
       ROUND(
               SUM(case when order_count > 1 then 1 else 0 end) / COUNT(DISTINCT user_id) * 100,
               2
       )                                                as repurchase_rate
FROM (SELECT user_id,
             COUNT(*) as order_count
      FROM gmall.ods_order_info
      WHERE
          -- 筛选 3 天内的订单
          create_time >= current_date - interval 3 day
      GROUP BY user_id) subquery;

select * from gmall.ods_order_info;

select * from ads_repurchase_rate;

-- 商品销售排行表
drop table if exists gmall.ads_top_merchandise_sales;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ads_top_merchandise_sales
(
    id            STRING,
    sku_name      STRING,
    total_sold    STRING,
    total_revenue string
) COMMENT '用户复购率统计表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED as PARQUET
    LOCATION '/warehouse/gmall/ads/ads_top_merchandise_sales/'
    TBLPROPERTIES ('compression' = "SNAPPY");
select *
from ads_top_merchandise_sales;

--商品销售排行榜
INSERT OVERWRITE TABLE gmall.ads_top_merchandise_sales
SELECT
    s.id,
    s.sku_name,
    COUNT(d.order_id) as total_sold,
    SUM(d.sku_num * d.order_price) as total_revenue
FROM
    gmall.ods_order_detail d
        JOIN
    gmall.ods_sku_info s ON d.sku_id = s.id
GROUP BY
    s.id, s.sku_name
ORDER BY
    total_revenue DESC
LIMIT 10;

-- 转化率指标表
drop table if exists gmall.ads_user_order_conversion_rate;
CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ads_user_order_conversion_rate
(
    stat_date       string COMMENT '统计日期',
    total_users     bigint COMMENT '总用户数',
    ordered_users   bigint COMMENT '已下单用户数',
    conversion_rate decimal(5, 2) COMMENT '转化率'
) COMMENT '用户下单转化率指标表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED as PARQUET
    LOCATION '/warehouse/gmall/ads/ads_repurchase_rate/'
    TBLPROPERTIES ('compression' = "SNAPPY");
select *
from ads_user_order_conversion_rate;

-- 转化率
INSERT OVERWRITE TABLE gmall.ads_user_order_conversion_rate
SELECT current_date()                                                     as stat_date,
       -- 统计总用户数
       COUNT(DISTINCT ui.id)                                              as total_users,
       -- 统计已下单用户数
       COUNT(DISTINCT oi.user_id)                                         as ordered_users,
       -- 计算转化率
       ROUND(COUNT(DISTINCT oi.user_id) / COUNT(DISTINCT ui.id) * 100, 2) as conversion_rate
FROM gmall.ods_user_info ui
         LEFT JOIN
     gmall.ods_order_info oi
     ON
         ui.id = oi.user_id;
-- 优化后的转化率
INSERT OVERWRITE TABLE gmall.ads_user_order_conversion_rate
SELECT current_date()                                                     as stat_date,
       COUNT(DISTINCT ui.id)                                              as total_users,
       COUNT(DISTINCT oi.user_id)                                         as ordered_users,
       ROUND(COUNT(DISTINCT oi.user_id) / COUNT(DISTINCT ui.id) * 100, 2) as conversion_rate
FROM gmall.ods_user_info ui
         LEFT JOIN
     gmall.ods_order_info oi
     ON
         ui.id = oi.user_id
WHERE oi.create_time >= date_sub(current_date(), 30);
