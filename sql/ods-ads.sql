-- 设置本地运行模式
set hive.exec.mode.local.auto = true;
show databases ;
use gmall;
show tables ;
-----------------------------ods层-----------------------------
drop table if exists ods_order_info;
create external table ods_order_info (
                                         `id` string COMMENT '订单号',
                                         `final_total_amount` decimal(16,2) COMMENT '订单金额',
                                         `order_status` string COMMENT '订单状态',
                                         `user_id` string COMMENT '用户 id',
                                         `out_trade_no` string COMMENT '支付流水号',
                                         `create_time` string COMMENT '创建时间',
                                         `operate_time` string COMMENT '操作时间',
                                         `province_id` string COMMENT '省份 ID',
                                         `benefit_reduce_amount` decimal(16,2) COMMENT '优惠金额',
                                         `original_total_amount` decimal(16,2) COMMENT '原价金额',
                                         `feight_fee` decimal(16,2) COMMENT '运费'

) COMMENT '订单表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    STORED AS
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_order_info/';
load data inpath "/origin_data/gmall/db/order_info" overwrite into table ods_order_info partition (dt= "2020-06-16");
load data inpath "/origin_data/gmall/db/order_info" overwrite into table ods_order_info partition (dt= "2020-06-17");
load data inpath "/origin_data/gmall/db/order_info" overwrite into table ods_order_info partition (dt= "2020-06-18");
select * from ods_order_info;

drop table if exists ods_order_detail;
create external table ods_order_detail(
                                          `id` string COMMENT '编号',
                                          `order_id` string COMMENT '订单号',
                                          `user_id` string COMMENT '用户 id',
                                          `sku_id` string COMMENT '商品 id',
                                          `sku_name` string COMMENT '商品名称',
                                          `order_price` decimal(16,2) COMMENT '商品价格',
                                          `sku_num` bigint COMMENT '商品数量',
                                          `create_time` string COMMENT '创建时间',
                                          `source_type` string COMMENT '来源类型',
                                          `source_id` string COMMENT '来源编号'
) COMMENT '订单详情表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    STORED AS
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_order_detail/';
load data inpath "/origin_data/gmall/db/order_detail" overwrite into table ods_order_detail partition (dt= "2020-06-16");
load data inpath "/origin_data/gmall/db/order_detail" overwrite into table ods_order_detail partition (dt= "2020-06-17");
load data inpath "/origin_data/gmall/db/order_detail" overwrite into table ods_order_detail partition (dt= "2020-06-18");
select * from ods_order_detail;

drop table if exists ods_sku_info;
create external table ods_sku_info(
                                      `id` string COMMENT 'skuId',
                                      `spu_id` string COMMENT 'spuid',
                                      `price` decimal(16,2) COMMENT '价格',
                                      `sku_name` string COMMENT '商品名称',
                                      `sku_desc` string COMMENT '商品描述',
                                      `weight` string COMMENT '重量',
                                      `tm_id` string COMMENT '品牌 id',
                                      `category3_id` string COMMENT '品类 id',
                                      `create_time` string COMMENT '创建时间'
) COMMENT 'SKU 商品表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    STORED AS
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_sku_info/';
load data inpath "/origin_data/gmall/db/sku_info" overwrite into table ods_sku_info partition (dt= "2020-06-16");
load data inpath "/origin_data/gmall/db/sku_info" overwrite into table ods_sku_info partition (dt= "2020-06-17");
load data inpath "/origin_data/gmall/db/sku_info" overwrite into table ods_sku_info partition (dt= "2020-06-18");
select * from ods_sku_info;

drop table if exists ods_user_info;
create external table ods_user_info(
                                       `id` string COMMENT '用户 id',
                                       `name` string COMMENT '姓名',
                                       `birthday` string COMMENT '生日',
                                       `gender` string COMMENT '性别',
                                       `email` string COMMENT '邮箱',
                                       `user_level` string COMMENT '用户等级',
                                       `create_time` string COMMENT '创建时间',
                                       `operate_time` string COMMENT '操作时间'
) COMMENT '用户表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    STORED AS
        INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_user_info/';

load data inpath "/origin_data/gmall/db/user_info" overwrite into table ods_user_info partition (dt= "2020-06-16");
load data inpath "/origin_data/gmall/db/user_info" overwrite into table ods_user_info partition (dt= "2020-06-17");
load data inpath "/origin_data/gmall/db/user_info" overwrite into table ods_user_info partition (dt= "2020-06-18");
select * from ods_user_info;

-----------------------------dwd层-----------------------------
-- 创建dwd_order_detail、dwd_order_info
DROP TABLE IF EXISTS dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail (
                                           `id` STRING COMMENT '订单编号',
                                           `order_id` STRING COMMENT '订单号',
                                           `user_id` STRING COMMENT '用户id',
                                           `sku_id` STRING COMMENT 'sku商品id',
                                           `province_id` STRING COMMENT '省份ID',
                                           `activity_id` STRING COMMENT '活动ID',
                                           `activity_rule_id` STRING COMMENT '活动规则ID',
                                           `coupon_id` STRING COMMENT '优惠券ID',
                                           `create_time` STRING COMMENT '创建时间',
                                           `source_type` STRING COMMENT '来源类型',
                                           `source_id` STRING COMMENT '来源编号',
                                           `sku_num` BIGINT COMMENT '商品数量',
                                           `original_amount` DECIMAL(16,2) COMMENT '原始价格',
                                           `split_activity_amount` DECIMAL(16,2) COMMENT '活动优惠分摊',
                                           `split_coupon_amount` DECIMAL(16,2) COMMENT '优惠券优惠分摊',
                                           `split_final_amount` DECIMAL(16,2) COMMENT '最终价格分摊'
) COMMENT '订单明细事实表表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_detail/'
    TBLPROPERTIES ("parquet.compression"="lzo");

select * from   dwd_order_detail;
DROP TABLE IF EXISTS dwd_order_info;
CREATE EXTERNAL TABLE dwd_order_info(
                                        `id` STRING COMMENT '编号',
                                        `order_status` STRING COMMENT '订单状态',
                                        `user_id` STRING COMMENT '用户ID',
                                        `province_id` STRING COMMENT '地区ID',
                                        `payment_way` STRING COMMENT '支付方式',
                                        `delivery_address` STRING COMMENT '邮寄地址',
                                        `out_trade_no` STRING COMMENT '对外交易编号',
                                        `tracking_no` STRING COMMENT '物流单号',
                                        `create_time` STRING COMMENT '创建时间(未支付状态)',
                                        `payment_time` STRING COMMENT '支付时间(已支付状态)',
                                        `cancel_time` STRING COMMENT '取消时间(已取消状态)',
                                        `finish_time` STRING COMMENT '完成时间(已完成状态)',
                                        `refund_time` STRING COMMENT '退款时间(退款中状态)',
                                        `refund_finish_time` STRING COMMENT '退款完成时间(退款完成状态)',
                                        `expire_time` STRING COMMENT '过期时间',
                                        `feight_fee` DECIMAL(16,2) COMMENT '运费',
                                        `feight_fee_reduce` DECIMAL(16,2) COMMENT '运费减免',
                                        `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免',
                                        `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免',
                                        `original_amount` DECIMAL(16,2) COMMENT '订单原始价格',
                                        `final_amount` DECIMAL(16,2) COMMENT '订单最终价格'
) COMMENT '订单事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_info/'
    TBLPROPERTIES ("parquet.compression"="lzo");
select * from dwd_order_info ;

DROP TABLE IF EXISTS dwd_user_info;
CREATE EXTERNAL TABLE dwd_user_info(
    `id` STRING COMMENT '用户id',
    `name` STRING COMMENT '姓名',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `email` STRING COMMENT '邮箱',
    `user_level` STRING COMMENT '用户等级',
    `create_time` STRING COMMENT '创建时间',
    `last_login_time` STRING COMMENT '最后登录时间'

 ) COMMENT '用户基础信息表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_user_info/'
    TBLPROPERTIES ("parquet.compression"="lzo");
select * from dwd_user_info;

DROP TABLE IF EXISTS dwd_sku_info;
CREATE EXTERNAL TABLE dwd_sku_info(
    `id` STRING COMMENT 'SKU id',
    `spu_id` STRING COMMENT 'SPU id',
    `price` DECIMAL(16,2) COMMENT '价格',
    `sku_name` STRING COMMENT '商品名称',
    `sku_desc` STRING COMMENT '商品描述',
    `weight` STRING COMMENT '重量',
    `tm_id` STRING COMMENT '品牌 id',
    `category3_id` STRING COMMENT '品类 id',
    `create_time` STRING COMMENT '创建时间'

 ) COMMENT 'SKU 商品表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_sku_info/'
    TBLPROPERTIES ("parquet.compression"="lzo");


DROP TABLE IF EXISTS dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail (
                                           order_id STRING COMMENT '订单号',
                                           user_id STRING COMMENT '用户ID',
                                           sku_id STRING COMMENT '商品ID',
                                           sku_name STRING COMMENT '商品名称',
                                           order_price DECIMAL(16,2) COMMENT '商品单价',
                                           sku_num BIGINT COMMENT '商品数量',
                                           final_total_amount DECIMAL(16,2) COMMENT '订单总金额',
                                           order_status STRING COMMENT '订单状态',
                                           province_id STRING COMMENT '省份ID',
                                           create_time STRING COMMENT '订单创建时间',
                                           operate_time STRING COMMENT '订单操作时间',
                                           benefit_reduce_amount DECIMAL(16,2) COMMENT '优惠金额',
                                           original_total_amount DECIMAL(16,2) COMMENT '原价金额',
                                           feight_fee DECIMAL(16,2) COMMENT '运费',
                                           source_type STRING COMMENT '来源类型',
                                           source_id STRING COMMENT '来源编号'
)
    PARTITIONED BY (dt STRING)  -- 按天分区
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC  -- 使用ORC列式存储
    LOCATION '/warehouse/gmall/dwd/dwd_order_detail'
    TBLPROPERTIES ("orc.compress"="SNAPPY");

DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info` (
                                             `id` string COMMENT '商品 id',
                                             `spu_id` string COMMENT 'spuid',
                                             `price` decimal(16,2) COMMENT '商品价格',
                                             `sku_name` string COMMENT '商品名称',
                                             `sku_desc` string COMMENT '商品描述',
                                             `weight` decimal(16,2) COMMENT '重量',
                                             `tm_id` string COMMENT '品牌 id',
                                             `tm_name` string COMMENT '品牌名称',
                                             `category3_id` string COMMENT '三级分类 id',
                                             `category2_id` string COMMENT '二级分类 id',
                                             `category1_id` string COMMENT '一级分类 id',
                                             `category3_name` string COMMENT '三级分类名称',
                                             `category2_name` string COMMENT '二级分类名称',
                                             `category1_name` string COMMENT '一级分类名称',
                                             `spu_name` string COMMENT 'spu 名称',
                                             `create_time` string COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_sku_info/'
    tblproperties ("parquet.compression"="lzo");
select * from dwd_dim_sku_info  ;

drop table if exists dwd_dim_user_info_his;
create external table dwd_dim_user_info_his(
                                               `id` string COMMENT '用户 id',
                                               `name` string COMMENT '姓名',
                                               `birthday` string COMMENT '生日',
                                               `gender` string COMMENT '性别',
                                               `email` string COMMENT '邮箱',
                                               `user_level` string COMMENT '用户等级',
                                               `create_time` string COMMENT '创建时间',
                                               `operate_time` string COMMENT '操作时间',
                                               `start_date` string COMMENT '有效开始日期',
                                               `end_date` string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
    tblproperties ("parquet.compression"="lzo");
select * from   dwd_dim_user_info_his;

-----------------------------------dws-------------------------------------------------
-- ③ DWS层构建
-- 结合ADS层指标构建DWS层宽表
-- 设计要点：
-- （1）DWS层的设计参考如下ADS层指标进行构建。
-- （2）DWS层的数据存储格式为orc列式存储lzo压缩格式。
-- （3）.  依据下单次数和下单金额等，编写SQL语句进行数据装载
select *
from dws_sku_action_daycount;
DROP TABLE IF EXISTS dws_user_action_daycount;
CREATE EXTERNAL TABLE dws_user_action_daycount(
     `dt` string COMMENT '统计日期',
     `user_id` string COMMENT '用户id',
     `user_level` string COMMENT '用户等级',
     `order_count` BIGINT COMMENT '下单次数',
     `order_amount` DECIMAL(16,2) COMMENT '下单金额',
     `payment_count` BIGINT COMMENT '支付次数',
     `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
     `refund_count` BIGINT COMMENT '退款次数',
     `refund_amount` DECIMAL(16,2) COMMENT '退款金额',
     `coupon_count` BIGINT COMMENT '优惠券使用次数',
     `coupon_amount` DECIMAL(16,2) COMMENT '优惠券使用金额'
)
    PARTITIONED BY (`dt` string)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_user_action_daycount/'
    TBLPROPERTIES ("parquet.compression"="lzo");
select  * from dws_user_action_daycount  ;

-- 每日商品行为
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount
(
    sku_id string comment 'sku_id',
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    payment_count bigint comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    refund_count bigint comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount decimal(16,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    favor_count bigint comment '被收藏次数',
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_sku_action_daycount/'
    tblproperties ("parquet.compression"="lzo");
select * from  dws_sku_action_daycount;
-- 最近30天发布的优惠券的补贴率
DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`          STRING COMMENT '统计日期',
    `coupon_id`   STRING COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `start_date`  STRING COMMENT '发布日期',
    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',
    `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '优惠券统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';

insert overwrite table ads_coupon_stats
select * from ads_coupon_stats
union
select
    '' dt,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from dws_trade_coupon_order_nd
where dt= "2020-06-18";
select * from ads_coupon_stats;


DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/ads/ads_user_retention/'
    TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table ads_user_retention
select * from ads_user_retention
union
select
    '2020-06-14' dt,
    login_date_first create_date, --新增日期
    datediff('2020-06-14',login_date_first) retention_day, --留存天数
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,--留存用户数量
    count(*) new_user_count,--新增用户数量
    cast(sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
    (
        select
            user_id,
            date_id login_date_first
        from dwd_user_register_inc  --取出历史七天的新增用户，注册即新增
        where dt>=date_add('2020-06-14',-7)
          and dt<'2020-06-14'
    )t1
        join
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td  --取出全量的登录用户(活跃用户)
        where dt='2020-06-14'
    )t2
    on t1.user_id=t2.user_id
group by login_date_first;




-- 创建ADS层品牌复购率表
DROP TABLE IF EXISTS ads_brand_repeat;
CREATE EXTERNAL TABLE ads_brand_repeat (
           brand_id STRING COMMENT '品牌ID',
           dt STRING COMMENT '统计日期',
           repeat_rate DECIMAL(5,2) COMMENT '复购率'
)
    LOCATION '/warehouse/gmall/ads/ads_brand_repeat';

-- 计算品牌复购率
WITH brand_purchase AS (
    SELECT
        sku.id,
        od.user_id,
        COUNT(DISTINCT od.order_id) AS order_count
    FROM dwd_order_detail od
             JOIN dwd_sku_info sku ON od.sku_id = sku.id
    WHERE od.dt= "2020-06-18"
    GROUP BY sku.id, od.user_id
)
INSERT OVERWRITE TABLE ads_brand_repeat
SELECT
    id,
    "2020-06-18" AS dt,
    ROUND(
            SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT user_id),
            2
    ) AS repeat_rate
FROM brand_purchase
GROUP BY id;

select  * from ads_user_retention;
select * from  ads_user_total;
select * from ads_order_total;
select * from ads_page_path;
select * from ads_user_retention;













