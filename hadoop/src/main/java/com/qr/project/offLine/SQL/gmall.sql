create database gmall;

show databases;

show tables;

-- 日志表
drop table if exists ods_log_inc;
create external table ods_log_inc(
                                     common struct<ar:string, ba:string, ch:string, `is`:string, md:string, mid:string, os:string, uid:string, vc:string> comment '公共信息',
                                     page struct<during_time: string, item: string, item_type: string, last_page_id: string, page_id: string, source_type: string> comment '页面信息',
                                     actions array<struct<action_id: string, item: string, item_type: string, ts: string>> comment '动作信息',
                                     displays array<struct<display_type: string, item: string, item_type: string, `order`: string, pos_id: string>> comment '曝光信息',
                                     `start` struct<entry: string, loading_time: string, open_ad_id: string, open_ad_ms: string, open_ad_skip_ms: string> comment '启动日志',
                                     err struct<error_code: string, msg: string> comment '异常日志',
                                     ts bigint comment '时间戳'
)comment "活动信息表"
    partitioned by (`dt` string)
    row format serde "org.apache.hadoop.hive.serde2.JsonSerDe"
    location "/warehouse/gmall/ods/ods_log_inc/";
load data inpath '/origin_data/gmall/log/topic_log/2020-06-14' into table ods_log_inc partition(dt = '2020-06-14');

-- 每日数据装载脚本 hdfs_to_ods_log.sh

-- 7.2 业务表
-- 7.2.1 活动信息表（全量表）
drop table if exists ods_activity_info_full;
create external table ods_activity_info_full(
                                                id bigint comment '活动id',
                                                activity_name string comment '活动名称',
                                                activity_type string comment '活动类型（1：满减，2：折扣）',
                                                activity_desc string comment '活动描述',
                                                start_time string comment '开始时间',
                                                end_time string comment '结束时间',
                                                create_time string comment '创建时间'
)comment '活动信息表'
    partitioned by (`dt` string)
    row format delimited fields terminated by "\t"
        null defined as ''
    location '/warehouse/gmall/ods/ods_activity_info_full/';

-- 7.2.2 活动规则表（全量表）
drop table if exists ods_activity_rule_full;
create external table ods_activity_rule_full(
                                                id bigint comment '编号',
                                                activity_id  string comment '类型',
                                                activity_type  string comment '活动类型',
                                                condition_amount string comment '满减金额',
                                                condition_num  string comment '满减件数',
                                                benefit_amount string comment '优惠金额',
                                                benefit_discount string comment '优惠折扣',
                                                benefit_level  string comment '优惠级别'
)comment '活动规则表'
    partitioned by (`dt` string)
    row format delimited fields terminated by "\t"
        null defined as ''
    location "/warehouse/gmall/ods/ods_activity_rule_full/";

-- 7.2.3 一级品类表（全量表）
DROP TABLE IF EXISTS ods_base_category1_full;
CREATE EXTERNAL TABLE ods_base_category1_full
(
    `id`   STRING COMMENT '编号',
    `name` STRING COMMENT '分类名称'
) COMMENT '一级品类表'
    PARTITIONED BY (`dt` STRING)
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/gmall/ods/ods_base_category1_full/';

-- 7.2.4 二级品类表（全量表）
DROP TABLE IF EXISTS ods_base_category2_full;
CREATE EXTERNAL TABLE ods_base_category2_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '二级分类名称',
    `category1_id` STRING COMMENT '一级分类编号'
) COMMENT '二级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_category2_full/';

-- 7.2.5 三级品类表（全量表）
DROP TABLE IF EXISTS ods_base_category3_full;
CREATE EXTERNAL TABLE ods_base_category3_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号'
) COMMENT '三级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_category3_full/';

-- 7.2.6 编码字典表（全量表）
DROP TABLE IF EXISTS ods_base_dic_full;
CREATE EXTERNAL TABLE ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_dic_full/';

-- 7.2.7 省份表（全量表）
DROP TABLE IF EXISTS ods_base_province_full;
CREATE EXTERNAL TABLE ods_base_province_full
(
    `id`         STRING COMMENT '编号',
    `name`       STRING COMMENT '省份名称',
    `region_id`  STRING COMMENT '地区ID',
    `area_code`  STRING COMMENT '地区编码',
    `iso_code`   STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2` STRING COMMENT '新版IOS-3166-2编码，供可视化使用'
) COMMENT '省份表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_province_full/';

-- 7.2.8 地区表（全量表）
DROP TABLE IF EXISTS ods_base_region_full;
CREATE EXTERNAL TABLE ods_base_region_full
(
    `id`          STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_region_full/';

-- 7.2.9 品牌表（全量表）
DROP TABLE IF EXISTS ods_base_trademark_full;
CREATE EXTERNAL TABLE ods_base_trademark_full
(
    `id`       STRING COMMENT '编号',
    `tm_name`  STRING COMMENT '品牌名称',
    `logo_url` STRING COMMENT '品牌logo的图片路径'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_base_trademark_full/';

-- 7.2.10 购物车表（全量表）
DROP TABLE IF EXISTS ods_cart_info_full;
CREATE EXTERNAL TABLE ods_cart_info_full
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户id',
    `sku_id`       STRING COMMENT 'sku_id',
    `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
    `sku_num`      BIGINT COMMENT '数量',
    `img_url`      BIGINT COMMENT '商品图片地址',
    `sku_name`     STRING COMMENT 'sku名称 (冗余)',
    `is_checked`   STRING COMMENT '是否被选中',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间',
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号'
) COMMENT '购物车全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_cart_info_full/';

-- 7.2.11 优惠券信息表（全量表）
DROP TABLE IF EXISTS ods_coupon_info_full;
CREATE EXTERNAL TABLE ods_coupon_info_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始领取时间',
    `end_time`         STRING COMMENT '结束领取时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_coupon_info_full/';

-- 7.2.12 商品平台属性表（全量表）
DROP TABLE IF EXISTS ods_sku_attr_value_full;
CREATE EXTERNAL TABLE ods_sku_attr_value_full
(
    `id`         STRING COMMENT '编号',
    `attr_id`    STRING COMMENT '平台属性ID',
    `value_id`   STRING COMMENT '平台属性值ID',
    `sku_id`     STRING COMMENT '商品ID',
    `attr_name`  STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_sku_attr_value_full/';

-- 7.2.13 商品表（全量表）
DROP TABLE IF EXISTS ods_sku_info_full;
CREATE EXTERNAL TABLE ods_sku_info_full
(
    `id`              STRING COMMENT 'skuId',
    `spu_id`          STRING COMMENT 'spuid',
    `price`           DECIMAL(16, 2) COMMENT '价格',
    `sku_name`        STRING COMMENT '商品名称',
    `sku_desc`        STRING COMMENT '商品描述',
    `weight`          DECIMAL(16, 2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌id',
    `category3_id`    STRING COMMENT '品类id',
    `sku_default_igm` STRING COMMENT '商品图片地址',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_sku_info_full/';

-- 7.2.14 商品销售属性值表（全量表）
DROP TABLE IF EXISTS ods_sku_sale_attr_value_full;
CREATE EXTERNAL TABLE ods_sku_sale_attr_value_full
(
    `id`                   STRING COMMENT '编号',
    `sku_id`               STRING COMMENT 'sku_id',
    `spu_id`               STRING COMMENT 'spu_id',
    `sale_attr_value_id`   STRING COMMENT '销售属性值id',
    `sale_attr_id`         STRING COMMENT '销售属性id',
    `sale_attr_name`       STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value_full/';

-- 7.2.15 SPU表（全量表）
DROP TABLE IF EXISTS ods_spu_info_full;
CREATE EXTERNAL TABLE ods_spu_info_full
(
    `id`           STRING COMMENT 'spu_id',
    `spu_name`     STRING COMMENT 'spu名称',
    `description`  STRING COMMENT '描述信息',
    `category3_id` STRING COMMENT '品类id',
    `tm_id`        STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/gmall/ods/ods_spu_info_full/';

-- 7.2.16 购物车表（增量表）
DROP TABLE IF EXISTS ods_cart_info_inc;
CREATE EXTERNAL TABLE ods_cart_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,cart_price :DECIMAL(16, 2),sku_num :BIGINT,img_url :STRING,sku_name
                  :STRING,is_checked :STRING,create_time :STRING,operate_time :STRING,is_ordered :STRING,order_time
                  :STRING,source_type :STRING,source_id :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '购物车增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_cart_info_inc/';

-- 7.2.17 评论表（增量表）
DROP TABLE IF EXISTS ods_comment_info_inc;
CREATE EXTERNAL TABLE ods_comment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,nick_name :STRING,head_img :STRING,sku_id :STRING,spu_id :STRING,order_id
                  :STRING,appraise :STRING,comment_txt :STRING,create_time :STRING,operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '评价表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_comment_info_inc/';

-- 7.2.18 优惠券领用表（增量表）
DROP TABLE IF EXISTS ods_coupon_use_inc;
CREATE EXTERNAL TABLE ods_coupon_use_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,coupon_id :STRING,user_id :STRING,order_id :STRING,coupon_status :STRING,get_time :STRING,using_time
                  :STRING,used_time :STRING,expire_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/';

-- 7.2.19 收藏表（增量表）
DROP TABLE IF EXISTS ods_favor_info_inc;
CREATE EXTERNAL TABLE ods_favor_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,spu_id :STRING,is_cancel :STRING,create_time :STRING,cancel_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_favor_info_inc/';

-- 7.2.20 订单明细表（增量表）
DROP TABLE IF EXISTS ods_order_detail_inc;
CREATE EXTERNAL TABLE ods_order_detail_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,sku_id :STRING,sku_name :STRING,img_url :STRING,order_price
                  :DECIMAL(16, 2),sku_num :BIGINT,create_time :STRING,source_type :STRING,source_id :STRING,split_total_amount
                  :DECIMAL(16, 2),split_activity_amount :DECIMAL(16, 2),split_coupon_amount
                  :DECIMAL(16, 2)> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_detail_inc/';

-- 7.2.21 订单明细活动关联表（增量表）
DROP TABLE IF EXISTS ods_order_detail_activity_inc;
CREATE EXTERNAL TABLE ods_order_detail_activity_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,activity_id :STRING,activity_rule_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细活动关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_detail_activity_inc/';

-- 7.2.22 订单明细优惠券关联表（增量表）
DROP TABLE IF EXISTS ods_order_detail_coupon_inc;
CREATE EXTERNAL TABLE ods_order_detail_coupon_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,coupon_id :STRING,coupon_use_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细优惠券关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/';

-- 7.2.23 订单表（增量表）
DROP TABLE IF EXISTS ods_order_info_inc;
CREATE EXTERNAL TABLE ods_order_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,consignee :STRING,consignee_tel :STRING,total_amount :DECIMAL(16, 2),order_status :STRING,user_id
                  :STRING,payment_way :STRING,delivery_address :STRING,order_comment :STRING,out_trade_no :STRING,trade_body
                  :STRING,create_time :STRING,operate_time :STRING,expire_time :STRING,process_status :STRING,tracking_no
                  :STRING,parent_order_id :STRING,img_url :STRING,province_id :STRING,activity_reduce_amount
                  :DECIMAL(16, 2),coupon_reduce_amount :DECIMAL(16, 2),original_total_amount :DECIMAL(16, 2),freight_fee
                  :DECIMAL(16, 2),freight_fee_reduce :DECIMAL(16, 2),refundable_time :DECIMAL(16, 2)> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_info_inc/';

-- 7.2.24 退单表（增量表）
DROP TABLE IF EXISTS ods_order_refund_info_inc;
CREATE EXTERNAL TABLE ods_order_refund_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                  :DECIMAL(16, 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_refund_info_inc/';

-- 7.2.25 订单状态流水表（增量表）
DROP TABLE IF EXISTS ods_order_status_log_inc;
CREATE EXTERNAL TABLE ods_order_status_log_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_status :STRING,operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_order_status_log_inc/';

-- 7.2.26 支付表（增量表）
DROP TABLE IF EXISTS ods_payment_info_inc;
CREATE EXTERNAL TABLE ods_payment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,user_id :STRING,payment_type :STRING,trade_no
                  :STRING,total_amount :DECIMAL(16, 2),subject :STRING,payment_status :STRING,create_time :STRING,callback_time
                  :STRING,callback_content :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_payment_info_inc/';

-- 7.2.27 退款表（增量表）
DROP TABLE IF EXISTS ods_refund_payment_inc;
CREATE EXTERNAL TABLE ods_refund_payment_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,sku_id :STRING,payment_type :STRING,trade_no :STRING,total_amount
                  :DECIMAL(16, 2),subject :STRING,refund_status :STRING,create_time :STRING,callback_time :STRING,callback_content
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退款表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_refund_payment_inc/';

-- 7.2.28 用户表（增量表）
DROP TABLE IF EXISTS ods_user_info_inc;
CREATE EXTERNAL TABLE ods_user_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,login_name :STRING,nick_name :STRING,passwd :STRING,name :STRING,phone_num :STRING,email
                  :STRING,head_img :STRING,user_level :STRING,birthday :STRING,gender :STRING,create_time :STRING,operate_time
                  :STRING,status :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_user_info_inc/';

-- 数据装载脚本 hdfs_to_ods_db.sh

-- 第8章 数仓开发之DIM层
DROP TABLE IF EXISTS dim_sku_full;
CREATE EXTERNAL TABLE dim_sku_full
(
    `id`                   STRING COMMENT 'sku_id',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              BOOLEAN COMMENT '是否在售',
    `spu_id`               STRING COMMENT 'spu编号',
    `spu_name`             STRING COMMENT 'spu名称',
    `category3_id`         STRING COMMENT '三级分类id',
    `category3_name`       STRING COMMENT '三级分类名称',
    `category2_id`         STRING COMMENT '二级分类id',
    `category2_name`       STRING COMMENT '二级分类名称',
    `category1_id`         STRING COMMENT '一级分类id',
    `category1_name`       STRING COMMENT '一级分类名称',
    `tm_id`                STRING COMMENT '品牌id',
    `tm_name`              STRING COMMENT '品牌名称',
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,value_id :STRING,attr_name :STRING,value_name:STRING>> COMMENT '平台属性',
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,sale_attr_value_id :STRING,sale_attr_name :STRING,sale_attr_value_name:STRING>> COMMENT '销售属性',
    `create_time`          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 导出测试
insert overwrite local directory '/export/bigadmin/files/ods_sku_info_full' row format delimited fields terminated by ","
select
    id,
    price,
    sku_name,
    sku_desc,
    weight,
    is_sale,
    spu_id,
    category3_id,
    tm_id,
    create_time
from ods_sku_info_full
where dt='2020-06-14';


-- 2）数据装载
with
    sku as
        (
            select
                id,
                price,
                sku_name,
                sku_desc,
                weight,
                is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            from ods_sku_info_full
            where dt='2020-06-14'
        ),
    spu as
        (
            select
                id,
                spu_name
            from ods_spu_info_full
            where dt='2020-06-14'
        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from ods_base_category3_full
            where dt='2020-06-14'
        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from ods_base_category2_full
            where dt='2020-06-14'
        ),
    c1 as
        (
            select
                id,
                name
            from ods_base_category1_full
            where dt='2020-06-14'
        ),
    tm as
        (
            select
                id,
                tm_name
            from ods_base_trademark_full
            where dt='2020-06-14'
        ),
    attr as
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            from ods_sku_attr_value_full
            where dt='2020-06-14'
            group by sku_id
        ),
    sale_attr as
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from ods_sku_sale_attr_value_full
            where dt='2020-06-14'
            group by sku_id
        )
insert overwrite table dim_sku_full partition(dt='2020-06-14')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on sku.tm_id=tm.id
         left join attr on sku.id=attr.sku_id
         left join sale_attr on sku.id=sale_attr.sku_id;