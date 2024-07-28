#!/bin/bash

# 该脚本的作用是初始化所有的业务数据，只需执行一次

MAXWELL_HOME=/soft/maxwell

import_data() {
    $MAXWELL_HOME/bin/maxwell-bootstrap --database gmall --table $1 --config $MAXWELL_HOME/config.properties
}

case $1 in
"activity_info")
  import_data activity_info
  ;;
"activity_rule")
  import_data activity_rule
  ;;
"activity_sku")
  import_data activity_sku
  ;;
"base_category1")
  import_data base_category1
  ;;
"base_category2")
  import_data base_category2
  ;;
"base_category3")
  import_data base_category3
  ;;
"base_province")
  import_data base_province
  ;;
"base_region")
  import_data base_region
  ;;
"base_trademark")
  import_data base_trademark
  ;;
"coupon_info")
  import_data coupon_info
  ;;
"coupon_range")
  import_data coupon_range
  ;;
"financial_sku_cost")
  import_data financial_sku_cost
  ;;
"sku_info")
  import_data sku_info
  ;;
"spu_info")
  import_data spu_info
  ;;
"user_info")
  import_data user_info
  ;;
"all")
  import_data activity_info
  import_data activity_rule
  import_data activity_sku
  import_data base_category1
  import_data base_category2
  import_data base_category3
  import_data base_province
  import_data base_region
  import_data base_trademark
  import_data coupon_info
  import_data coupon_range
  import_data financial_sku_cost
  import_data sku_info
  import_data spu_info
  import_data user_info
  ;;
esac