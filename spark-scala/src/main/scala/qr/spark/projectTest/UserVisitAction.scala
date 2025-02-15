package qr.spark.projectTest
//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: String,//用户的ID
                           session_id: String,//Session的ID
                           page_id: String,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: String,//某一个商品品类的ID
                           click_product_id: String,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: String)//城市 id