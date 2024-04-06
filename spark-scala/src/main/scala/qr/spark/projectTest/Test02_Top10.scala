package qr.spark.projectTest

import org.apache.spark.{SparkConf, SparkContext}

object Test02_Top10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("input/user_visit_action.txt")

    val userRDD = rdd.map(line => {
      val data: Array[String] = line.split("_")
      UserVisitAction(
        data(0),
        data(1),
        data(2),
        data(3),
        data(4),
        data(5),
        data(6),
        data(7),
        data(8),
        data(9),
        data(10),
        data(11),
        data(12)
      )
    })
    val categoryRDD = userRDD.flatMap(user => {
      if (user.click_category_id != "-1") {
        // 点击数据
        List(CategoryCountInfo(user.click_category_id, 1, 0, 0))
      } else if (user.order_category_ids != "null") {
        // 下单数据
        val orders: Array[String] = user.order_category_ids.split(",")
        orders.map(order => CategoryCountInfo(order, 0, 1, 0))

      } else if (user.pay_category_ids != "null") {
        // 支付数据
        val pays: Array[String] = user.pay_category_ids.split(",")
        pays.map(pay => CategoryCountInfo(pay, 0, 0, 1))

      } else {
        List()
      }
    })

    val reduceRDD = categoryRDD.map(info => (info.categoryId, info)).reduceByKey((res, elem) => {
        res.clickCount += elem.clickCount
        res.orderCount += elem.orderCount
        res.payCount += elem.payCount
        res
      })

    val categoryReduceRDD = reduceRDD.map(_._2)
    val sortRDD = categoryReduceRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)

    sortRDD.foreach(println)

    Thread.sleep(600000)

    sc.stop()
  }

}
