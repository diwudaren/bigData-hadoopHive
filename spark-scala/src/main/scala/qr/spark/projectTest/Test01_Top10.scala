package qr.spark.projectTest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * SparkCore实战
 * 需求分析（方案一）常规算子
 * 思路：分别统计每个品类点击的次数，下单的次数和支付的次数。然后想办法将三个RDD联合到一起。
 * （品类，点击总数）（品类，下单总数）（品类，支付总数）
 * （品类，（点击总数，下单总数，支付总数））
 * 然后就可以按照各品类的元组（点击总数，下单总数，支付总数）进行倒序排序了，因为元组排序刚好是先排第一个元素，然后排第二个元素，最后第三个元素。最后取top10即可。
 *
 *
 */
object Test01_Top10 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("input/user_visit_action.txt")
    val filerRDD = rdd.filter(line => {
      val data = line.split("_")
      !"-1".equals(data(6)) || !"null".equals(data(8)) || !"null".equals(data(10))
    })
    val flatMapRDD = filerRDD.flatMap(line => {
      val data = line.split("_")
      if (!"-1".equals(data(6))) {
        List((data(6), (1, 0, 0)))
      } else if (!"null".equals(data(8))) {
        val orders = data(8).split(",")
        orders.map(order => (order, (0, 1, 0)))
      } else if (!"null".equals(data(10))) {
        val pays = data(10).split(",")
        pays.map((_, (0, 0, 1)))
      } else {
        List()
      }
    })
    val reduceRDD = flatMapRDD.reduceByKey((res, elem) => (res._1 + elem._1, res._2 + elem._2, res._3 + elem._3))
    val result = reduceRDD.sortBy(_._2, false).take(10)
    result.foreach(println)
    sc.stop()
  }

}
