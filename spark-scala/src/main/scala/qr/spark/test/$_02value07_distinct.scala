package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * distinct()去重
 *    注意：distinct会存在shuffle过程。
 */
object $_02value07_distinct {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf =new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))
    // 打印去重后新生成的新RDD
    rdd.distinct().collect().foreach(println)
    // 对RDD采用Task去重，提高并发度
    rdd.distinct(2).collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
