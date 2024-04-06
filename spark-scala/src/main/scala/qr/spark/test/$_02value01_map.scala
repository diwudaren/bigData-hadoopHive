package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 2.3 Transformation转换算子
 * map()映射
 */
object $_02value01_map {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.makeRDD(1 to 4, 2)
    // 调用map方法，每个元素乘以2
    val mapRdd = rdd.map(_ * 2)
    mapRdd.collect().foreach(println)
    //关闭sc
    sc.stop()
  }
}
