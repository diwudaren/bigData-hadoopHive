package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 双Value类型交互
 *  intersection()交集
 *  需求说明：创建两个RDD，求两个RDD的交集
 */
object $_03DoubleValue01_intersection {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd1 = sc.makeRDD(1 to 6)
    val rdd2 = sc.makeRDD(4 to 8)
    val rdd3 = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }

}
