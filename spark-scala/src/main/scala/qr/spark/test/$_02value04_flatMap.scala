package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap()扁平化
 * 需求说明：创建一个集合，集合里面存储的还是子集合，把所有子集合中数据取出放入到一个大的集合中。
 */
object $_02value04_flatMap {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7)), 2)
    rdd.flatMap(list => list).collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
