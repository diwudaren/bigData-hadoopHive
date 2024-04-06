package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy()排序
 */
object $_02value10_sortBy {
  def main(args: Array[String]): Unit = {
    // 创建sc配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    //创建sc对象
    val sc = new SparkContext(conf)
    //编写任务代码
    val rdd = sc.makeRDD(List(2, 1, 3, 4, 6, 5))
    //默认是升序排
    val sortRdd = rdd.sortBy(num=>num)
    sortRdd.collect().foreach(println)

    // 配置为倒序排
    val sortRdd1 = rdd.sortBy(num => num, false)
    sortRdd1.collect().foreach(println)

    // 按照字符的int值排序
    val sortRdd2 = rdd.sortBy(num => num.toInt)
    sortRdd2.collect().foreach(println)

    // 创建一个RDD
    val rdd2 = sc.makeRDD(List((2, 1), (1, 2), (1, 1), (2, 2)))
    // 先按照tuple的第一个值排序，相等再按照第2个值排
    val sortRdd3 = rdd2.sortBy(t => t)
    sortRdd3.collect().foreach(println)

    //关闭连接
    sc.stop()
  }
}
