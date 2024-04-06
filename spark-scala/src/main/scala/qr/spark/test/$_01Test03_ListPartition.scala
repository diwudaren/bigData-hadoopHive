package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从集合创建RDD
 */
object $_01Test03_ListPartition {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置文件
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    // 默认环境的核数
    // 可以手动填写参数控制分区的个数
    val intRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)
    intRDD.saveAsTextFile("output")
    // 关闭sc
    sc.stop()

  }
}
