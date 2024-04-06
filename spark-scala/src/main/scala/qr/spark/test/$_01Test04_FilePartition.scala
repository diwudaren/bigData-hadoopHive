package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从文件创建RDD
 */
object $_01Test04_FilePartition {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    //创建sc对象
    val sc = new SparkContext(conf)
    //编写任务代码
    val intADD = sc.textFile("input/1.txt", 3)
    intADD.saveAsTextFile("output")
    //关闭sc
    sc.stop()

  }
}
