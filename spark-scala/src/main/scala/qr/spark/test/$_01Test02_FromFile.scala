package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 由外部存储系统的数据集创建RDD
 */
object $_01Test02_FromFile {

  def main(args: Array[String]): Unit = {
  // 创建sc的配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
  // 创建sc对象
  val sc = new SparkContext(conf)
  // 编写任务代码
  val lineADD = sc.textFile("input/1.txt")
    lineADD.collect().foreach(println)
  // 关闭sc
    sc.stop()
  }



}
