package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * takeOrdered()返回该RDD排序后前n个元素组成的数组
 *      返回该RDD排序后的前n个元素组成的数组
 *
 */
object $_06action05_takeOrdered {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val takeResult = rdd.takeOrdered(2)
    println(takeResult.mkString(","))
    sc.stop()
  }
}
