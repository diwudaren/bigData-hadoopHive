package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *  RDD依赖关系
 *      查看血缘关系
 *
 */
object $_08Lineage01 {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.textFile("input/1.txt")
    println(fileRDD.toDebugString)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(resultRDD.toDebugString)

    resultRDD.collect()

    sc.stop()
  }
}
