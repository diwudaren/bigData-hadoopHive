package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * 查看依赖关系
 *
 *
 */
object $_08Lineage02 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val fileRDD: RDD[String] = sc.textFile("input/1.txt")
    println(fileRDD.dependencies)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(mapRDD.dependencies)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(resultRDD.dependencies)

    resultRDD.collect()

    // 查看localhost:4040页面，观察DAG图
    Thread.sleep(10000000)

    //4.关闭连接
    sc.stop()
  }
}
