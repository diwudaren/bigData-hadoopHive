package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 *  mapValues()只对V进行操作
 *      针对于(K,V)形式的类型只对V进行操作
 *
 *   需求说明：创建一个pairRDD，并将value添加字符串"|||"
 *
 */
object $_04KeyValue07_mapValues {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
    rdd.mapValues(_+"|||").collect().foreach(println)

    sc.stop()
  }
}
