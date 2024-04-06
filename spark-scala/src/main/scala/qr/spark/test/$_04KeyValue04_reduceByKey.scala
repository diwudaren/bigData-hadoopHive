package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * reduceByKey()按照K聚合V
 *
 *    reduceByKey和groupByKey区别
 *        reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[K,V]。
 *        groupByKey：按照key进行分组，直接进行shuffle
 *        开发指导：在不影响业务逻辑的前提下，优先选用reduceByKey。求和操作不影响业务逻辑，求平均值影响业务逻辑，后续会学习功能更加强大的归约算子，能够在预聚合的情况下实现求平均值。
 *
 *  需求说明：统计单词出现次数
 */
object $_04KeyValue04_reduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
    val reduceRdd = rdd.reduceByKey((v1, v2) => v1 + v2)
    reduceRdd.collect().foreach(println)
    sc.stop()

  }
}
