package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Action行动算子
 *    collect()以数组的形式返回数据集
 *
 */
object $_06action01_collect {
  //1.创建SparkConf并设置App名称
  val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

  //2.创建SparkContext，该对象是提交Spark App的入口
  val sc: SparkContext = new SparkContext(conf)

  //3具体业务逻辑
  //3.1 创建第一个RDD
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

  //3.2 收集数据到Driver
  rdd.collect().foreach(println)

  //4.关闭连接
  sc.stop()
}
