package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * sortByKey()按照K进行排序
 *    在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的
 *
 *  需求说明：创建一个pairRDD，按照key的正序和倒序进行排序
 *
 */
object $_04KeyValue06_sortByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))

    // 默认key的正序排序
    rdd.sortByKey().collect().foreach(println)

    // 按照key的倒叙
    rdd.sortByKey(false).collect().foreach(println)


    sc.stop()
  }
}
