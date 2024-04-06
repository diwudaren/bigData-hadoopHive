package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import qr.spark.partition.MyPartitioner

/**
 * 自定义分区
 */
object $_04KeyValue02_partitionBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    // 自定义分区
    val rdd3 = rdd.partitionBy(new MyPartitioner(2))
    rdd3.collect().foreach(println)

    Thread.sleep(100000)

    // 关闭连接
    sc.stop()
  }
}
