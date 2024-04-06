package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitions()以分区为单位执行Map
 */
object $_02value02_mapPartitions {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val  rdd = sc.makeRDD(1 to 4, 2)
    val mapRdd = rdd.mapPartitions(datas => datas.map(_ * 2))
    mapRdd.collect().foreach(println)

    val value = rdd.mapPartitions(list => {
      println("mapPartition 调用")
      list.filter(i => i % 2 == 0)
    })
    value.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }

}
