package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * coalesce()合并分区
 *    Coalesce算子包括：配置执行Shuffle和配置不执行Shuffle两种方式。
 *
 */
object $_02value08_coalesce {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    // 1、不执行Shuffle方式
    // 创建一个RDD
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4,5,6), 3)
    // 缩减分区
    val coalesceRdd1 = rdd1.coalesce(2)
    val indexRdd1 = coalesceRdd1.mapPartitionsWithIndex((index, datas) => datas.map((index, _)))
    indexRdd1.collect().foreach(println)

    // 2、执行Shuffle方式
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    val coalesceRdd2 = rdd2.coalesce(2, true)
    val indexRdd2 = coalesceRdd2.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    indexRdd2.collect().foreach(println)

    // 延迟一段时间，观察http://localhost:4040页面，查看Shuffle读写数据
    Thread.sleep(100000)

    // 关闭连接
    sc.stop()
  }

}
