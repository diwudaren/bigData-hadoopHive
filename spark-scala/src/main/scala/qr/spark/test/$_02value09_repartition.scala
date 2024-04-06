package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * repartition()重新分区（执行Shuffle）
 *     coalesce和repartition区别
 *     repartition实际上是调用的coalesce，进行shuffle // rdd.coalesce(2, true)
 */
object $_02value09_repartition {

  def main(args: Array[String]): Unit = {
    // 创房sc配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val partitionRdd = rdd.repartition(2)
    val indexRdd = partitionRdd.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    indexRdd.collect().foreach(println)
    Thread.sleep(100000)
    // 关闭连接
    sc.stop()
  }

}
