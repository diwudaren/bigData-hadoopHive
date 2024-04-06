package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitionsWithIndex()带分区号
 * 需求说明：创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
 */
object $_02value03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.makeRDD(1 to 4, 2)
    val indexRdd = rdd.mapPartitionsWithIndex((index, itens) => {
      itens.map((index, _))
    })
    indexRdd.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
