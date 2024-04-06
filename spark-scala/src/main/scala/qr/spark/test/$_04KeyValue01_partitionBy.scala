package qr.spark.test

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.{Partition, Partitioner}

/**
 *
 * Key-Value类型
 * partitionBy()按照K重新分区
 * 将RDD[K,V]中的K按照指定Partitioner重新进行分区；
 * 如果原有的RDD和新的RDD是一致的话就不进行分区，否则会产生Shuffle过程
 *
 * 需求说明：创建一个3个分区的RDD，对其重新分区
 */
object $_04KeyValue01_partitionBy {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    val rdd2 = rdd.partitionBy(new HashPartitioner(2))
    val indexRdd = rdd2.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    indexRdd.collect().foreach(println)


    Thread.sleep(100000)

    // 关闭连接
    sc.stop()
  }


}
