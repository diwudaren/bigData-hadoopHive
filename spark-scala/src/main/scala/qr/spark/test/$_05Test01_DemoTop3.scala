package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordered.orderingToOrdered

/**
 *
 * 案例实操（省份广告被点击Top3）
 *
 *      数据准备：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
 *          文件 input/agent.log
 *
 *  需求： 统计出每一个省份广告被点击次数的Top3
 */
object $_05Test01_DemoTop3 {
  def main(args: Array[String]): Unit = {
    // 创建sc配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.textFile("input/agent.log")
    val tupleRdd = rdd.map(line => {
      val data = line.split(" ")
      ((data(1), data(4)),1)
    })
    val reduceCountRDD = tupleRdd.reduceByKey(_ + _)
    val value = reduceCountRDD.map(tuple => {
      (tuple._1._1, (tuple._1._2, tuple._2))
    })
    // 偏函数的写法
//    val value = provinceRDD.map({ case ((province, id), count) => (province, (id, count)) })
    val provinceRDD1 = value.groupByKey()
    val result = provinceRDD1.mapValues(it => {
      val list = it.toList
      list.sortWith(_._2 > _._2).take(3)
    })
    result.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
