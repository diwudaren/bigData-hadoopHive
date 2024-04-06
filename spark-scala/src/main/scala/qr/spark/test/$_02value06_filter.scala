package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * filter()过滤
 *    需求说明：创建一个RDD，过滤出对2取余等于0的数据
 */
object $_02value06_filter {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd = sc.makeRDD(1 to 4, 2)
    val filterRdd = rdd.filter(_ % 2 == 0)
    filterRdd.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
