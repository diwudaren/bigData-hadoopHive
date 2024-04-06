package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * union()并集不去重
 *  并集：全包括
 *
 *  需求说明：创建两个RDD，求并集
 */
object $_03DoubleValue02_union {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd1 = sc.makeRDD(1 to 6)
    val rdd2 = sc.makeRDD(4 to 8)
    val rdd3 = rdd1.union(rdd2)
    rdd3.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }

}
