package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy()分组
 *    groupBy会存在shuffle过程
 *    shuffle：将不同的分区数据进行打乱重组的过程
 *    shuffle一定会落盘。可以在local模式下执行程序，通过4040看效果。
 */
object $_02value05_groupby {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    // 3.1 创建一个RDD
    val rdd = sc.makeRDD(1 to 4, 2)
    // 3.2 将每个分区的数据放到一个数组并收集到Driver端打印
    rdd.groupBy(_ % 2).collect().foreach(println)

    // 3.3 创建一个RDD
    val rdd1 = sc.makeRDD(List("hello", "hive", "hadoop", "spark", "scala"))
    // 3.4 按照首字母第一个单词相同分组
    rdd1.groupBy(str=>str.substring(0,1)).collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
