package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *  zip()拉链
 *    该操作可以将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的元素。
 *    将两个RDD组合成Key/Value形式的RDD，这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
 *
 *    需求说明：创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
 *
 */
object
$_03DoubleValue04_zip {
  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf)
    // 编写任务代码
    val rdd1 = sc.makeRDD(1 to 6)
    val rdd2 = sc.makeRDD(4 to 9)
    val rdd3 = rdd1.zip(rdd2)
    rdd3.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
