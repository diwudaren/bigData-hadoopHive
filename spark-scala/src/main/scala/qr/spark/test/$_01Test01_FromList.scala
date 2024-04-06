package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从集合中创建RDD
 */
object $_01Test01_FromList {

  def main(args: Array[String]): Unit = {
    // 创建sc的配置对象
    val conf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")
    // 创建sc对象
    val sc = new SparkContext(conf);
    // 编写任务代码
    val list = List(1,2,3);
    // 从集合创建add
    val intAdd = sc.parallelize(list)
    intAdd.collect().foreach(println)

    // 底层调用 parallelize 推荐使用
    val intADD1 = sc.makeRDD(list)
    intADD1.collect().foreach(println)
    sc.stop()


  }

}
