package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Kryo序列化框架
 *    经在Spark内部使用Kryo来序列化。
 *
 */
object $_07serializable02_Kryo {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkCoreTest")
      .setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化的自定义类
      .registerKryoClasses(Array(classOf[Search]))

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)

    val search = new Search("hello")
    val result: RDD[String] = rdd.filter(search.isMatch)

    result.collect.foreach(println)

    sc.stop()
  }
}

// 关键字封装在一个类里面
// 需要自己先让类实现序列化  之后才能替换使用kryo序列化
class Search(val query: String) extends Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }
}