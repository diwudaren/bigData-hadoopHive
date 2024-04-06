package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * foreach()遍历RDD中每一个元素
 *
 *
 */
object $_06action08_foreach {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    // 保存成Text文件
    rdd.collect().foreach(println)
    println("****************************************")
    rdd.foreach(println)
    sc.stop()
  }
}
