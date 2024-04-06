package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * aggregateByKey()分区内和分区间逻辑不同的归约
 *
 *
 */
object $_04KeyValue05_aggregateByKey {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 5), ("b", 7), ("b", 2), ("b", 4), ("b", 6), ("a", 7)), 2)

    //3.2 取出每个分区相同key对应值的最大值，然后相加
    rdd.aggregateByKey(0)(math.max,_+_).collect().foreach(println)


    sc.stop()

  }
}
