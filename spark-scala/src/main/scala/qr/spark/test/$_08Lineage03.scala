package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * Stage任务划分
 *    RDD任务切分中间分为：Application、Job、Stage和Task
        *（1）Application：初始化一个SparkContext即生成一个Application；
        *（2）Job：一个Action算子就会生成一个Job；
        *（3）Stage：Stage等于宽依赖的个数加1；
        *（4）Task：一个Stage阶段中，最后一个RDD的分区个数就是Task的个数。
    *注意：Application->Job->Stage->Task每一层都是1对n的关系。
 *
 */
object $_08Lineage03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //Application：初始化一个SparkContext即生成一个Application
    val sc = new SparkContext(conf)

    //textFile,flatMap,map算子全部是窄依赖,不会增加stage阶段
    val lineRDD: RDD[String] = sc.textFile("input/1.txt")
    val flatMapRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))

    //reduceByKey算子会有宽依赖,stage阶段加1，2个stage
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    //Job：一个Action算子就会生成一个Job，2个Job
    //job0打印到控制台
    resultRDD.collect().foreach(println)
    //job1输出到磁盘
    resultRDD.saveAsTextFile("output")

    //阻塞线程,方便进入localhost:4040查看
    Thread.sleep(Long.MaxValue)

    //TODO 3 关闭资源
    sc.stop()
  }
}
