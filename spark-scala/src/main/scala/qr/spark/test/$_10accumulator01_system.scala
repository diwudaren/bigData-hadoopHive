package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * 累加器
 *
 *
 */
object $_10accumulator01_system {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))
    //需求:统计a出现的所有次数 ("a",10)
    var sum = 0
    val sumAccumulator = sc.longAccumulator("sum")
    dataRDD.foreach({
      case (a,count) =>{
        sumAccumulator.add(count)
      }
    })

    println(("a",sumAccumulator.value))

    sc.stop()
  }

}
