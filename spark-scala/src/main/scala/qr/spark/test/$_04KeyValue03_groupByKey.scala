package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey()按照K重新分组
 * groupByKey对每个key进行操作，但只生成一个seq，并不进行聚合
 *
 */
object $_04KeyValue03_groupByKey {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    val rdd = sc.makeRDD(Array(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
    val groupRdd = rdd.groupByKey()
    groupRdd.collect().foreach(println)
    //计算相同key对应值的相加结果
    groupRdd.map(t => (t._1, t._2.sum)).collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
