package qr.spark.word

import org.apache.spark._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1、 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    // 2、创建SparkContext, 该对象是提交Spark App 的入口
    val sc = new SparkContext(conf)
    // 3、读取指定位置文件
    val lineRdd = sc.textFile("input")
    // 4、 读取的一行一行的数据分解成一个个的单词（扁平化）
    val wordRdd = lineRdd.flatMap(_.split(" "))
    // 5、 将数据转换结构
    val wordToOneRdd = wordRdd.map((_, 1))
    // 6、 将转换后的数据进行聚合处理
    val wordToSumRdd = wordToOneRdd.reduceByKey(_ + _)
    // 7、 将统计结果采集到控制台打印
    wordToSumRdd.collect().foreach(println)
    // 8、 关闭连接
    sc.stop()
  }
}
