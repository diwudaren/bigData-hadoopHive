package qr.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * RDD序列化
 *    闭包检查 闭包引入（有闭包就需要进行序列化）
 *
 */
object $_7serializable01_object {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val user1 = new User()
    user1.name = "zhangsan"

    val user2 = new User()
    user2.name = "lisi"

    val rdd = sc.makeRDD(List(user1, user2))
    rdd.foreach(user => println(user.name+" love "+user1.name))


    sc.stop()
  }
}

class User extends Serializable {
  var name: String =_
}