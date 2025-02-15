package qr.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object $_09cache01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建一个RDD，读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("input")

    //3.1.业务逻辑
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map {
      word => {
        println("************")
        (word, 1)
      }
    }

    //3.5 cache缓存前打印血缘关系
    println(wordToOneRdd.toDebugString)

    //3.4 数据缓存。
    //cache底层调用的就是persist方法,缓存级别默认用的是MEMORY_ONLY
    wordToOneRdd.cache()

    //3.6 persist方法可以更改存储级别
    // wordToOneRdd.persist(StorageLevel.MEMORY_AND_DISK_2)

    //3.2 触发执行逻辑
    wordToOneRdd.collect().foreach(println)

    //3.5 cache缓存后打印血缘关系
    //cache操作会增加血缘关系，不改变原有的血缘关系
    println(wordToOneRdd.toDebugString)

    println("==================================")

    //3.3 再次触发执行逻辑
    wordToOneRdd.collect().foreach(println)

    Thread.sleep(1000000)

    //4.关闭连接
    sc.stop()
  }
}
