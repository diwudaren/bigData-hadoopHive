package qr.spark.partition

import org.apache.spark.{Partition, Partitioner}

/**
 *
 * 要实现自定义分区器，需要继承org.apache.spark.Partitioner类，并实现下面三个方法。
（1）numPartitions: Int:返回创建出来的分区数。
（2）getPartition(key: Any): Int:返回给定键的分区编号（0到numPartitions-1）。
（3）equals():Java 判断相等性的标准方法。这个方法的实现非常重要，Spark需要用这个方法来检查你的分区器对象是否和其他分区器实例相同，这样Spark才可以判断两个RDD的分区方式是否相同。

 * @param partitions 分区数
 */
class MyPartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {

    key match {
      case _:String => 0
      case i:Int => i%numPartitions
      case _ => 0
    }
  }
}
