package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 自定义分区器,把所有的数据到放进1号分区中
 */
object Spark02_Oper18_MyPartitioner {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper18_MyPartitioner")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val partitionerRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))

    partitionerRDD.saveAsTextFile("output")

  }

}

// 声明分区器
// 集成Partitioner类
class MyPartitioner(partitions: Int) extends Partitioner {
  /**
   * 实现默认的两个方法
   *
   * @return
   */
  override def numPartitions: Int = {
    partitions
  }

  //全部放进1号分区
  override def getPartition(key: Any): Int = {
    1
  }
}