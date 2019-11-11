package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
 * 2. 需求：创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
 *
 */
object Spark02_Oper17_PartitionBy {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper17_PartitionBy")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    val size = rdd.partitions.size

    println(size)

    var rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))

    val size1 = rdd2.partitions.size

    println(size1)

    sc.stop()

  }


}
