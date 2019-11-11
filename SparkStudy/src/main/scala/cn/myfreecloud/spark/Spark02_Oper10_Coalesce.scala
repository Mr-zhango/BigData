package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
 * 2. 需求：创建一个4个分区的RDD，对其缩减分区
 */
object Spark02_Oper10_Coalesce {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper07_Filter")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4 )


    println("缩减分区之前:"+listRDD.partitions.size)

    // 缩减分区可以简单的理解为合并分区
    println("缩减分区之后:"+listRDD.coalesce(3).partitions.size)


    sc.stop()
  }
}
