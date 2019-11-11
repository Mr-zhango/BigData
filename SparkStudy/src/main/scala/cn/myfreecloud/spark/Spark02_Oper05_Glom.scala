package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 1. 作用：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
 * 2. 需求：创建一个4个分区的RDD，并将每个分区的数据放到一个数组
 */
object Spark02_Oper05_Glom {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper04_FlatMap")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8), 3)

    // 将我们一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array =>{
      println(array.mkString(","))
    })

    sc.stop()

  }



}
