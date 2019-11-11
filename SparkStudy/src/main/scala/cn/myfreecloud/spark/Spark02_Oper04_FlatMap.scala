package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 */
object Spark02_Oper04_FlatMap {
  def main(args: Array[String]): Unit = {
    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper04_FlatMap")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))

    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

    flatMapRDD.collect().foreach(println)

    sc.stop()

  }
}
