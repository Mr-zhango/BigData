package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 需求:创建一个RDD，使每个元素*2组成新的RDD
 */
object Spark02_Oper02_MapPartion {
  def main(args: Array[String]): Unit = {
    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper02_MapPartion")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD((1 to 10))

    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {

      /**
       * 循环的分区数目
       * 循环的分区数目
       * 循环的分区数目
       * 循环的分区数目
       *
       * 因为有四个核心,相当于自动分了四个分区
       */
      println("循环的分区数目")

      // mapPartitions:注意,mapPartitions可以对RDD中所有的分区进行遍历

      // mapPartitions的效率优于map算子,减少了发送到执行器的算的交互次数

      // mapPartitions可能出现内存溢出,要根据实际情况来决定
      datas.map(datas => datas*2)
    })

    mapPartitionsRDD.collect().foreach(println)

    sc.stop();
  }
}
