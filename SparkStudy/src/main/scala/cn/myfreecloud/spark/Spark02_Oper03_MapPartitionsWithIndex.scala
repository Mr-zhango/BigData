package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_Oper03_MapPartitionsWithIndex {

  /**
   * 需求:创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
   * @param args
   */
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper03_MapPartitionsWithIndex")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD((1 to 10))

    // 同时匹配多个参数,使用模式匹配的方法

    //{}

    //
    val tupleRDD: RDD[(Int,String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_,"分区号:"+num))
      }
    }

    tupleRDD.collect.foreach(println)

    sc.stop()
  }
}
