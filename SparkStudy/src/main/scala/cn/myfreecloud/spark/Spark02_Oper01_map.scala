package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper01_map {
  def main(args: Array[String]): Unit = {
    // 创建RDD


    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper01_map")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD((1 to 10))

    // RDD 实际上就是一种装饰者设计模式
    //val mapRDD :RDD[Int] = listRDD.map( x => x*2)
    // 参数只有一个并且相同,所以可以使用_代替
    val mapRDD: RDD[Int] = listRDD.map(_ * 2)

    mapRDD.collect().foreach(println)

  }
}
