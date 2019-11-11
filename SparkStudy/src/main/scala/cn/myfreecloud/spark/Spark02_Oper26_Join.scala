package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_Oper26_Join {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper24_SortByKey")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    val value: RDD[(Int, String)] = rdd.mapValues(_+"|||")

    value.collect().foreach(println)

  }
}
