package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 1. 针对于(K,V)形式的类型只对V进行操作
 * 2. 需求：创建一个pairRDD，并将value添加字符串"|||"
 *
 */
object Spark02_Oper25_MapValues {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper25_MapValues")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    val value: RDD[(Int, String)] = rdd.mapValues(_+"|||")

    value.collect().foreach(println)

  }
}
