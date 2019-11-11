package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 作用：将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
 * 2. 需求：创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
 *
 */
object Spark02_Oper16_Zip {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper16_Zip")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.parallelize(Array(1,2,3),3)

    val list2RDD: RDD[String] = sc.parallelize(Array("a","b","c"),3)

    // 拉链必须一一对应否则抛异常
    // spark中分区数也要相等

    val zipRDD: RDD[(Int,String)] = listRDD.zip(list2RDD)

    zipRDD.collect().foreach(println)

    sc.stop()

  }


}
