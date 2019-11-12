package cn.myfreecloud.spark.action

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 1. 作用：针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
 * 2. 需求：创建一个PairRDD，统计每种key的个数
 */

object Action_04_CountByKey {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action_03_SaveAsXXXFile")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val byKey: scala.collection.Map[Int,Long] = rdd.countByKey

    byKey.foreach(println)


  }

}
