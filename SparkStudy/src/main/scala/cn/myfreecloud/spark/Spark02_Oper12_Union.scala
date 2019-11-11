package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对两个RDD求并集
 *
 */
object Spark02_Oper12_Union {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper11_SortBy")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.parallelize(1 to 5)

    val list2RDD: RDD[Int] = sc.parallelize(5 to 10)


    val unionRDD: RDD[Int] = listRDD.union(list2RDD)

    unionRDD.collect().foreach(println)

    sc.stop()

  }


}
