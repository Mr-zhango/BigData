package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对两个RDD求 笛卡尔集
 *
 */
object Spark02_Oper15_Cartesian {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper15_Cartesian")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.parallelize(1 to 3 )

    val list2RDD: RDD[Int] = sc.parallelize(2 to 5)


    val cartesianRDD: RDD[(Int,Int)] = listRDD.cartesian(list2RDD)

    cartesianRDD.collect().foreach(println)

    sc.stop()

  }


}
