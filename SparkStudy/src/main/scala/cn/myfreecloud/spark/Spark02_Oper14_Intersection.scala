package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对两个RDD求交集
 *
 */
object Spark02_Oper14_Intersection {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper14_Intersection")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.parallelize(3 to 8 )

    val list2RDD: RDD[Int] = sc.parallelize(1 to 5)


    val intersectionRDD: RDD[Int] = listRDD.intersection(list2RDD)

    intersectionRDD.collect().foreach(println)

    sc.stop()

  }


}
