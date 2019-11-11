package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对两个RDD求差
 *
 */
object Spark02_Oper13_Subtract {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper11_SortBy")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.parallelize(3 to 8 )

    val list2RDD: RDD[Int] = sc.parallelize(1 to 5)


    val subtractRDD: RDD[Int] = listRDD.subtract(list2RDD)

    subtractRDD.collect().foreach(println)

    sc.stop()

  }


}
