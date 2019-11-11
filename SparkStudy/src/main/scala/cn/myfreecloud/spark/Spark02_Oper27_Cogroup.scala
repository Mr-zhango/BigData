package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 1. 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
 * 2. 需求：创建两个pairRDD，并将key相同的数据聚合到一个元组。
 */
object Spark02_Oper27_Cogroup {

  def main(args: Array[String]): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper24_SortByKey")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    val value: RDD[(Int, String)] = rdd.mapValues(_ + "|||")

    value.collect().foreach(println)

  }
}
