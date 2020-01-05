package cn.myfreecloud.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 求("a", 1), ("a", 3), ("b", 3), ("b", 5), ("c", 4)
 * 每个key对一个的value的平均值
 *
 */
object AverageValue {
  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    val sc = new SparkContext(sparkConfig)

    val init_data = Array(("a", 1), ("a", 3), ("b", 3), ("b", 5), ("c", 4))
    val data = sc.parallelize(init_data)

    val value: RDD[(String, (Int, Int))] = data.combineByKey(
      v => (v, 1), (acc: (Int, Int), newV)
      => (acc._1 + newV, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int))
      => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    value.foreach(println(_))
  }
}
