package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 作用：groupByKey也是对每个key进行操作，但只生成一个sequence。
 * 2. 需求：创建一个pairRDD，将相同key对应值聚合到一个sequence中，并计算相同key对应值的相加结果。
 */
object Spark02_Oper21_AggregateByKey {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper17_PartitionBy")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(Array("one", "two", "two", "three", "three", "three"),4)

    val wordPairsRDD = rdd.map(word => (word, 1))


    //相当于reduceByKey
    val value = wordPairsRDD.reduceByKey(_+_)


    value.collect().foreach(println)


    sc.stop()

  }


}
