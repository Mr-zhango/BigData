package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 1.	作用：aggregateByKey的简化操作，seqop和combop相同
 * 2.	需求：创建一个pairRDD，计算相同key对应值的相加结果
 *
 * FoldByKey 分区内和分区间计算规则一样
 */
object Spark02_Oper22_FoldByKey {

  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper22_FoldByKey")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val value = rdd.foldByKey(0)(_+_)

    val value2 = rdd.aggregateByKey(0)(_+_,_+_)

    value.collect().foreach(println)

    println("****************************")
    value2.collect().foreach(println)

    sc.stop()

  }


}
