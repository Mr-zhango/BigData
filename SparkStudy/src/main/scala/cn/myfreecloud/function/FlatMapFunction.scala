package cn.myfreecloud.function

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成
  *
  */
object FlatMapFunction {
  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("FlatMapFun")

    val sc = new SparkContext(sparkConfig)


    val sourceFilter = sc.parallelize(Array("xiaoming", "xiaojiang", "xiaohe", "dazhi"))


    sourceFilter.collect().foreach(println _)

    println("********************")
    val filter = sourceFilter.filter(_.contains("xiao"))
    filter.collect().foreach(println _)

    sc.stop()
  }
}
