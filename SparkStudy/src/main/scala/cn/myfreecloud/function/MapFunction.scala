package cn.myfreecloud.function

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark的map函数,
  *
  * 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
  *
  */
object MapFunction {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    val sc = new SparkContext(sparkConfig)

    val source  = sc.parallelize(1 to 10)

    source.collect().foreach(println _)

    println("*****************************")

    val mapAdd = source.map( _ * 2)

    mapAdd.collect().foreach(println _)

    sc.stop()
  }
}
