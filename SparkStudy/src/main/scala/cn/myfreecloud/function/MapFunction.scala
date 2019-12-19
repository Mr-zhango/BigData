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

    // 注意:to 包含最后一个值  但是until不包含最后一个值
    val source = sc.parallelize(1 to 10)
    source.collect().foreach(println(_))

    println("*****************************")

    val untilSource = sc.parallelize(1 until 10)
    untilSource.collect().foreach(println(_))

    println("*****************************")

    val mapAdd = source.map(_ * 2)

    mapAdd.collect().foreach(println _)

    sc.stop()
  }
}
