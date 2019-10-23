package cn.myfreecloud.function

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 与sortByKey类似，但是更灵活,可以用func先对数据进行处理，
  * 按照处理后的数据比较结果排序
  */
object SortByFun {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(List(1,2,3,4))

    rdd.sortBy(x => x).collect().foreach(println(_))

    rdd.sortBy(x => x%3).collect().foreach(println(_))

    sc.stop()
  }

}
