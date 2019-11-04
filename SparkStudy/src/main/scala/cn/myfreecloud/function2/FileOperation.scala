package cn.myfreecloud.function2

import cn.myfreecloud.spark.WordCountLocalDemo.logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FileOperation {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("FileOperation")

    val sc = new SparkContext(sparkConfig)


      // 读取文件
    val lines: RDD[String] =  sc.textFile("input")

    // 切分字符
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 转化为元组的形式
    val wordsTuple: RDD[(String,Int)] = words.map((_, 1))

    // 使用reduce操作来进行计算
    wordsTuple.reduceByKey(_ + _).collect().foreach(println)

    logger.info("complete")


    sc.stop()
  }

}
