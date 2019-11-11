package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object WordCountLocalDemo {

  //引入日志框架
  val logger = LoggerFactory.getLogger(WordCountLocalDemo.getClass)

  def main(args: Array[String]): Unit = {


    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountLocalDemo")

    //创建sparkContext,该对象是spark APP的入口
    val sc = new SparkContext(sparkConfig)

    val lines: RDD[String] = sc.textFile("input")

    // 切分字符
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 转化为元组的形式
    val wordsTuple: RDD[(String,Int)] = words.map((_, 1))

    // 使用reduce操作来进行计算
    // 对转换后的数据进行分组聚合 两两聚合
    val wordToSum: RDD[(String,Int)] = wordsTuple.reduceByKey(_ + _)

    // 打印
    wordToSum.collect().foreach(println)

    logger.info("complete")

    sc.stop()

  }


}
