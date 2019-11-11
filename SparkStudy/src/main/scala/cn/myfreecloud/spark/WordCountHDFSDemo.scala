package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object WordCountHDFSDemo {
  //引入日志框架
  val logger = LoggerFactory.getLogger(WordCountLocalDemo.getClass)


  def main(args: Array[String]): Unit = {


    val sparkConfig = new SparkConf().setMaster("spark://hadoop102:7077").setAppName("WordCountRemoteDemo")

    /**
     * 创建sparkContext,该对象是spark APP的入口
     */
    val sparkContext = new SparkContext(sparkConfig)

    val file = sparkContext.textFile("hdfs://hadoop102:8020/README.txt")

    // 切分字符
    val words = file.flatMap(_.split(" "))

    // 转化为元组的形式
    val wordsTuple = words.map((_, 1))

    // 使用reduce操作来进行计算
    wordsTuple.reduceByKey(_ + _).collect().foreach(println _)

    logger.info("complete")

    sparkContext.stop()

  }

}

