package cn.myfreecloud.mapperreduce

import cn.myfreecloud.spark.WordCountLocalDemo.logger
import org.apache.spark.{SparkConf, SparkContext}

object MapperReduceTest {


  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    /**
     * 程序中只要初始化了sc,就可以称之为一个 driver
     */
    val sc = new SparkContext(sparkConfig)

    // 读取文件
    val file = sc.textFile("D://input")

    // 切分字符 flatMap 扁平化操作 按照 " " 切分成一个一个的单词
    val words = file.flatMap(_.split(" "))

    // 转化为元组的形式
    // 转化结构, 单词+次数的元组
    val wordsTuple = words.map((_,1))

    // 使用reduce操作来进行计算
    wordsTuple.reduceByKey(_+_).collect().foreach(println)

    logger.info("complete")



    logger.info("简化写法--开始")
    // 简化写法:
    sc.textFile("D://input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}
