package cn.myfreecloud.spark
import org.apache.spark.{SparkConf,SparkContext}

object WordCountJar extends App{


  val sparkConfig = new SparkConf().setAppName("WordCountJar")

  val sparkContext = new SparkContext(sparkConfig)

  val file = sparkContext.textFile("hdfs://hadoop102:8020/README.txt")

  // 切分字符
  val words = file.flatMap(_.split(""))

  // 转化为元组的形式
  val wordsTuple = words.map((_,1))

  // 使用reduce操作来进行计算
  wordsTuple.reduceByKey(_+_).saveAsTextFile("hdfs://hadoop102:8020/sparkoutcode")

  sparkContext.stop()

}
