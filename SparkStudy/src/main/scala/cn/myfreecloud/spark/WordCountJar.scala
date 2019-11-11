package cn.myfreecloud.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountJar extends App{


  //设置Spark计算框架的运行环境
  val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountLocalDemo")

  //创建sparkContext,该对象是spark APP的入口
  val sc = new SparkContext(sparkConfig)

  // hadoop上的路径
  //val file = sc.textFile("/opt/module/spark/input")

  val file = sc.textFile("file:///opt/module/spark/input")

  // 切分字符
  val words: RDD[String] = file.flatMap(_.split(" "))

  // 转化为元组的形式
  val wordsTuple = words.map((_,1))

  // 使用reduce操作来进行计算
  val wordToSum: RDD[(String,Int)] = wordsTuple.reduceByKey(_+_)

  wordToSum.collect().foreach(println)
  sc.stop()

}
