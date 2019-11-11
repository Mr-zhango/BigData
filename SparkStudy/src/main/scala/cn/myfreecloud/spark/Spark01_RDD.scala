package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    // 创建RDD




    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_RDD")


    // 1. 从内存中创建

    // 2. 从内存中创建

    // 两种方式的底层实现是一样的


    // 3. 从外部存储中创建
    // 默认情况下可以读取项目路径,也可以读取其他路径:HDFS

    //创建sparkContext,该对象是spark APP的入口
    val sc = new SparkContext(sparkConfig)


    //val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //val value: RDD[Int] = sc.parallelize(Array(1,2,3,4))

    // 读取文件时,传递的分区数是最小分区数,但是并不一定是这个分区,取决于hadoop读取文件时候的分片规则
    val fileRDD: RDD[String] = sc.textFile("input",3)
    //listRDD.collect().foreach(println)

    //value.collect().foreach(println)


    // RDD的文件保存到本地

    fileRDD.saveAsTextFile("output")

  }
}
