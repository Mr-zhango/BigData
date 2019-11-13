package cn.myfreecloud.spark.json

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON


/**
 * 解析 json 数据 注意:一行一个数据
 */
object JSON01_JSON {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JSON01_JSON")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    val jsonRDD: RDD[String] = sc.textFile("input/user.json")

    val result = jsonRDD.map(JSON.parseFull)

    // map映射成对偶数据
    result.foreach(println)

    // 释放资源
    sc.stop()

  }

}
