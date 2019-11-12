package cn.myfreecloud.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 测试序列化接口
 */

object Action_05_Serializable {

  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.创建一个Search对象
    val search = new Search("h")

    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatch1(rdd)
    match1.collect().foreach(println)
  }
}


/**
 * Exception in thread "main" org.apache.spark.SparkException: Task not serializable
 * - object not serializable (class: cn.myfreecloud.spark.action.Search, value: cn.myfreecloud.spark.action.Search@1f3b992)
 * 	- field (class: cn.myfreecloud.spark.action.Search$$anonfun$getMatch1$1, name: $outer, type: class cn.myfreecloud.spark.action.Search)
 * 	- object (class cn.myfreecloud.spark.action.Search$$anonfun$getMatch1$1, <function1>)
 *
 * @param query
 */


/**
 * 使用继承,混入特质,还是要序列化
 *
 * @param query
 */
class Search(query: String) extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

}
