package cn.myfreecloud.spark.lineage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 检查点
 */
object Lineage01_CheckPoint {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Lineage01_CheckPoint")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    // 设定检查点的保存目录

    // 副本一般保存在HDFS上
    sc.setCheckpointDir("checkpointDir")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    // 创建检查点
    // val unit:Unit = mapRDD.checkpoint()

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.checkpoint()

    // 必须有行动算子才会执行checkpoint操作
    reduceRDD.foreach(println)

    // 查看血缘关系
    println(reduceRDD.toDebugString)

    // 释放资源
    sc.stop()

  }

}
