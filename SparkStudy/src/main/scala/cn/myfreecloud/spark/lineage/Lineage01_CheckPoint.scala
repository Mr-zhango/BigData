package cn.myfreecloud.spark.lineage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 检查点
 */
object Lineage01_CheckPoint {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Lineage01_CheckPoint")

    val sc = new SparkContext(sparkConfig)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[(Int,Int)] = rdd.map((_,1))

    val value:RDD[(Int,Int)] = mapRDD.reduceByKey(_+_)


  }

}
