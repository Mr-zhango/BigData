package cn.myfreecloud.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Action_01_Reduce {
  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action_01_Reduce")

    val sc = new SparkContext(sparkConfig)

    val rdd1 = sc.makeRDD(1 to 10, 2)
    // 求和
    val reduceResult = rdd1.reduce(_+_)

    rdd1.collect().foreach(println)

    println(reduceResult)

    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))

    val tuple = rdd2.reduce((x,y)=>(x._1 + y._1,x._2 + y._2))

    println(tuple)

  }

}
