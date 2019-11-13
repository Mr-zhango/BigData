package cn.myfreecloud.spark.sharedata

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 解析 json 数据 注意:一行一个数据
 */
object BroadcastVariables01_ShareData {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadcastVariables01_ShareData")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    // 指定两个分区
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 因为涉及到不同分区的数据进行累加,性能低下
    // val i: Int = dataRDD.reduce(_ + _)

    var sum = 0

    // sum的值都只是在driver端,可以传递到executor端,但是executor端的数据sum不能传递到driver端

    // 所以我们需要累加器来进行共享变量,来进行数据的累加

    // 创建累加器对象
    //

    val accumulator: LongAccumulator = sc.longAccumulator



    //   dataRDD.foreach(i =>
    //      sum = sum + i
    //    )

    dataRDD.foreach {
      case i => {
        // 执行累加器的累加功能
        accumulator.add(i)
      }
    }


    // 取累加器的值
    println(accumulator.value)

    // 释放资源
    sc.stop()

  }

}
