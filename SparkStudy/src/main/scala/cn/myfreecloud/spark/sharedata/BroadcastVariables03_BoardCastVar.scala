package cn.myfreecloud.spark.sharedata

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 解析 json 数据 注意:一行一个数据
 */
object BroadcastVariables03_BoardCastVar {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadcastVariables03_BoardCastVar")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    // 指定两个分区
    val dataRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

    val list = List((1, 1), (2, 2), (3, 3))

    // 使用广播变量可以减少数据的传输
    val boardCastRDD: Broadcast[List[(Int, Int)]] = sc.broadcast(list)


    // 使用map来实现了一个类似于join的操作
    // 构建广播变量
    val resultRDD: RDD[(Int, (String, Any))] = dataRDD.map {

      case (key, value) => {
        var v2: Any = null

        // 使用广播变量  是一种调优的策略
        for (t <- boardCastRDD.value) {

          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
      }

    }

    resultRDD.foreach(println)

    // 释放资源
    sc.stop()

  }

}
