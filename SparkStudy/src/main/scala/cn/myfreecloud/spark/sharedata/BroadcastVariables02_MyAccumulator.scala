package cn.myfreecloud.spark.sharedata

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 自定义累加器
 */
object BroadcastVariables02_MyAccumulator {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadcastVariables02_MyAccumulator")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    // 指定两个分区
    val dataRDD: RDD[String] = sc.makeRDD(List("hello","world","hive","hbase","scala","spark"), 2)

    // 因为涉及到不同分区的数据进行累加,性能低下
    // val i: Int = dataRDD.reduce(_ + _)

    var sum = 0

    // sum的值都只是在driver端,可以传递到executor端,但是executor端的数据sum不能传递到driver端

    // 所以我们需要累加器来进行共享变量,来进行数据的累加

    // 创建累加器对象
    val accumulator = new WordAccumulator

    // 向spark进行累加器的注册
    sc.register(accumulator)

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

/**
 * 自定义累加器实现: 单词累加
 * 1:继承累加器 AccumulatorV2
 * 2:重写 or 实现抽象方法
 * 3:创建累加器
 */
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  // 实现,ctrl + i   重写,ctrl + o

  val list = new util.ArrayList[String]()

  // 当前的累加器是否为初始化状态  为空就是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  // 复制 产生一个新的累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator();
  }

  // 重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  /** 向累加器中增加数据 **/
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }

  }

  // 合并,不同分区之间进行合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }

}
