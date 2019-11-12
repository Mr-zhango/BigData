package cn.myfreecloud.spark.action

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 1. 作用：在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。
 * 2. 参数描述：
 * （1）zeroValue：给每一个分区中的每一个key一个初始值；
 * （2）seqOp：函数用于在每一个分区中用初始值逐步迭代value；
 * （3）combOp：函数用于合并每个分区中的结果。
 * 3. 需求：创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
 */

object Action_03_Aggregate {
  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action_03_Aggregate")

    val sc = new SparkContext(sparkConfig)


    val rdd1 = sc.parallelize(1 to 10)

    val result = rdd1.aggregate(0)(_+_,_+_)

    println(result)


    val rdd2 = sc.parallelize(1 to 10)

    val result2 = rdd2.fold(0)(_+_)

    println(result2)

  }

}
