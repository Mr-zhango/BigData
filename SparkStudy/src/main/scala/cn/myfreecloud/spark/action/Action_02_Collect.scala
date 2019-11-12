package cn.myfreecloud.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Action_02_Collect {
  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action_01_Collect")

    val sc = new SparkContext(sparkConfig)

    val rdd1 = sc.makeRDD(1 to 10)

    rdd1.collect().foreach(println)


    // 统计总个数
    val count = rdd1.count()

    println("数据总数:"+count)

    println("**************************************")

    // 取出第一个
    val first = rdd1.first()

    println("取出第一个:"+first)


    // 取出前三个
    val take = rdd1.take(3)

    take.foreach(println)

    /**
     * takeOrdered(n)案例
     * 1. 作用：返回该RDD排序后的前n个元素组成的数组
     * 2. 需求：创建一个RDD，统计该RDD的条数
     */

    val takeOrderedRdd = sc.parallelize(Array(2,5,4,6,8,3,1))

   val ints: Array[Int] = takeOrderedRdd.takeOrdered(3)


    ints.foreach(println)


    // aggregate

    /**
     * 1. 参数：(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
     * 2. 作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
     *    然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。这个函数最终返回的类型不需要和RDD中元素类型一致。
     */



  }

}
