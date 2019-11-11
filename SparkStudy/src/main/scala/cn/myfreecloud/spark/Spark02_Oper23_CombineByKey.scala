package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 参数：(createCombiner: V => C,  mergeValue: (C, V) => C,  mergeCombiners: (C, C) => C)
 * 1.	作用：对相同K，把V合并成一个集合。
 * 2.	参数描述：
 * （1）createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。如果这是一个新的元素,combineByKey()会使用一个叫作createCombiner()的函数来创建那个键对应的累加器的初始值
 * （2）mergeValue: 如果这是一个在处理当前分区之前已经遇到的键，它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
 * （3）mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
 *
 *
 * 3.	需求：创建一个pairRDD，根据key计算每种key的均值。（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
 */
object Spark02_Oper23_CombineByKey {

  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper23_CombineByKey")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val combine = rdd.combineByKey(
      (_, 1), // (a,1)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // 分区内函数计算
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // 分区间函数计算规则
    )


    combine.collect().foreach(println)


    println("****************************")

    val result = combine.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }


    result.collect().foreach(println)
    sc.stop()


    val rddFinnal = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    rddFinnal.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    println("****************************************")
    rddFinnal.foldByKey(0)(_ + _).collect().foreach(println)

    println("****************************************")

    //rddFinnal.combineByKey(x=>x,_+_,_+_).collect().foreach(println)
    rddFinnal.combineByKey(

      x => x,
      (x: Int,y) => x + y,
      (x: Int, y: Int) => x + y

    ).collect().foreach(println)


    // 无法对x类型进行推断
    //rddFinnal.combineByKey(x => x, (x,y) => x + y, (x: Int, y: Int) => x + y).collect().foreach(println)
  }


}
