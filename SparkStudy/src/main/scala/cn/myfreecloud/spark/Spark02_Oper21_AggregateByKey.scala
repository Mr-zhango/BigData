package cn.myfreecloud.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 1. 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置。
 * 2. 需求：创建一个pairRDD，取出每个分区相同的key的最大值,然后相加
 * 难点是不同分区之间的数据进行相加
 */
object Spark02_Oper21_AggregateByKey {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper21_AggregateByKey")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    // 查看分区情况
    rdd.glom().collect().foreach(array =>{
      println(array.mkString(","))
    })

    // 初始值是0  相同的分区取最大值,不同的分区进行累加
    val value = rdd.aggregateByKey(0)(math.max(_,_)  ,_+_)

    value.collect().foreach(println)


    // 相同分区内相同的key进行相加,不同分区内相同的key进行相加 wordCount
    val value2 = rdd.aggregateByKey(0)(_+_,_+_)

    value2.collect().foreach(println)
    sc.stop()

  }


}
