package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy(func,[ascending], [numTasks]) 案例
 *
 */
object Spark02_Oper11_SortBy {
  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper11_SortBy")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.parallelize(1 to 16,4 )

    // 重新分成两个区
    val resultRDD: RDD[Int] = listRDD.repartition(2)

    resultRDD.collect().foreach(println)

    println("*************************************")

    // coalesce: 可以通过 shuffle:Boolean=true/false决定是否进行分区  coalesce默认是不进行shuffle的,只进行分区的合并,为了提高性能
    // repartition:实际上是调用coalesce的,默认是进行shuffle的
    resultRDD.glom().collect().foreach(array =>{
      println(array.mkString(","))
    })



    // 排序操作
    listRDD.sortBy(x => x,false).collect()

    sc.stop()

  }


}
