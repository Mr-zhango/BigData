package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求:生成数据,按照指定规则进行数据的过滤
 */
object Spark02_Oper07_Filter {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper07_Filter")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    // 分组后形成了对偶元组(K_V  分组的key_分组的数据集合)
    val filterRDD: RDD[Int] = listRDD.filter(x => x%2 ==0)

    filterRDD.collect().foreach(println)

    sc.stop()
  }
}
