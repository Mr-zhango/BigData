package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求:生成数据,按照指定规则进行数据的过滤
 */
object Spark02_Oper08_Sample {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper07_Filter")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    /**
     * false: 表示不放回的抽样
     * fraction:1 都满足   0:都不满足 0.4:每个数据对0.4进行打分
     * seed:1
     */
    val filterRDD: RDD[Int] = listRDD.sample(false,0.6,1)

    filterRDD.collect().foreach(println)

    sc.stop();
  }
}
