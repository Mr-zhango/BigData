package cn.myfreecloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求:生成数据,按照指定规则进行数据的数据进行去重,并且放到指定的分区中去
 */
object Spark02_Oper09_Distinct {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper07_Filter")

    val sc = new SparkContext(sparkConfig)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    // shuffle 后的结果放到2个分区中去
    val filterRDD: RDD[Int] = listRDD.distinct(2)

    /**
     * 去重后的数据,顺序被打乱了
     */
    filterRDD.saveAsTextFile("output")

    sc.stop()
  }

}
