package cn.myfreecloud.spark.action

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 结果保存成文件
 */

object Action_03_SaveAsXXXFile {
  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action_03_SaveAsXXXFile")

    val sc = new SparkContext(sparkConfig)


    val rdd = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))

    rdd.saveAsTextFile("out1")
    rdd.saveAsSequenceFile("out2")
    rdd.saveAsObjectFile("out3")

  }

}
