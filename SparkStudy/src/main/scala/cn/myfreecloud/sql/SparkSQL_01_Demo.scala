package cn.myfreecloud.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_01_Demo {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01_Demo")

    //
    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val dataFrame:DataFrame = sparkSession.read.json("input/user.json")

    dataFrame.show()

    sparkSession.stop()

  }
}
