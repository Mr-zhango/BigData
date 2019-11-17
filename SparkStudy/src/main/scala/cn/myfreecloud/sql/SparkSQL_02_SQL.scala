package cn.myfreecloud.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_02_SQL {
  def main(args: Array[String]): Unit = {

    //设置Spark计算框架的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_02_SQL")

    //
    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val dataFrame:DataFrame = sparkSession.read.json("input/user.json")



    // 将我们的dataFrame转化为一张表

    dataFrame.createOrReplaceTempView("user")

    // 采用sql的语法访问数据

    sparkSession.sql("select * from user").show

    sparkSession.stop()

  }
}
