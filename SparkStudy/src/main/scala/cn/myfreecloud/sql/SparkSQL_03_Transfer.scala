package cn.myfreecloud.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL_03_Transfer {
  def main(args: Array[String]): Unit = {
    //设置Spark计算框架的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_02_SQL")

    //
    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 创建RDD
    val rdd:RDD[(Int,String,Int)] = sparkSession.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",20),(3,"wangwu",20)))

    // sparkSession 对象的名字
    // 引入隐式转换需要的包
    import sparkSession.implicits._
    // 转换为DF
    val dataFram:DataFrame = rdd.toDF("id","name","age")

    // 转换为DS
    val dataSet:Dataset[User] =  dataFram.as[User]


    // 转换为DF
    val df:DataFrame = dataSet.toDF()

    // 转换为RDD
    val rddResult:RDD[Row] = df.rdd


    rddResult.foreach(
      // 获取数据时,可以通过索引来访问数据
      row => {
        println(row.getInt(0))
      }
    )
    sparkSession.stop()
  }
}


// 增加样例类

case class User(id:Int,name:String,age:Int)