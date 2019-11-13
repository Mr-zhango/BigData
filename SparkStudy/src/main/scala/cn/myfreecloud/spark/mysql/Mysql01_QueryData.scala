package cn.myfreecloud.spark.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


/**
 * 解析 json 数据 注意:一行一个数据
 */
object Mysql01_QueryData {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Mysql01_QueryData")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    //创建JdbcRDD
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from `user` where `id`>=? and id <= ?;",
      1,
      10,
      1,
      r => (r.getString(1), r.getString(2),r.getString(3))
    )

    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)



    // 释放资源
    sc.stop()

  }

}
