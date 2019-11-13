package cn.myfreecloud.spark.mysql

import java.sql
import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 解析 json 数据 注意:一行一个数据
 */
object Mysql02_SaveDataToMysql {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Mysql02_SaveDataToMysql")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd"
    val userName = "root"
    val passWd = "123456"


    val dataRDD = sc.makeRDD(List(("1", "ceshi1", "10"), ("2", "ceshi2", "20"), ("3", "ceshi3", "30")))


    /*
       这样写会导致序列化失败
    val connection = DriverManager.getConnection(url, userName, passWd)

     dataRDD.foreach {
       case (id,name,age) => {

         Class.forName(driver)

         val sql = "insert into `user` (id,name,age) values (?,?,?) "

         val statement = connection.prepareStatement(sql)
         statement.setString(1,id)
         statement.setString(2,name)
         statement.setString(3,age)


         // 执行sql
         statement.execute()
         statement.close()

       }

     }
     connection.close()*/


    // 获取每个分区内的数据,单独进行计算
    dataRDD.foreachPartition(datas => {

      // 在每个分区内部创建一次数据库的链接
      // 缺点就是单个分区中数据过多会造成内存溢出
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, userName, passWd)


      datas.foreach {

        case (id, name, age) => {
          val sql = "insert into `user` (id,name,age) values (?,?,?) "

          val statement = connection.prepareStatement(sql)
          statement.setString(1, id)
          statement.setString(2, name)
          statement.setString(3, age)

          // 执行sql
          statement.execute()
          statement.close()
        }

      }

      connection.close()
    })

    // 释放资源
    sc.stop()

  }

}
