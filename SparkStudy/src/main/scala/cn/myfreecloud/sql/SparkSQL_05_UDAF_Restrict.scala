package cn.myfreecloud.sql

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator


/**
 * 用户自定义的聚合函数:强类型的
 */
object SparkSQL_05_UDAF_Restrict {
  def main(args: Array[String]): Unit = {
    //设置Spark计算框架的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_05_UDAF_Restrict")

    //
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    // 引入隐式转换需要的包
    import sparkSession.implicits._


    // 创建聚合函数对象

    val udaf = new MyRestrictAgeAvgFunction

    // 将聚合函数转换成查询的列
    val avgCloum: TypedColumn[UserBean, Double] = udaf.toColumn.name("restrictAvg")


    // 创建RDD
    val dataFrame: DataFrame = sparkSession.read.json("input/2.json")


    val userDs: Dataset[UserBean] = dataFrame.as[UserBean]

    // 应用函数
    userDs.select(avgCloum).show()


    sparkSession.stop()
  }
}

// 声明用户的自定义聚合函数   求年龄的平均值

case class UserBean(name: String, age: BigInt)

// 样例类的属性默认是val的 想要改变要写成var
case class AvgBuffer(var sum: BigInt, var count: Int)

// 1. 继承 Aggregator
// 2.实现方法
class MyRestrictAgeAvgFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  // 初始化值
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  /**
   * 聚合数据
   *
   * @param b
   * @param a
   * @return
   */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }


  /**
   * 缓冲区的合并操作
   *
   * @param b1
   * @param b2
   * @return
   */
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  /**
   * 完成计算
   *
   * @param reduction
   * @return
   */
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  /**
   *
   * @return
   */
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
