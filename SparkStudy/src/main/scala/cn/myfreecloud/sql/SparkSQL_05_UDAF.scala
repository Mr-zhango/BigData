package cn.myfreecloud.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * 用户自定义的聚合函数:弱类型的
 */
object SparkSQL_05_UDAF {
  def main(args: Array[String]): Unit = {
    //设置Spark计算框架的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_05_UDAF")

    //
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 创建RDD
    val dataFrame:DataFrame = sparkSession.read.json("input/2.json")

    // 自定义聚合函数
    // 创建聚合函数对象

    val udaf = new MyAgeAvgFunction

    // 注册聚合函数
    sparkSession.udf.register("ageAvg",udaf)
    dataFrame.createOrReplaceTempView("user")

    sparkSession.sql("select ageAvg(age) from user").show()

    sparkSession.stop()
  }
}

// 声明用户的自定义聚合函数   求年龄的平均值

// 1. 继承 UserDefinedAggregateFunction
class MyAgeAvgFunction extends UserDefinedAggregateFunction {


  // 函数输入数据的结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 函数返回的数据类型
  override def dataType: DataType = {
    DoubleType
  }

  // 稳定性
  override def deterministic: Boolean = {
    true
  }

  // 当前函数计算时候的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 第一个结构
    buffer(0) = 0L

    // 第二个结构
    buffer(1) = 0L
  }

  // 根据查询结果更新缓冲区的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // buffer.getLong(0) 缓冲区中的年龄

    // input.getLong(0)  没一个年龄

    // buffer.getLong(1) 缓冲区中的数量
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1


  }

  // 将多个结点的缓冲区进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)


  }

  // 计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
