package cn.myfreecloud.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DataSetWcApp {
  def main(args: Array[String]): Unit = {
    // 1.env
    // 2.source
    // 3.transform
    // 4.sink
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val textDataSet: DataSet[String] = environment.readTextFile("D:\\input\\hello.txt")

    val aggSet = textDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    aggSet.print()
  }

}
