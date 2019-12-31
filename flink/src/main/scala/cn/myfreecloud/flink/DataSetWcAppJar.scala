package cn.myfreecloud.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object DataSetWcAppJar {
  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)

    val inputPath = tool.get("input")

    val outputPath = tool.get("output")


    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val textDataSet: DataSet[String] = environment.readTextFile(inputPath)

    val aggSet = textDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    aggSet.writeAsCsv(outputPath).setParallelism(1)

    environment.execute()

  }

}
