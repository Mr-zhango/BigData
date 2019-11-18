package cn.myfreecloud.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamIng01_WordCount {
  def main(args: Array[String]): Unit = {



    // 使用sparkStreaming 完成wordcount

    // spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamIng01_WordCount")

    // 试试数据分析的环境对象
    // 采集周期:以指定的时间为周期  采集实时数据
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))


    // 从指定的端口中采集数据 String 一行一行的数据
    val sockectLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)

    // 将采集的数据进行分解(扁平化)

    val wordDStream: DStream[String] = sockectLineDStream.flatMap( line => line.split(" "))

    // 数据转换结构,方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_,1))

    // 聚合
    val resultDstream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    // 打印
    resultDstream.print()

    // 因为要长期执行,所以不能停止
    // streamingContext.stop()

    // 启动采集器
    streamingContext.start()


    // Driver等待采集器的执行

    // 等待  采集器  终止之后停止
    streamingContext.awaitTermination()
  }
}
