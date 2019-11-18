package cn.myfreecloud.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 有状态数据统计:累加统计单词,保存了之前的统计状态
 */
object StreamIng05_Update_Status {
  def main(args: Array[String]): Unit = {


    // 使用sparkStreaming 完成wordcount

    // spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamIng05_Update_Status")

    // 试试数据分析的环境对象
    // 采集周期:以指定的时间为周期  采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 保存数据的状态,需要设定检查点的路径
    streamingContext.sparkContext.setCheckpointDir("checkpointDir")

    // 从指定的端口中采集数据 String 一行一行的数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop102:2181",
      "root",
      Map("atguigu" -> 3)

    )

    // 将采集的数据进行分解(扁平化)

    val wordDStream: DStream[String] = kafkaDStream.flatMap(
      t => t._2.split(" ")
    )

    // 数据转换结构,方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // val resultDstream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)


    // 将转换后的数据进行聚合处理
    val ststusDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        // 从没出现过就是0
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    // 打印
    ststusDStream.print()

    // 启动采集器
    streamingContext.start()

    streamingContext.awaitTermination()
  }
}