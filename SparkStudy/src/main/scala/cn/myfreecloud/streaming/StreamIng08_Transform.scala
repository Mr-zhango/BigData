package cn.myfreecloud.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 转换函数
 */
object StreamIng08_Transform {
  def main(args: Array[String]): Unit = {

    // 使用sparkStreaming 完成wordcount

    // spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamIng08_Transform")

    // 采集周期:以指定的时间为周期  采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 启动采集器
    streamingContext.start()

    streamingContext.awaitTermination()
  }
}