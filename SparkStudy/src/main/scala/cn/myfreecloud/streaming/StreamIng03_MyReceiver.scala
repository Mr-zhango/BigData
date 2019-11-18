package cn.myfreecloud.streaming

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 自定义采集器
 */
object StreamIng03_MyReceiver {
  def main(args: Array[String]): Unit = {


    // 使用sparkStreaming 完成wordcount

    // spark的配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamIng03_MyReceiver")

    // 试试数据分析的环境对象
    // 采集周期:以指定的时间为周期  采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))


    // 从指定的端口中采集数据 String 一行一行的数据
    val receiverDStreaming: DStream[String] = streamingContext.receiverStream(new MyReceiver("hadoop102",9999))

    // 将采集的数据进行分解(扁平化)

    val wordDStream: DStream[String] = receiverDStreaming.flatMap(
      line => line.split(" ")
    )

    // 数据转换结构,方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 聚合
    val resultDstream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 打印
    resultDstream.print()

    // 启动采集器
    streamingContext.start()

    streamingContext.awaitTermination()
  }
}


// 声明采集器
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {


  var socket: java.net.Socket = null

  def receive(): Unit = {
    socket = new java.net.Socket(host, port)

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    var line: String = null

    while ((line = reader.readLine()) != null) {
      // 将采集的数据存储到采集器内部进行转换

      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }


    }
  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket == null
    }
  }
}