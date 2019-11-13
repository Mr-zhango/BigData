package cn.myfreecloud.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * spark读取Hbase数据
 */
object Hbase01_ReadData {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Hbase01_ReadData")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    conf.set(TableInputFormat.INPUT_TABLE, "student")


    //TableInputFormat rowKey的主键类型
    //从HBase读取数据形成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,

      //TableInputFormat rowKey的主键类型

      classOf[TableInputFormat],
      // key 类型
      classOf[ImmutableBytesWritable],
      // value类型
      classOf[Result])

    val count: Long = hbaseRDD.count()


    println(count)

    /**
     * -- 创建一个表 并且制定rowkey
     * create 'student','info'
     *
     * 查看已经有的表
     * list
     *
     * put 'student','1001','info:name',"zhangsan"
     *
     */
    //对hbaseRDD进行处理
    hbaseRDD.foreach {

      // 1001
      case (rowkey, result) => {

        val cells: Array[Cell] = result.rawCells()

        for (cell <- cells) {

          println(rowkey)
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }




      }
    }

    //关闭连接
    sc.stop()
  }


}
