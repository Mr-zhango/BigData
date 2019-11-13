package cn.myfreecloud.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * spark读取Hbase数据
 */
object Hbase04_WriteToMysql {

  def main(args: Array[String]): Unit = {


    val sparkConfig: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Hbase04_WriteToMysql")

    // 创建spark上下文对象
    val sc = new SparkContext(sparkConfig)

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()

    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1002", "zhaowu"), ("10003", "zhaoliu"), ("10004", "tianqi")))

    // 创建RDD
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {

      case (rowKey, name) => {

        // Put 对象的参数是rowKey
        val put = new Put(Bytes.toBytes(rowKey))

        // 列族                                         key                     value
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        // rowkey
        (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
      }
    }


    // 当前作业配置Hbase参数
    val jobConf = new JobConf(conf)

    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")


    putRDD.saveAsHadoopDataset(jobConf)


    //关闭连接
    sc.stop()
  }


}
