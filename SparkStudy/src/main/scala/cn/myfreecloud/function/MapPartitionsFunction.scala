package cn.myfreecloud.function

import org.apache.spark.{SparkConf, SparkContext}

/**
  *类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U]。
  * 假设有N个元素，有M个分区，那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区
  */
object MapPartitionsFunction {


  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))

    rdd.collect().foreach(println(_))

    val result = rdd.mapPartitions(partitionsFun)

    result.collect().foreach(println(_))

    sc.stop()
  }


  def partitionsFun(iter : Iterator[(String,String)]) : Iterator[String] = {

    var woman = List[String]()

    woman.foreach("woman的值是:"+println(_))

    while (iter.hasNext){

      val next = iter.next()

      next match {
        case (_,"female") => woman = next._1 :: woman
        case _ =>
      }
    }
    woman.iterator
  }

}


