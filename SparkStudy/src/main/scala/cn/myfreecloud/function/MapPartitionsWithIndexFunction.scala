package cn.myfreecloud.function

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，
  * func的函数类型必须是(Int, Interator[T]) => Iterator[U]
  */
object MapPartitionsWithIndexFunction {
  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))

    val result = rdd.mapPartitionsWithIndex(partitionsFunIndex)

    result.collect().foreach(println(_))

    sc.stop()


  }

  def partitionsFunIndex(index : Int, iter : Iterator[(String,String)]) : Iterator[String] = {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = "["+index+"]"+next._1 :: woman
        case _ =>
      }
    }
    woman.iterator
  }


}
