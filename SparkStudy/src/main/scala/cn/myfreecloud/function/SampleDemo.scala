package cn.myfreecloud.function

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，
  * true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
  *   例子从RDD中随机且有放回的抽出50%的数据，随机种子值为3（即可能以1 2 3的其中一个起始值）
  */
object SampleDemo {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[3]").setAppName("TestAPP")

    val sc = new SparkContext(sparkConfig)

    val rdd = sc.parallelize(1 to 10)

    println("***********************")
    rdd.collect().foreach(println(_))


    var sample1 = rdd.sample(true,0.4,2)

    println("***********************")
    sample1.collect().foreach(println(_))


    var sample2 = rdd.sample(false,0.2,3)

    println("***********************")
    sample2.collect().foreach(println(_))

    sc.stop()
  }

}
