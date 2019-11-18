package cn.myfreecloud.streaming

/**
 * 有状态数据统计:累加统计单词,保存了之前的统计状态
 */
object StreamIng06_Window_Demo {
  def main(args: Array[String]): Unit = {
    val ints = List(1,2,3,4,5,6)

    // 滑动
    //val iterator: Iterator[List[Int]] = ints.sliding(2)
    //val iterator: Iterator[List[Int]] = ints.sliding(3)
    val iterator: Iterator[List[Int]] = ints.sliding(3,3)

    // 窗口滑动效果展示
    for (list <- iterator){
      println(list.mkString(","))
    }

  }
}