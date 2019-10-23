package cn.myfreecloud.function


/**
  * map和FlatMap的区别
  *
  */
object FlatMapFunction02 {

  def main(args: Array[String]): Unit = {

    val array = Array(1, 2, 3, 4, 5)

    /** 1   1,2   1,2,3   1,2,3,4    1,2,3,4,5**/

    val result = array.flatMap(1 to _)

    println(result.mkString("||"))

    println("*******************")

    flatMap1()

    println("####################")
    map1()
  }

  def flatMap1(): Unit = {
    val li = List(1, 2, 3)
    val res = li.flatMap(x => x
    match {
      case 3 => List('a', 'b')
      case _ => List(x * 2)
    })

    println(res)
  }

  def map1(): Unit = {
    val li = List(1, 2, 3)
    val res = li.map {
      case 3 => List('a', 'b')
      case x => x * 2
    }
    println(res)
  }
}
