package demo

import scala.collection.mutable

object TestScalaFunctiion {

  def main(args: Array[String]): Unit = {


    val countMap = new mutable.HashMap[String, Int]()

    if(!countMap.contains("6")) {
      countMap += ("6" -> 0)
    }else{

    }

    countMap.update("6", countMap("6") + 1)

//    val iterator = countMap.iterator
//
//    while (iterator.hasNext){
//      println(iterator.next())
//    }


    val keys = countMap.keySet
    for(key <- keys){
      //println(key)
      println(key + countMap.get(key))
    }


  }
}
