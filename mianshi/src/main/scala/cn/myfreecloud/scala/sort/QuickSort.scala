package cn.myfreecloud.scala.sort

object QuickSort {
  def main(args: Array[String]): Unit = {

    println("**********************quick************************")
    // 测试scala快速排序
    val list: List[Int] = List(100, 2, 3, 89, 5, 865, 4)
    val sortedList: List[Int] = quickSort(list)
    sortedList.foreach(println(_))

    println("**********************Puple************************")
    val listPuple = List(3, 12, 43, 23, 7, 1, 2, 0)


    println("**********************Binary************************")
    val sortedListBinary: List[Int] = quickSort(list)
    sortedListBinary.foreach(println(_))


    println("**********************merge************************")

    val listMerge = List(3, 12, 43, 23, 7, 1, 2, 0)
    val sortedListMerged: List[Int] = mergedSort((x: Int, y: Int) => x < y)(listMerge)
    sortedListMerged.foreach(println(_))

  }

  // scala 快速排序,时间复杂度O(nlog(n))
  def quickSort(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case List() => List()
    case head :: tail =>
      val (left, right) = tail.partition(_ < head)
      quickSort(left) ::: head :: quickSort(right)
  }

  def sort(list: List[Int]): List[Int] = list match {
    case List() => List()
    case head :: tail => compute(head, sort(tail))
  }

  def compute(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if (data <= head) data :: dataSet else head :: compute(data, tail)
  }

  // scala 二分查找,时间复杂度O(log2n)
  def binarySort(list: List[Int], x: Int): Int = {
    var start = 0
    var end = list.size - 1
    var index = 0
    while (start <= end) {
      index = (start + end) / 2
      if (list(index) < x) {
        start = index + 1
      } else if (list(index) > x) {
        end = index - 1
      } else {
        println(s"${x} in index:${index}")
        return index
      }

    }
    -1
  }

  // scala 归并排序核心实现,平均时间复杂度O(nlog(n))
  def mergeSort(left: List[Int], right: List[Int]): List[Int] = (left, right) match {
    case (Nil, _) => left
    case (_, Nil) => right
    case (x :: xTail, y :: yTail) =>
      if (x <= y) x :: mergeSort(xTail, right)
      else
        y :: mergeSort(left, yTail)
  }


  // scala 归并排序 常用,平均时间复杂度O(nlog(n))
  def mergedSort[T](less: (T, T) => Boolean)(list: List[T]): List[T] = {

    def merged(xList: List[T], yList: List[T]): List[T] = {
      (xList, yList) match {
        case (Nil, _) => yList
        case (_, Nil) => xList
        case (x :: xTail, y :: yTail) => {
          if (less(x, y)) x :: merged(xTail, yList)
          else
            y :: merged(xList, yTail)
        }
      }
    }

    val n = list.length / 2
    if (n == 0) list
    else {
      val (x, y) = list splitAt n
      merged(mergedSort(less)(x), mergedSort(less)(y))
    }
  }

}
