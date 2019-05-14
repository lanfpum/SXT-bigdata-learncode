package lxpsee.top.scala4learn.ppt.list

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/25 07:23.
  */
class Demo4List {

  def decorator(l: List[Int], prefix: String): Unit = {
    if (l != Nil) {
      println(prefix + l.head)
      decorator(l.tail, prefix)
    }
  }

}

object Demo4List {
  def main(args: Array[String]): Unit = {
    /*val d = new Demo4List
    val list = List(1, 2, 3, 4)
    d.decorator(list, "jim ")*/

    /*val list = scala.collection.mutable.LinkedList(1, 4, 5, 6, 7, 8)
    var currentList = list

    while (currentList != Nil) {
      currentList.elem = currentList.elem * 2
      println(currentList.elem)
      currentList = currentList.next
    }*/

    /*val list = scala.collection.mutable.LinkedList(1, 4, 5, 6, 7, 8)
    var currentList = list
    var first = true

    while (currentList != Nil && currentList.next != Nil) {

      if (first) {
        currentList.elem = currentList.elem * 2
        println(currentList.elem)
        first = false
      }

      currentList = currentList.next.next

      if (currentList != Nil) {
        currentList.elem = currentList.elem * 2
        println(currentList.elem)
      }
    }*/

    val l = List("Leo", "Jen", "Peter", "Jack").zip(List(100, 90, 75, 83))
    l.foreach(println(_))
  }
}
