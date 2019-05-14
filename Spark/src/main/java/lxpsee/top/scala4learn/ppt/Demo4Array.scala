package lxpsee.top.scala4learn.ppt

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 11:00.
  */
object Demo4Array {
  def main(args: Array[String]): Unit = {
    /*val a = new Array[Int](10)
    a(0) = 1

    for(e <- a) println(e)*/

    /*val a = Array("Hello","Jim")
    a(0) = "hi"
    for (e <- a) println(e)*/

    /*val a= Array("Hi", 30)
    for (e <- a) println(e)*/

    import scala.collection.mutable.ArrayBuffer

    /*val b = ArrayBuffer[Int]()
    b += 1
    b += (2, 3, 5)
    b ++= Array(6, 7, 8)
    b.trimEnd(2)

    for (e <- b) println(e)
    val a = b.toArray
    val sum = a.sum
    println(sum)
    println(a.max)
    println(a.mkString("+"))
    println(a.mkString("<", "-", ">"))*/

    /*val a = Array(1, 4, 5, 7, 8, 2)
    val a2 = for (e <- a) yield e * e
    for (ee <- a2) println(ee)
    println("---------------")

    val a3 = for (e <- a if e % 2 == 0) yield e * e
    for (ee <- a3) println(ee)
    println("---------------")

    val a4 = a.filter(_ % 2 == 0).map(3 * _)
    for (ee <- a4) println(ee)
    println("---------------")*/

    // 每发现一个第一个负数之后的负数，就进行移除，性能较差，多次移动数组
    /*val a = ArrayBuffer[Int]()
    a += (4, 2, 3, 5, -1, -8, -6, -1, -5)

    var ffn = false
    var al = a.length
    var index = 0

    while (index < al) {
      if (a(index) >= 0) index += 1
      else {
        if (!ffn) {
          ffn = true
          println(a(index) + " +++")
          index += 1
        } else {
          println(a(index) + " -- ")
          a.remove(index)
          index -= 1
        }
      }
    }

    for (e <- a) println(e)*/

    val a = ArrayBuffer[Int]()
    a += (4, 2, 3, 5, -1, 7, -8, -6, -1, -5, 5)

    var ffn = false
    // 记住不用删除的元素索引
    val keepIndexes = for (i <- 0 until a.length if !ffn || a(i) >= 0) yield {
      if (a(i) < 0) {
        ffn = true
      }
      i
    }

    for (e <- keepIndexes) print(e + " ")
    println("------")

    // 将原来的可变数组的元素进行更新，移动不删除的元素到最前面
    for (i <- 0 until keepIndexes.length) {
      a(i) = a(keepIndexes(i))
    }

    // 删除原来元素后面的所有元素
    a.trimEnd(a.length - keepIndexes.length)
    for (e <- a) println(e)

  }


}
