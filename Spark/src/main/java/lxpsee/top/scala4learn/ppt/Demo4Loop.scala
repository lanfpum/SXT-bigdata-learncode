package lxpsee.top.scala4learn.ppt

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 09:47.
  */
object Demo4Loop {
  def main(args: Array[String]): Unit = {

    //Scala有while do循环，基本语义与Java相同
    //   var n = 10

    /* while (n > 0) {
      println(n)
      n -= 1
    }*/

    // Scala没有for循环，只能使用while替代for循环，或者使用简易版的for语句
    /*
    for (i <- 1 to n) println(i)*/

    //    for (i <- 1 until n) println(i)

    //    for(c <- "Hello World") println(c)

    /*import scala.util.control.Breaks._

    breakable {
      var n = 10

      for (c <- "hello world") {
        if (n == 5) break()
        println(c)
        n -= 1
      }
    }*/

    // 多重for循环：九九乘法表
    /*for (i <- 1 to 9; j <- 1 to 9) {
      if (j == 9) {
        println(i * j)
      } else {
        print(i * j + " ")
      }
    }*/

    //if守卫：取偶数
    //    for (i <- 1 to 100 if i % 2 == 0) print(i + " ")

    for (i <- 1 to 10) yield i
  }

}
