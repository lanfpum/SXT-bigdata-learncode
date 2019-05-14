package lxpsee.top.scala4learn.ppt.traitDemo

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 17:30.
  */
class Person3 {
  println("Person's constructor!")
}

trait Logger1 {
  println("Logger's constructor!")
}

trait MyLogger1 extends Logger1 {
  println("MyLogger's constructor!")
}

trait TimeLogger1 extends Logger1 {
  println("TimeLogger's constructor!")
}

class Student1 extends Person3 with MyLogger1 with TimeLogger1 {
  println("Student's constructor!")
}

object Person3 {
  def main(args: Array[String]): Unit = {
    val p = new Student1

  }
}

