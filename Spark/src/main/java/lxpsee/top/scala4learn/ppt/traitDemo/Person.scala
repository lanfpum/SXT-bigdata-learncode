package lxpsee.top.scala4learn.ppt.traitDemo

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 17:20.
  */
trait Logged {
  def log(msg: String) {}
}

trait MyLogger extends Logged {
  override def log(msg: String) {
    println("log: " + msg)
  }
}

class Person(val name: String) extends Logged {
  def sayHello {
    println("Hi, I'm " + name);
    log("sayHello is invoked!")
  }
}

object Person {
  def main(args: Array[String]): Unit = {
    val p1 = new Person("leo")
    p1.sayHello
    val p2 = new Person("jack") with MyLogger
    p2.sayHello
  }
}
