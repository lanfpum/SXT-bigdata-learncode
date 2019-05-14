package lxpsee.top.scala4learn.ppt.traitDemo

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 17:25.
  */
trait Handler {
  def handle(data: String) {}
}

trait DataValidHandler extends Handler {
  override def handle(data: String) {
    println("check data: " + data)
    super.handle(data)
  }
}

trait SignatureValidHandler extends Handler {
  override def handle(data: String) {
    println("check signature: " + data)
    super.handle(data)
  }
}

class Person2(val name: String) extends SignatureValidHandler with DataValidHandler {
  def sayHello = {
    println("Hello, " + name);
    handle(name)
  }
}

object Person2 {
  def main(args: Array[String]): Unit = {
    val p = new Person2("jim")
    p.sayHello
  }
}
