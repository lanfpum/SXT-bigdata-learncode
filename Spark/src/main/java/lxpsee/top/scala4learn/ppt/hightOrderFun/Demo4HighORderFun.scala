package lxpsee.top.scala4learn.ppt.hightOrderFun

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/25 06:26.
  */
class Demo4HighORderFun {
  /* def sayHello(name: String): Unit = {
     println("hello " + name)
   }

   val sayHelloVal = sayHello _*/

  /*val sayHello = (name: String) => println("hello " + name)

  def greeting(func: (String) => Unit, name: String) {
    func(name)
  }*/

  def getGreetingFun(msg: String) = (name: String) => println(msg + "," + name)

  val greetingFun = getGreetingFun("hello ")
}

object Demo4HighORderFun {
  def main(args: Array[String]): Unit = {
    val d = new Demo4HighORderFun
    //    d.sayHelloVal("寂寞")
    //    d.greeting(d.sayHello, "Tom")
    d.greetingFun("leo")
  }
}
