/*
package lxpsee.top.scala4learn.ppt.actorDemo

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/25 10:53.
  */
class HelloActor extends scala.actors.Actor {
  override def act(): Unit = {
    while (true) {
      receive {
        case name: String => println("Hello, " + name)
      }
    }
  }
}

class Demo4Actor {

}

object Demo4Actor {
  def main(args: Array[String]): Unit = {
    val h = new HelloActor
    h.start()
    for (i <- 1 to 10) {
      h ! "jin" + i
    }

  }
}
*/
