package lxpsee.top.scala4learn.ppt.genericDemo

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/25 10:17.
  */
class Person(val name: String) {
  def sayHello = println("hello , i'm  " + name)

  def makeFriends(p: Person): Unit = {
    sayHello
    p.sayHello
  }
}

class Student(name: String) extends Person(name)

class Party[T <: Person](p1: T, p2: T) {
  def play = p1.makeFriends(p2)
}

class Demo4Generic {

}

object Demo4Generic {
  def main(args: Array[String]): Unit = {
    val p1 = new Person("jim")
    val p2 = new Person("green")
    val par = new Party[Person](p1, p2)
    par.play

    val s1 = new Student("tom")
    val s2 = new Student("kobe")
    val par2 = new Party[Student](s1, s2)
    par2.play

  }
}

