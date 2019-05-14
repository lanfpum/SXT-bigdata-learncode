package lxpsee.top.scala4learn.ppt

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/25 08:44.
  */
object Demo4Match {
  def main(args: Array[String]): Unit = {
    def greeting(arr: Array[String]): Unit = {
      arr match {
        case Array("Leo") => println("hi , leo ")
        case Array(girl1, girl2, girl3) => println("Hi, girls, nice to meet you. " + girl1 + " and " + girl2 + " and " + girl3)
        case Array("Leo", _*) => println("Hi, Leo, please introduce your friends to me.")
        case _ => println("hey, who are you?")
      }
    }

    def greeting4List(list: List[String]): Unit = {
      list match {
        case "Leo" :: Nil => println("Hi, Leo!")
        case girl1 :: girl2 :: girl3 :: Nil => println("Hi, girls, nice to meet you. " + girl1 + " and " + girl2 + " and " + girl3)
        case "Leo" :: tail => println("Hi, Leo, please introduce your friends to me.")
        case _ => println("hey, who are you?")
      }
    }

    val a = Array("Leo", "tom")
    greeting(a)

    val l = List("Leo")
    greeting4List(l)
  }

  val grades = Map("Leo" -> "A", "Jack" -> "B", "Jen" -> "C")

  def getGrade(name: String): Unit = {
    val grade = grades.get(name)
    grade match {
      case Some(grade) => println("your grade is " + grade)
      case None => println("Sorry, your grade information is not in the system")
    }
  }

  getGrade("Leo")
  getGrade("LP")

}
