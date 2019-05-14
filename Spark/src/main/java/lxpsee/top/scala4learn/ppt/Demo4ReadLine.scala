package lxpsee.top.scala4learn.ppt

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 09:43.
  */
object Demo4ReadLine {

  def main(args: Array[String]): Unit = {
    //    val name = readLine("Welcome to Game House. Please tell me your name: ")
    print("Thanks. Then please tell me your age: ")
    val age = readInt()

    if (age > 18) {
      printf("Hi, %s, you are %d years old, so you are legel to come here!", age)
    } else {
      printf("Sorry, boy, %s, you are only %d years old. you are illegal to come here!", age)

    }
  }

}
