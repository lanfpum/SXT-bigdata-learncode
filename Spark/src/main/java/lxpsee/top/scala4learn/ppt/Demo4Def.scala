package lxpsee.top.scala4learn.ppt

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 10:05.
  */
object Demo4Def {
  def main(args: Array[String]): Unit = {
    /*def sayHello(name:String,age:Int): Unit = {
      if (age >18){
        printf("hi %s, you are a big boy\n", name)
      } else {
        printf("hi %s, you are a little boy\n", name)
      }
    }

    sayHello("LP",30)*/

    /* def sayHello(name:String) = print("Hello " + name)
     sayHello("lp")*/

    //实现累加的功能：
    /* def sum(n:Int) = {
       var sum = 0

       for (i <- 1 to n) {
         sum += i
       }

       sum
     }

     print(sum(4))*/

    //斐波那契数列
    /* def fab(n: Int): Int = {
       if (n <= 1) 1
       else fab(n - 1) + fab(n - 2)
     }

     print(fab(4))*/

    // 使用函数的默认参数
    /*def sayName(fn:String,mn:String="lxpsee",ln:String="top") = fn+mn+ln
    println(sayName("lp"))
    println(sayName("lp","jim"))
    println(sayName("lp","jim","kobe"))*/

    // 使用 变长参数
    /*def sum(nums: Int*) = {
      var sum = 0

      for (i <- nums) {
        sum += i
      }

      sum
    }

    println(sum(1, 2, 3, 5))
    val s = sum(1 to 5: _*)
    println(s)*/

    //使用递归函数实现累加
    def sum2(nums: Int*): Int = {
      if (nums.length == 0) 0
      else nums.head + sum2(nums.tail: _*)
    }

    println(sum2(5, 2, 1))


  }

}
