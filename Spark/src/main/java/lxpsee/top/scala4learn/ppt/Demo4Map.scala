package lxpsee.top.scala4learn.ppt

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/22 15:31.
  */
object Demo4Map {
  def main(args: Array[String]): Unit = {

    /*val ages = Map("Leo" -> 30, "jim" -> 20, "tom" -> 22)
    //    ages("jim") = 22
    println("no " + ages("jim"))

    val ages2 = scala.collection.mutable.Map("Leo" -> 30, "jim" -> 20, "tom" -> 22)
    ages2("jim") = 22
    println("no " + ages2("jim"))

    for ((key, value) <- ages) println(key + " " + value)

    val resmap = for ((key, value) <- ages) yield (value, key)
    for ((key, value) <- resmap) println(" res " + key + " " + value)*/

    val names = Array("leo", "jack", "mike")
    val ages = Array(30, 24, 26)
    val nameAges = names.zip(ages)
    for ((key, value) <- nameAges) println(key + " " + value)

  }

}
