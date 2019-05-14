package lxpsee.top.sparkBookLearn.scalaVersion

import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/15 08:23.
  *
  * 求平方
  */
object SquareDemoScala {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SquareDemoScala").setMaster("local"))
    val result = sc.parallelize(List(1, 2, 3, 4)).map(x => x * x)
    println(result.collect().mkString(","))
    val resAvg = sc.parallelize(List(2, 5, 6, 7, 9))
      .aggregate((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = resAvg._1 / resAvg._2.toDouble
    println(avg.toString)
  }

}
