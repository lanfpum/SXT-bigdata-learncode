package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 18:49.
  *
  * 排序
  * 取值
  */
object GroupTop3 {
  def main(args: Array[String]): Unit = {
    val context = new SparkContext(new SparkConf().setMaster("local").setAppName("GroupTop3Scala"))
    context.textFile(LPConstants.SCORE_TXT_LOCAL_FILE_PATH)
      .map(line => (line.split(" ")(0), line.split(" ")(1)))
      .groupByKey()
      .map(g => {
        import scala.collection.mutable.ArrayBuffer
        val beforSort = ArrayBuffer[Int]()
        val iterator = g._2.iterator

        while (iterator.hasNext) {
          beforSort += iterator.next().toInt
        }

        val sortArr = beforSort.sorted.reverse // 降序排序，buff

        val result = ArrayBuffer[Int]()
        for (i <- 0 to 2) result += sortArr(i)
        (g._1, result)
      })
      .foreach(t => {
        println("------")
        println("class : " + t._1)
        val iterator = t._2.iterator
        while (iterator.hasNext) println("score : " + iterator.next())
        println("------")
      })
  }
}
