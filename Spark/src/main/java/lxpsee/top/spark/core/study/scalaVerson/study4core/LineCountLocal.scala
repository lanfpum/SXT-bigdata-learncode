package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 10:39.
  */
object LineCountLocal {
  def main(args: Array[String]): Unit = {
    val context = new SparkContext(new SparkConf().setMaster("local").setAppName("LineCountLocal"))
    val lines = context.textFile(LPConstants.HELLO_TXT_LOCAL_FILE_PATH)
    val lineCount = lines.map((_, 1)).reduceByKey(_ + _)
    lineCount.foreach(t => println(t._1 + " 出现 " + t._2 + "次"))
  }
}
