package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 17:24.
  * 数组的获取方式，String 装换成 Int
  */
object SecondarySort {
  def main(args: Array[String]): Unit = {
    val context = new SparkContext(new SparkConf().setMaster("local").setAppName("SecondarySortScala"))
    context.textFile(LPConstants.SORT_TXT_LOCAL_FILE_PATH)
      .map(line => (new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line))
      .sortByKey()
      .map((_._2))
      .foreach(println(_))
  }
}
