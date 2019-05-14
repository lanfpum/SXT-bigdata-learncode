package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 16:32.
  */
object SortWordCountByValue {
  def main(args: Array[String]): Unit = {
    val context = new SparkContext(new SparkConf().setAppName("SortWordCountByValue").setMaster("local"))
    context.textFile(LPConstants.SPARK_TXT_LOCAL_FILE_PATH)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(reduceRDD => (reduceRDD._2, reduceRDD._1))
      .sortByKey(false)
      .map(sortRDD => (sortRDD._2, sortRDD._1))
      .foreach(valueRDD => println(valueRDD._1 + " 出现 " + valueRDD._2))
  }
}
