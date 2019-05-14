package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 18:11.
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val context = new SparkContext(new SparkConf().setMaster("local").setAppName("Top3scala"))
    context.textFile(LPConstants.TOP_TXT_LOCAL_FILE_PATH)
      .map(line => (line.toInt, line))
      .sortByKey(false)
      .map(_._2)
      .take(3)
      .foreach(println(_))
  }
}
