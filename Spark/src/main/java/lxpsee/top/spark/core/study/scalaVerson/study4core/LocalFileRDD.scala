package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 08:58.
  */
object LocalFileRDD {
  def main(args: Array[String]): Unit = {
    val context = new SparkContext(new SparkConf().setAppName("LocalFileRDD").setMaster("local"))
    val lines = context.textFile(LPConstants.SPARK_TXT_LOCAL_FILE_PATH)
    val count = lines.map(line => line.length).reduce(_ + _)
    println("spark.txt total word is " + count)
  }
}
