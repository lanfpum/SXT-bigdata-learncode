package lxpsee.top.spark.core.study.scalaVerson.study4core

import lxpsee.top.spark.core.study.constant.LPConstants
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/25 18:40.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val context = new SparkContext(conf)
    val lines = context.textFile(LPConstants.SPARK_TXT_LOCAL_FILE_PATH)
    val words = lines.flatMap(line => line.split(" "))
    val pair = words.map(word => (word, 1))
    val wordCounts = pair.reduceByKey((_ + _))
    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))
  }

}
