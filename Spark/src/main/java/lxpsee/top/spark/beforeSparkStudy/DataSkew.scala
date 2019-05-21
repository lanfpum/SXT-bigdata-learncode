package lxpsee.top.spark.beforeSparkStudy

import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/21 16:16.
  *
  * wordcount解决数据倾斜简单版本
  */
object DataSkew {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("DataSkew"))


  }

}
