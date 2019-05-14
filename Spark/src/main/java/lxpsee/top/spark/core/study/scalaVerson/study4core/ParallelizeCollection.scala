package lxpsee.top.spark.core.study.scalaVerson.study4core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/3/27 08:24.
  */
object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numsRdd = sc.parallelize(nums)
    val sum = numsRdd.reduce(_ + _)
    println("1-10: " + sum)
  }

}
