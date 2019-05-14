package lxpsee.top.spark.streaming.study.scalaversion

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/13 16:12.
  */
object WCStreamingByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WCStreamingByScala").setMaster("local[2]")
    val context = new StreamingContext(conf, Seconds(5))

    context.socketTextStream("localhost", 8888)
      .map(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    context.start()
    context.awaitTermination()
  }

}
