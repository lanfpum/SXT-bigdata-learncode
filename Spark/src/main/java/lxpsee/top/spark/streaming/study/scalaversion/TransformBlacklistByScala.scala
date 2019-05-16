package lxpsee.top.spark.streaming.study.scalaversion

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/15 18:02.
  *
  * 不用checkpoint
  */
object TransformBlacklistByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlacklistByScala")
    val jsc = new StreamingContext(conf, Seconds(5))

    val blackListRDD = jsc.sparkContext.parallelize(List(("tom", true)))

    jsc.socketTextStream("ip201", 9999)
      .map(log => (log.split(" ")(1), log))
      .transform(
        _.leftOuterJoin(blackListRDD)
          .filter(joinRDD => {
            if (joinRDD._2._2.getOrElse(false)) false
            else true
          })
          .map(_._2._1)
      ).print()

    jsc.start()
    jsc.awaitTermination()
  }
}
