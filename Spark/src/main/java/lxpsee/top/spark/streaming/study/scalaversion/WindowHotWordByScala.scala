package lxpsee.top.spark.streaming.study.scalaversion

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/15 19:01.
  */
object WindowHotWordByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCountByScala")
    val jsc = new StreamingContext(conf, Seconds(5))

    jsc.socketTextStream("ip201", 9999)
      .map(log => (log.split(" ")(1), 1))
      .reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))
      .transform(reduceRDD => {
        reduceRDD.map(t => (t._2, t._1))
          .sortByKey(false)
          .map(t => (t._2, t._1))
          .take(3)
          .foreach(t => println(t._1 + " : " + t._2))
        reduceRDD
      })
      .print()

    jsc.start()
    jsc.awaitTermination()
  }
}
