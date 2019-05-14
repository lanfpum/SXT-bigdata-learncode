package lxpsee.top.spark.streaming.study.scalaversion

import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/14 11:38.
  */
object KafkaReceiverWordCountByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaReceiverWordCountByScala").setMaster("local[2]")
    val jsc = new StreamingContext(conf, Seconds(2))
    val zkQuorum = "192.168.217.201:2181,192.168.217.202:2181,192.168.217.203:2181"
    val topicThreadMap = Map("wordcount" -> 1)

    /* val wc = KafkaUtils.createStream(jsc, zkQuorum, "group1", topicThreadMap)
       .flatMap(t => {
         t._2.split(" ")
       })
       .map((_, 1))
       .reduceByKey(_ + _)

     wc.print()*/

    jsc.start()
    jsc.awaitTermination()
  }

}
