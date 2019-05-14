package lxpsee.top.spark.streaming.study.scalaversion

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/13 17:21.
  */
object HDFSWordCountByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCountByScala")
    val jsc = new StreamingContext(conf, Seconds(5))
    val lines = jsc.textFileStream("D:\\workDir\\otherFile\\temp\\2019-4\\test")
    val words = lines.flatMap(line => {
      line.split(" ")
    })
    val pairs = words.map(word => {
      (word, 1)
    })
    val wc = pairs.reduceByKey(_ + _)

    wc.print()

    jsc.start()
    jsc.awaitTermination()

  }
}
