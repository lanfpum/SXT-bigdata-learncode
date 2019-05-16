package lxpsee.top.spark.streaming.study.scalaversion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/15 08:56.
  */
object UpdateStateByKeyWordCountByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCountByScala")
    val jsc = new StreamingContext(conf, Seconds(5))

    jsc.checkpoint(SparkSqlStudyConstants.CHECK_POINT_HDFS_PATH)

    jsc.socketTextStream("ip201", 9999)
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        var totalCount = state.getOrElse(0)

        for (value <- values) totalCount += value

        Option(totalCount)
      })
      .print()

    jsc.start()
    jsc.awaitTermination()
  }

}
