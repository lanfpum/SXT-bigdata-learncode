package lxpsee.top.spark.streaming.study.javaversion.output

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/15 20:27.
  */
object PersistWordCountByScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("PersistWordCountByScala")
    val jsc = new StreamingContext(conf, Seconds(90))

    jsc.checkpoint(SparkSqlStudyConstants.CHECK_POINT_HDFS_PATH)

    jsc.socketTextStream("ip201", 9999)
      .map(_.split(" "))
      .map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        var totalCount = state.getOrElse(0)

        for (value <- values) {
          totalCount += value
        }

        Option(totalCount)
      })
      .foreachRDD(forRDD => {
        forRDD.foreachPartition(wordcounts => {
          val conn = ConnectionPool.getConnection
          val sttm = conn.createStatement()

          while (wordcounts.hasNext) {
            val wordcount = wordcounts.next()
            val sql = "INSERT INTO wordcount(word,count) VALUES('" + wordcount._1 + "'," + wordcount._2 + ")"
            sttm.execute(sql)
          }

          ConnectionPool.returnCOnmnection(conn)
        })
      })

    jsc.start()
    jsc.awaitTermination()
  }
}
