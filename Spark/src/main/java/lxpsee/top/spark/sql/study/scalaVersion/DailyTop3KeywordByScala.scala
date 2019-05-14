package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/13 11:45.
  */
object DailyTop3KeywordByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(
      new SparkConf()
        .setAppName("DailyTop3KeywordByScala")
        .setMaster("spark://ip201:7077").set("spark.testing.memory", "2147480000")
    )
    val sqlContext = new HiveContext(sparkContext)
    import scala.collection.JavaConverters._
    import scala.collection.mutable.ArrayBuffer

    val queryParamMap = Map("city" -> Array("bj", "sz", "sh"), "platform" -> Array("android", "ios"), "version" -> Array("1.2", "1.3"))
    val queryParamMapBroadcast = sparkContext.broadcast(queryParamMap)

    val dateKeyWordUVRDD = sparkContext.textFile(SparkSqlStudyConstants.KEY_WORD_LOCAL_FILE_PATH)
      .filter(log => {
        val logarr = log.split("\t")

        if (logarr.length < 6) false

        val city = logarr(3)
        val platform = logarr(4)
        val version = logarr(5)

        val queryParamMap = queryParamMapBroadcast.value
        val cities = queryParamMap.get("city")
        val platforms = queryParamMap.get("platform")
        val versions = queryParamMap.get("version")

        if (cities.size > 0 && !cities.contains(city)) false

        if (platforms.size > 0 && !platforms.contains(platform)) false

        if (versions.size > 0 && !versions.contains(version)) false

        true
      })
      .map(log => (log.split("\t")(0) + "_" + log.split("\t")(1), log.split("\t")(2)))
      .groupByKey()
      .map(t => {
        val userIT = t._2.iterator

        val userList = ArrayBuffer[String]()

        while (userIT.hasNext) {
          val user = userIT.next()

          if (!userList.contains(user)) userList += user
        }

        (t._1, userList.size)
      })
      .map(t => Row(t._1.split("_")(0), t._1.split("_")(1), t._2))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("keyword", StringType, true),
      StructField("uv", LongType, true)))

    val dateKeyWordUVDF = sqlContext.createDataFrame(dateKeyWordUVRDD, structType)
    dateKeyWordUVDF.registerTempTable("daily_keyword_uv")

    val dailyTop3KeywordDF = sqlContext.sql("SELECT date,keyword,uv "
      + "FROM ("
      + "SELECT "
      + "date,keyword,uv,"
      + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
      + "FROM daily_keyword_uv "
      + ") tmp "
      + "WHERE rank <= 3")
    val top3DateKeywordsRDD = dailyTop3KeywordDF.rdd
      .map(row => (row.getString(0), row.getString(1) + "_" + row.getLong(2)))
      .groupByKey()
      .map(t => {
        val date = t._1
        val keyworduvIT = t._2.iterator
        var totalUV = 0l
        var dateKeywords = date

        while (keyworduvIT.hasNext) {
          val keyworduv = keyworduvIT.next()
          totalUV += keyworduv.split("_")(1).toLong
          dateKeywords += "," + keyworduv
        }

        (totalUV, dateKeywords)
      }).sortByKey(false)

    val sortedRowRDD = top3DateKeywordsRDD.map(t => {
      val arr = t._2.split(",")
      val date = arr(0)

      val rowList: List[Row] = List(
        Row(date, arr(1).split("_")(0), arr(1).split("_")(1).toLong),
        Row(date, arr(2).split("_")(0), arr(1).split("_")(1).toLong),
        Row(date, arr(3).split("_")(0), arr(1).split("_")(1).toLong)
      )

      rowList.asJava
    })

    // todo   无法创建finalDF
    //    sqlContext.createDataFrame(sortedRowRDD, structType)

  }

}
