package lxpsee.top.spark.streaming.study.scalaversion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/16 11:27.
  */
object Top3HotProductByJava {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProductByJava")
    val jsc = new StreamingContext(conf, Seconds(5))

    jsc.socketTextStream(SparkSqlStudyConstants.NC_SERVER_IP, SparkSqlStudyConstants.NC_SERVER_PORT)
      .map(log => (log.split(" ")(2) + "_" + log.split(" ")(1), 1))
      .reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))
      .foreachRDD(categoryProductCountRDD => {
        val rowRDD = categoryProductCountRDD.map(log => {
          val log1Arr = log._1.split("_")
          Row(log1Arr(0), log1Arr(1), log._2.toInt)
        })

        val structType = StructType(Array(
          StructField("category", StringType, true),
          StructField("product", StringType, true),
          StructField("click_count", IntegerType, true)
        ))

        val sqlContext = new SQLContext(categoryProductCountRDD.context)

        sqlContext.createDataFrame(rowRDD, structType).registerTempTable("product_click_log")
        sqlContext.sql("SELECT category,product,click_count FROM ("
          + "SELECT category,product,click_count,"
          + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
          + "FROM product_click_log"
          + ") tmp WHERE rank <= 3")
          .show()
      })

    jsc.start()
    jsc.awaitTermination()
  }

}
