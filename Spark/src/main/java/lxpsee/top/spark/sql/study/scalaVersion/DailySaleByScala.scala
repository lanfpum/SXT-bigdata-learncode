package lxpsee.top.spark.sql.study.scalaVersion

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/6 16:48.
  */
object DailySaleByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("DailySaleByScala"))
    val sqlContext = new SQLContext(sparkContext)

    val userSaleLog = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")

    val logRowRDD = sparkContext.parallelize(userSaleLog)
      .filter(log => if (log.split(",").length == 3) true else false)
      .map(log => Row(log.split(",")(0), log.split(",")(1).toDouble))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)))

    // 无法直接使用内置函数，这里使用Map代替，第一个是列名，第二个是内置函数名
    sqlContext.createDataFrame(logRowRDD, structType)
      .groupBy("date")
      .agg(Map(("sale_amount", "sum")))
      .sort("date")
      .show()

  }
}
