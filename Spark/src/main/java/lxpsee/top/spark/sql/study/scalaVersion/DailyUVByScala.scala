package lxpsee.top.spark.sql.study.scalaVersion

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/6 10:15.
  *
  * 这里着重说明一下！！！
  * 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换
  *
  * import org.apache.spark.sql.functions._  必须导入，否则无法提示内置函数
  */
object DailyUVByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("DailyUVByScala"))
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123")

    val userAccessLogROWRDD = sparkContext.parallelize(userAccessLog)
      .map(log => Row(log.split(",")(0), log.split(",")(1).toInt))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userid", IntegerType, true)))

    /**
      * / 这里讲解一下uv的基本含义和业务
      * 每天都有很多用户来访问，但是每个用户可能每天都会访问很多次
      * 所以，uv，指的是，对用户进行去重以后的访问总数
      *
      * 这里，正式开始使用Spark 1.5.x版本提供的最新特性，内置函数，countDistinct
      * 讲解一下聚合函数的用法
      * 首先，对DataFrame调用groupBy()方法，对某一列进行分组
      * 然后，调用agg()方法 ，第一个参数，必须，必须，传入之前在groupBy()方法中出现的字段
      * 第二个参数，传入countDistinct、sum、first等，Spark提供的内置函数
      * 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
      */
    sqlContext.createDataFrame(userAccessLogROWRDD, structType)
      .groupBy("date")
      .agg('date, countDistinct('userid))
      .sort("date")
      .map(row => (row.getString(1), row.getLong(2)))
      .collect()
      .foreach(println)
  }
}
