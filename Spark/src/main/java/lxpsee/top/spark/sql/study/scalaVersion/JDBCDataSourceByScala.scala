package lxpsee.top.spark.sql.study.scalaVersion

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/6 08:18.
  */
object JDBCDataSourceByScala {
  def main(args: Array[String]): Unit = {
    val mysql_url = "jdbc:mysql://localhost:3306/lanp_test_db"
    val mysql_driver = "com.mysql.jdbc.Driver"

    val student_infos = "student_infos"
    val student_scores = "student_scores"
    val good_student_infos = "good_student_infos"

    val sparkContext = new SparkContext(new SparkConf().setAppName("JDBCDataSourceByScala").setMaster("local"))
    val sqlContext = new SQLContext(sparkContext)

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123")

    val scoreRDD = sqlContext.read
      .jdbc(mysql_url, student_scores, prop)
      .rdd
      .map(row => (row.get(0).toString, row.get(1).toString.toInt))

    val goodStudentRowRDD = sqlContext.read
      .jdbc(mysql_url, student_infos, prop)
      .rdd
      .map(row => (row.get(0).toString, row.get(1).toString.toInt))
      .join(scoreRDD)
      .map(t => Row(t._1, t._2._1, t._2._2))
      .filter(_.get(2).toString.toInt > 80)

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", IntegerType, true)
    ))

    val goodStudentDF = sqlContext.createDataFrame(goodStudentRowRDD, structType)

    goodStudentDF.rdd.foreach(row => println(row))
  }

}
