package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/23 17:22.
  */
object RDD2DataFrameProgrammaticallyByScala extends App {
  val sparkContext = new SparkContext(
    new SparkConf()
      .setAppName("RDD2DataFrameProgrammaticallyByScala")
      .setMaster("spark://ip201:7077")
      .set("spark.testing.memory", "2147480000"))
  val sqlContext = new SQLContext(sparkContext)

  val studentRDD = sparkContext.textFile(SparkSqlStudyConstants.STUDENTS_TXT_HDFS_FILE_PATH)
    .map(line => line.split(","))
    .map(arr => Row(arr(0).trim.toInt, arr(1), arr(2).trim.toInt))

  val structType = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))

  val studentDF = sqlContext.createDataFrame(studentRDD, structType)

  studentDF.registerTempTable("students")

  val teenagerDF = sqlContext.sql("select * from students where age < 18")

  teenagerDF.rdd.collect().foreach(row => println(row))
}
