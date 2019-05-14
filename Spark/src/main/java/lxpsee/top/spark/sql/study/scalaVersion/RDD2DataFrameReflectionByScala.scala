package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/23 15:45.
  *
  * * 如果要用scala开发spark程序
  * * 然后在其中，还要实现基于反射的RDD到DataFrame的转换，就必须得用object extends App的方式
  * * 不能用def main()方法的方式，来运行程序，否则就会报no typetag for ...class的错误
  *
  * 在Scala中使用反射方式，进行RDD到DataFrame的转换，需要手动导入一个隐式转换
  *
  * 元素为case class的RDD,直接对它使用toDF()方法，即可转换为DataFrame
  * 在scala中，row中的数据的顺序，反而是按照我们期望的来排列的，这个跟java是不一样的哦
  */
object RDD2DataFrameReflectionByScala extends App {
  val sparkContext = new SparkContext(
    new SparkConf()
      .setAppName("RDD2DataFrameReflectionByScala")
      .setMaster("spark://ip201:7077")
      .set("spark.testing.memory", "2147480000"))
  val sqlContext = new SQLContext(sparkContext)

  import sqlContext.implicits._

  case class Student(id: Int, name: String, age: Int)

  val studentDaframe = sparkContext.textFile(SparkSqlStudyConstants.STUDENTS_TXT_HDFS_FILE_PATH)
    .map(line => line.split(","))
    .map(arr => Student(arr(0).trim.toInt, arr(1), arr(2).trim.toInt))
    .toDF()

  studentDaframe.registerTempTable("students")

  val teenagerRDD = sqlContext.sql("select * from students where age < 18").rdd

  teenagerRDD.map(row => Student(row(0).toString.toInt, row(1).toString, row(2).toString.toInt))
    .collect()
    .foreach(stu => println(stu.id + " - " + stu.name + " - " + stu.age))

  teenagerRDD.map(row => Student(row.getAs("id"), row.getAs("name"), row.getAs("age")))
    .collect()
    .foreach(stu => println(stu.id + " : " + stu.name + " : " + stu.age))

  teenagerRDD.map(row => {
    val map = row.getValuesMap(Array("id", "name", "age"))
    Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
  }).collect()
    .foreach(stu => println(stu.id + " ---- " + stu.name + " ---- " + stu.age))

}
