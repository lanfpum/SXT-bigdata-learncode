package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/26 11:23.
  */
object HiveDataSourceByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setAppName("HiveDataSourceByScala").set("spark.testing.memory", "2147480000"))
    val hiveContext = new HiveContext(sparkContext)

    hiveContext.sql("DROP TABLE IF EXISTS lp_spark_study.student_infos")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS lp_spark_study.student_infos (name STRING,age INT)")
    hiveContext.sql("LOAD DATA LOCAL INPATH '" + SparkSqlStudyConstants.Hive_STUDENT_INFO_LOCAL_PATH + "' INTO TABLE lp_spark_study.student_infos")

    hiveContext.sql("DROP TABLE IF EXISTS lp_spark_study.student_scores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS lp_spark_study.student_scores (name STRING,score INT)")
    hiveContext.sql("LOAD DATA LOCAL INPATH '" + SparkSqlStudyConstants.Hive_STUDENT_SCORE_LOCAL_PATH + "' INTO TABLE lp_spark_study.student_scores")

    val goodStudentInfoDF = hiveContext.sql("SELECT si.name,si.age,ss.score "
      + "FROM lp_spark_study.student_infos AS si "
      + "JOIN lp_spark_study.student_scores AS ss "
      + "ON si.name = ss.name WHERE ss.score >= 80")

    hiveContext.sql("DROP TABLE IF EXISTS lp_spark_study.good_student_infos")
    goodStudentInfoDF.write.mode(SaveMode.Overwrite).saveAsTable("lp_spark_study.good_student_infos")

    val listStr = hiveContext.table("lp_spark_study.good_student_infos")
      .rdd
      .map(row => (row.getAs("name"), row.getAs("age").toString.toInt, row.getAs("score").toString.toInt))
      .collect()

    for (elem <- listStr) {
      println(elem)
    }

  }

}
