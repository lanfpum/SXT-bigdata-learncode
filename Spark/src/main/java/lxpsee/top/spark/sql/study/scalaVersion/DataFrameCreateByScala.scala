package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/22 09:12.
  */
object DataFrameCreateByScala {
  def main(args: Array[String]): Unit = {
    val data = new SQLContext(
      new SparkContext(
        new SparkConf()
          //        .setMaster("local")
          .setMaster("spark://ip201:7077")
          .setAppName("DataFrameCreateByScala")
          .set("spark.testing.memory", "2147480000")
      )
    ).read
      //      .json(SparkSqlStudyConstants.STUDENTS_JSON_LOCAL_FILE_PATH)
      .json(SparkSqlStudyConstants.STUDENTS_JSON_HDFS_FILE_PATH)
    data.show()
    data.printSchema()
    data.select("name").show()
    data.select(data.col("name"), data.col("age") + 1).show()
    data.filter(data.col("age") > 18).show()
    data.groupBy("age").count().show()
  }
}
