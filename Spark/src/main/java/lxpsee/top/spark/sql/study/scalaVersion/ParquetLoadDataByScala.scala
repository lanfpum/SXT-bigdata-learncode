package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/23 22:14.
  */
object ParquetLoadDataByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("ParquetLoadDataByScala"))
    val sqlContext = new SQLContext(sparkContext)
    val userDF = sqlContext.read.parquet(SparkSqlStudyConstants.USER_PARQUET_LOCAL_FILE_PATH)
    userDF.registerTempTable("user")
    val nameList = sqlContext.sql("select name from user")
      .rdd
      .map(row => "name : " + row.getAs(0))
      .collect()

    for (elem <- nameList) {
      println(elem)
    }

  }

}
