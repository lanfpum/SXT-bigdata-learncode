package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/23 18:21.
  */
object ManuallySpecifyOptionsByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(
      new SparkConf()
        .setAppName("RDD2DataFrameProgrammaticallyByScala")
        //        .setMaster("spark://ip201:7077")
        .setMaster("local")
        .set("spark.testing.memory", "2147480000"))
    val sqlContext = new SQLContext(sparkContext)

    val usesDF = sqlContext.read.format("parquet").load(SparkSqlStudyConstants.USER_PARQUET_LOCAL_FILE_PATH)
    usesDF.show()
    usesDF.select("name").write.format("json").save(SparkSqlStudyConstants.OUT_PUT_DIR_LOCAL + "/uses.json")
  }
}
