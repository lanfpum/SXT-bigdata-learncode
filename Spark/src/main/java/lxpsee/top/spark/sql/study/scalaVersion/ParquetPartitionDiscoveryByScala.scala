package lxpsee.top.spark.sql.study.scalaVersion

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/24 09:12.
  */
object ParquetPartitionDiscoveryByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(
      new SparkConf()
        .setAppName("RDD2DataFrameProgrammaticallyByScala")
        .setMaster("spark://ip201:7077")
        .set("spark.testing.memory", "2147480000"))
    val sqlContext = new SQLContext(sparkContext)
    val usesDF = sqlContext.read
      .parquet("hdfs://lanpengcluster/user/lanp/testJar/spark-sql-study/testfile/gender=male/country=US/users.parquet")
    usesDF.printSchema()
    usesDF.show()
  }

}
