package lxpsee.top.spark.sql.study.scalaVersion

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/24 09:32.
  */
object ParquetMergeSchemaByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(
      new SparkConf()
        .setAppName("RDD2DataFrameProgrammaticallyByScala")
        .setMaster("spark://ip201:7077")
        .set("spark.testing.memory", "2147480000"))
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    val studentsWithNameAge = Array(("leo", 23), ("jim", 21), ("tom", 18))
    val studentsWithNameAgeDF = sparkContext.parallelize(studentsWithNameAge).toDF("name", "age")
    studentsWithNameAgeDF.write.mode(SaveMode.Append)
      .save("hdfs://lanpengcluster/user/lanp/testJar/spark-sql-study/testfile/student")

    val studentsWithNameGrade = Array(("curry", "A"), ("kobe", "A"), ("james", "B"))
    val studentsWithNameGradeDF = sparkContext.parallelize(studentsWithNameGrade).toDF("name", "grade")

    studentsWithNameGradeDF.write.mode(SaveMode.Append).parquet("hdfs://lanpengcluster/user/lanp/testJar/spark-sql-study/testfile/student")

    val studen = sqlContext.read.option("mergeSchema", "true")
      .parquet("hdfs://lanpengcluster/user/lanp/testJar/spark-sql-study/testfile/student")
    studen.printSchema()
    studen.show()
  }
}
