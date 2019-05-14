package lxpsee.top.spark.sql.study.scalaVersion

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/4/24 16:48.
  *
  * 查询成绩为80分以上的学生的基本信息与成绩信息
  */
object JSONDataSourceByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(
      new SparkConf()
        .setAppName("JSONDataSourceByScala")
        .setMaster("spark://ip201:7077")
        .set("spark.testing.memory", "2147480000"))
    val sqlContext = new SQLContext(sparkContext)

    val studentScoresDF = sqlContext.read.json(SparkSqlStudyConstants.STUDENTS_SCORE_JSON_HDFS_FILE_PATH)
    studentScoresDF.registerTempTable("students")
    val goodStudentScoresDF = sqlContext.sql("select name,score from students where score >= 80")
    val goodStudentNames = goodStudentScoresDF.rdd.map(row => row.getString(0)).collect()

    val studentInfoJSONs = Array("{\"name\":\"Leo\", \"age\":18}", "{\"name\":\"Marry\", \"age\":17}", "{\"name\":\"Jack\", \"age\":19}")
    val studentInfosDF = sqlContext.read.json(sparkContext.parallelize(studentInfoJSONs))
    studentInfosDF.registerTempTable("student_infos")

    var sql = "select name,age from student_infos where name in ("

    for (i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if (i < goodStudentNames.length - 1) sql += ","
    }

    sql += ")"

    // join后面会maven打包报错，先生成rdd再去join，前面是查询出来超过80的rdd名字
    val goodStudentInfosDFROWRDD = sqlContext.sql(sql).rdd.map(row => (row.getAs("name").toString, row.getAs("age").toString.toInt))

    val goodStudentRowsRDD = goodStudentScoresDF.rdd.map(row => (row.getAs("name").toString, row.getAs("score").toString.toInt))
      .join(goodStudentInfosDFROWRDD)
      .map(info => Row(info._1, info._2._1.toString.toInt, info._2._2.toString.toInt))

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)
    ))

    val resultDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType)
    resultDF.printSchema()
    resultDF.show()

    resultDF.write.mode(SaveMode.Overwrite).json(SparkSqlStudyConstants.HDFS_OUT_PUT_DIR + "/JSONDataSourceByScala")
  }

}
