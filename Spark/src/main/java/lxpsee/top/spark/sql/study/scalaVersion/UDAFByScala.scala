package lxpsee.top.spark.sql.study.scalaVersion

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/5/8 10:25.
  */
object UDAFByScala {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf()
      .setMaster("local")
      .setAppName("UDFByScala"))
    val sqlContext = new SQLContext(sparkContext)

    val names = Array("Leo", "Marry", "Jack", "Tom", "Leo", "Leo", "Leo", "Marry", "Marry", "Jack")
    val nameRowRDD = sparkContext.parallelize(names)
      .map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))
    val nameDF = sqlContext.createDataFrame(nameRowRDD, structType)
    nameDF.registerTempTable("names")
    sqlContext.udf.register("strLen", new StringCountByScala)

    sqlContext.sql("SELECT name,strLen(name) FROM names GROUP BY name")
      .collect()
      .foreach(println)
  }

}
