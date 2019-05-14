package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/22 08:58.
 */
public class DataFrameCreateByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
                .setAppName("DataFrameCreateByJava")
                .setMaster("local"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> dataset = sqlContext.read().json(SparkSqlStudyConstants.STUDENTS_JSON_LOCAL_FILE_PATH);
        dataset.show();
        dataset.printSchema();
        dataset.select("name").show();
        dataset.select(dataset.col("name"), dataset.col("age").plus(1)).show();
        dataset.filter(dataset.col("age").gt(18)).show();
        dataset.groupBy("age").count().show();
    }
}
