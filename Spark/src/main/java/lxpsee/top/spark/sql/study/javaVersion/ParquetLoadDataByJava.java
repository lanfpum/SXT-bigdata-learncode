package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/23 22:08.
 */
public class ParquetLoadDataByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("ParquetLoadDataByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> usesDF = sqlContext.read().parquet(SparkSqlStudyConstants.USER_PARQUET_LOCAL_FILE_PATH);
        usesDF.registerTempTable("users");
        Dataset<Row> nameDF = sqlContext.sql("select name from users");

        List<String> stringList = nameDF.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                return "name : " + row.getString(0);
            }
        }).collect();

        for (String string : stringList) {
            System.out.println(string);
        }

    }
}
