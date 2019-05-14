package lxpsee.top.spark.sql.study.javaVersion;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/24 09:05.
 */
public class ParquetPartitionDiscoveryByJava {
    public static void main(String[] args) {
        SQLContext sqlContext = new SQLContext(
                new SparkContext(new SparkConf().setMaster("local").setAppName("ParquetPartitionDiscoveryByJava")));
        Dataset<Row> usesDF = sqlContext.read()
                .parquet("D:\\workDir\\otherFile\\temp\\2019-4\\gender=male\\country=US\\users.parquet");
        usesDF.printSchema();
        usesDF.show();
    }
}
