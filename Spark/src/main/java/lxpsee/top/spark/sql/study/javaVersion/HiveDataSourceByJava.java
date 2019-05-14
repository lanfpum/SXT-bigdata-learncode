package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/25 08:55.
 * <p>
 * Hive数据源
 * <p>
 * 创建HiveContext，注意，这里，它接收的是SparkContext作为参数，不是JavaSparkContext
 * 存在表则删除，创建表，导入数据到表中
 * 执行sql查询，关联两张表，查询成绩大于80分的学生
 * 可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
 * 可以用table()方法，针对hive表，直接创建DataFrame
 * <p>
 * 注意脚本不要定义executor的内存大小，会导致内存不足
 */
public class HiveDataSourceByJava {
    public static void main(String[] args) {
        final String STUDENT_INFO_TABLE_NAME = "lp_spark_study.student_infos";
        final String STUDENT_SCORE_TABLE_NAME = "lp_spark_study.student_scores";
        final String GOOD_STUDENT_TABLE_NAME = "lp_spark_study.good_student_infos";

        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                        .setAppName("HiveDataSourceByJava")
//                        .setMaster("spark://ip201:7077")
                        .set("spark.testing.memory", "2147480000")
        );
        HiveContext hiveContext = new HiveContext(sparkContext.sc());

        hiveContext.sql("DROP TABLE IF EXISTS " + STUDENT_INFO_TABLE_NAME);
        hiveContext.sql("CREATE TABLE IF NOT EXISTS " + STUDENT_INFO_TABLE_NAME + " (name STRING,age INT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '" + SparkSqlStudyConstants.Hive_STUDENT_INFO_LOCAL_PATH
                + "' INTO TABLE " + STUDENT_INFO_TABLE_NAME);

        hiveContext.sql("DROP TABLE IF EXISTS " + STUDENT_SCORE_TABLE_NAME);
        hiveContext.sql("CREATE TABLE IF NOT EXISTS " + STUDENT_SCORE_TABLE_NAME + " (name STRING,score INT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '" + SparkSqlStudyConstants.Hive_STUDENT_SCORE_LOCAL_PATH
                + "' INTO TABLE " + STUDENT_SCORE_TABLE_NAME);

        Dataset<Row> goodStudentDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM " + STUDENT_INFO_TABLE_NAME + " si "
                + "JOIN " + STUDENT_SCORE_TABLE_NAME + " ss "
                + "ON si.name=ss.name WHERE ss.score>=80");

        hiveContext.sql("DROP TABLE IF EXISTS " + GOOD_STUDENT_TABLE_NAME);
//        goodStudentDF.write().insertInto(GOOD_STUDENT_TABLE_NAME);
        goodStudentDF.write().mode(SaveMode.Overwrite).saveAsTable(GOOD_STUDENT_TABLE_NAME);

        List<String> goodStudentRows = hiveContext.table(GOOD_STUDENT_TABLE_NAME).javaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                return row.getAs("name") + " ： " + row.getAs("age") + " ： " + row.getAs("score");
            }
        }).collect();

        for (String goodStudentRow : goodStudentRows) {
            System.out.println(goodStudentRow);
        }

        sparkContext.close();
    }
}
