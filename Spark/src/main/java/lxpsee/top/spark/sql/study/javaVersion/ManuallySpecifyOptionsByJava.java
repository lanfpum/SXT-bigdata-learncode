package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/23 18:14.
 */
public class ManuallySpecifyOptionsByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
                .setMaster("local").setAppName("GenericLoadSaveByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> peopleDF = sqlContext.read().format("json").load(SparkSqlStudyConstants.PEOPLE_JSON_LOCAL_FILE_PATH);
        peopleDF.show();
        peopleDF.write().format("parquet").save(SparkSqlStudyConstants.OUT_PUT_DIR_LOCAL + "/people.parquet");
    }
}
