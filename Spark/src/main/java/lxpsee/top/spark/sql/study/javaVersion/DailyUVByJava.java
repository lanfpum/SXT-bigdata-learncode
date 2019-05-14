package lxpsee.top.spark.sql.study.javaVersion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/6 16:19.
 */
public class DailyUVByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local")
                .setAppName("DailyUVByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext.sc());

        List<String> userAccessLog = new ArrayList<String>(9);
        userAccessLog.add("2015-10-01,1122");
        userAccessLog.add("2015-10-01,1122");
        userAccessLog.add("2015-10-01,1123");
        userAccessLog.add("2015-10-01,1124");
        userAccessLog.add("2015-10-01,1124");
        userAccessLog.add("2015-10-02,1122");
        userAccessLog.add("2015-10-02,1121");
        userAccessLog.add("2015-10-02,1123");
        userAccessLog.add("2015-10-02,1123");

        JavaRDD<Row> logRowRDD = sparkContext.parallelize(userAccessLog).map(new Function<String, Row>() {
            public Row call(String s) throws Exception {
                String[] arr = s.split(",");
                return RowFactory.create(arr[0], Integer.valueOf(arr[1]));
            }
        });

        List<StructField> structFields = new ArrayList<StructField>(2);
        structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("userid", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> logDF = sqlContext.createDataFrame(logRowRDD, structType);

        // Java版本中没有使用到内置函数
        logDF.registerTempTable("log");
        sqlContext.sql("select date,count(distinct(userid)) as uv from log group by date ORDER BY date").show();
    }
}
