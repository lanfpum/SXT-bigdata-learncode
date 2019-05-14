package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
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
 * Created by 努力常态化 on 2019/4/23 17:09.
 * <p>
 * 以编程方式动态指定元数据，将RDD转换为DataFrame
 */
public class RDD2DataFrameProgrammaticallyByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
                .setMaster("local").setAppName("RDD2DataFrameProgrammaticallyByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        final JavaRDD<String> lines = sparkContext.textFile(SparkSqlStudyConstants.STUDENTS_TXT_LOCAL_FILE_PATH);
        JavaRDD<Row> studentRowRDD = lines.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] arr = line.split(",");
                return RowFactory.create(
                        Integer.valueOf(arr[0].trim()),
                        arr[1],
                        Integer.valueOf(arr[2].trim())
                );
            }
        });

        /**
         * 动态构造元数据
         * 	 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
         * 	 或者是配置文件中，加载出来的，是不固定的
         * 	 所以特别适合用这种编程的方式，来构造元数据
         */
        List<StructField> structFields = new ArrayList<StructField>(3);
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 使用动态构造的元数据，将RDD转换为DataFrame
        Dataset<Row> studentDF = sqlContext.createDataFrame(studentRowRDD, structType);
        studentDF.registerTempTable("students");
        Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age  < 18");
        List<Row> studentList = teenagerDF.javaRDD().collect();

        for (Row row : studentList) {
            System.out.println(row);
        }
    }
}
