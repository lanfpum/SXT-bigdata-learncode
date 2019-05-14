package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/24 11:51.
 * <p>
 * 查询成绩为80分以上的学生的基本信息与成绩信息
 */
public class JSONDataSourceByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("JSONDataSourceByJava").setMaster("local"));
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset<Row> localStudentDF = sqlContext.read().json(SparkSqlStudyConstants.STUDENTS_SCORE_JSON_LOCAL_FILE_PATH);
        localStudentDF.registerTempTable("students");
        Dataset<Row> gt80Studen = sqlContext.sql("select name,score from students where score >= 80");
        List<String> goodStudentNames = gt80Studen.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        List<String> studentInfoJSONs = new ArrayList<String>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");

        JavaRDD<String> studentInfoRDD = sparkContext.parallelize(studentInfoJSONs);
        Dataset<Row> studentInfoDF = sqlContext.read().json(studentInfoRDD);
        studentInfoDF.registerTempTable("student_infos");

        String sql = "select name,age from student_infos where name in (";

        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";

            if (i < goodStudentNames.size() - 1) sql += ",";
        }

        sql += ")";

        Dataset<Row> goodStudentsInfo = sqlContext.sql(sql);

        JavaRDD<Row> redultRDD = goodStudentsInfo.javaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        // 解析时候会解析成long 格式
                        return new Tuple2<String, Integer>(row.getString(0),
                                Integer.parseInt(String.valueOf(row.getLong(1))));
                    }
                })
                .join(gt80Studen.javaRDD().mapToPair(
                        new PairFunction<Row, String, Integer>() {
                            public Tuple2<String, Integer> call(Row row) throws Exception {
                                return new Tuple2<String, Integer>(row.getString(0),
                                        Integer.parseInt(String.valueOf(row.getLong(1))));
                            }
                        }))
                .map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
                    public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                        return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
                    }
                });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> resultDF = sqlContext.createDataFrame(redultRDD, structType);
        resultDF.show();
        resultDF.printSchema();
        resultDF.write().json(SparkSqlStudyConstants.OUT_PUT_DIR_LOCAL + "/good-students");
    }
}
